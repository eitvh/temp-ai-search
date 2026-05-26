import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pymysql
import requests
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

TIKHUB_YT_KEY = os.getenv("TIKHUB_YT_API_KEY")

MYSQL_HOST = os.getenv("CRAWL_DB_HOST")
MYSQL_PORT = int(os.getenv("CRAWL_DB_PORT", "3306"))
MYSQL_USER = os.getenv("CRAWL_DB_USERNAME")
MYSQL_PASSWORD = os.getenv("CRAWL_DB_PASSWORD")
MYSQL_DATABASE = os.getenv("CRAWL_DB_DATABASE")

MONGO_URI_ATLAS = os.getenv("MONGO_URI_ATLAS")
MONGO_DB_ATLAS = os.getenv("MONGO_DB_ATLAS")
MONGO_COLL_ATLAS_YOUTUBE = os.getenv("MONGO_COLL_ATLAS_YOUTUBE")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
AWS_BUCKET_TRANSCRIPT = os.getenv("AWS_BUCKET_TRANSCRIPT", "cloudbreakr-youtube")
AWS_PREFIX_TRANSCRIPT = os.getenv("AWS_PREFIX_TRANSCRIPT", "transcript").strip("/")

BASE_URL = "https://api.tikhub.io"
GET_VIDEO_CAPTIONS_ENDPOINT = "/api/v1/youtube/web_v2/get_video_captions_v2"

MYSQL_TABLE = os.getenv("MYSQL_TABLE_V2", "ai_yt_info_v2")
MYSQL_VIDEO_ID_COLUMN = os.getenv("MYSQL_VIDEO_ID_COLUMN", "video_id")
MYSQL_LANGUAGE_CODE_COLUMN = os.getenv("MYSQL_LANGUAGE_CODE_COLUMN", "language_code")
MYSQL_S3_URL_COLUMN = os.getenv("MYSQL_S3_URL_COLUMN", "s3_url")
MYSQL_ERROR_COLUMN = os.getenv("MYSQL_ERROR_COLUMN", "error")

PROCESS_DATA = int(os.getenv("PROCESS_DATA", "0"))
MONGO_READ_BATCH_SIZE = int(os.getenv("MONGO_READ_BATCH_SIZE", "500"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0"))
FORCE_REPROCESS = os.getenv("FORCE_REPROCESS", "false").lower() == "true"
TARGET_LANG = os.getenv("TARGET_LANG", "en")
SUBTITLE_FORMAT = os.getenv("SUBTITLE_FORMAT", "srt")
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "100"))
MYSQL_MAX_PACKET_SAFE_CHUNK = int(os.getenv("MYSQL_MAX_PACKET_SAFE_CHUNK", "100"))
CHECKPOINT_UPLOAD_EVERY_FLUSH = os.getenv("CHECKPOINT_UPLOAD_EVERY_FLUSH", "true").lower() == "true"

BASE_DIR = Path(__file__).resolve().parent
LOCAL_PAYLOAD_DIR = BASE_DIR / "youtube_transcript_v2_payloads"
CHECKPOINT_FILE = BASE_DIR / "youtube_transcript_s3_v2_checkpoint.json"
S3_CHECKPOINT_KEY = f"{AWS_PREFIX_TRANSCRIPT}/_checkpoint/youtube_transcript_s3_v2_checkpoint.json"

if not TIKHUB_YT_KEY:
    raise ValueError("Missing TIKHUB_YT_API_KEY in .env")

if not MYSQL_HOST:
    raise ValueError("Missing CRAWL_DB_HOST in .env")

if not MYSQL_USER:
    raise ValueError("Missing CRAWL_DB_USERNAME in .env")

if not MYSQL_PASSWORD:
    raise ValueError("Missing CRAWL_DB_PASSWORD in .env")

if not MYSQL_DATABASE:
    raise ValueError("Missing CRAWL_DB_DATABASE in .env")

if not MONGO_URI_ATLAS:
    raise ValueError("Missing MONGO_URI_ATLAS in .env")

if not MONGO_DB_ATLAS:
    raise ValueError("Missing MONGO_DB_ATLAS in .env")

if not MONGO_COLL_ATLAS_YOUTUBE:
    raise ValueError("Missing MONGO_COLL_ATLAS_YOUTUBE in .env")

if not AWS_BUCKET_TRANSCRIPT:
    raise ValueError("Missing AWS_BUCKET_TRANSCRIPT in .env")


class TikHubServerError(Exception):
    pass


def validate_identifier(value, name):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value or ""):
        raise ValueError(f"Invalid {name}")
    return value


MYSQL_TABLE = validate_identifier(MYSQL_TABLE, "MYSQL_TABLE")
MYSQL_VIDEO_ID_COLUMN = validate_identifier(MYSQL_VIDEO_ID_COLUMN, "MYSQL_VIDEO_ID_COLUMN")
MYSQL_LANGUAGE_CODE_COLUMN = validate_identifier(MYSQL_LANGUAGE_CODE_COLUMN, "MYSQL_LANGUAGE_CODE_COLUMN")
MYSQL_S3_URL_COLUMN = validate_identifier(MYSQL_S3_URL_COLUMN, "MYSQL_S3_URL_COLUMN")
MYSQL_ERROR_COLUMN = validate_identifier(MYSQL_ERROR_COLUMN, "MYSQL_ERROR_COLUMN")


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


mongo_client = MongoClient(MONGO_URI_ATLAS)
mongo_db = mongo_client[MONGO_DB_ATLAS]
source_coll = mongo_db[MONGO_COLL_ATLAS_YOUTUBE]


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def now_datetime_utc():
    return datetime.now(timezone.utc)


def epoch_seconds():
    return int(time.time())


def get_s3_url(bucket, s3_key):
    return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


def get_transcript_s3_key(video_id):
    if AWS_PREFIX_TRANSCRIPT:
        return f"{AWS_PREFIX_TRANSCRIPT}/{video_id}.json"
    return f"{video_id}.json"


def upload_json_to_s3(data, bucket, s3_key):
    body = json.dumps(data, ensure_ascii=False, indent=2)
    get_s3_client().put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return get_s3_url(bucket, s3_key)


def upload_file_json_to_s3(file_path, bucket, s3_key):
    body = Path(file_path).read_bytes()
    get_s3_client().put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )
    return get_s3_url(bucket, s3_key)


def upload_checkpoint_to_s3(checkpoint):
    return upload_json_to_s3(checkpoint, AWS_BUCKET_TRANSCRIPT, S3_CHECKPOINT_KEY)


def empty_checkpoint():
    return {
        "status": "NEW",
        "process_data": PROCESS_DATA,
        "force_reprocess": FORCE_REPROCESS,
        "target_lang": TARGET_LANG,
        "subtitle_format": SUBTITLE_FORMAT,
        "flush_batch_size": FLUSH_BATCH_SIZE,
        "mongo_read_batch_size": MONGO_READ_BATCH_SIZE,
        "last_mongo_id": "",
        "last_video_id": "",
        "last_error": "",
        "run_started_at": "",
        "total_seen": 0,
        "run_processed": 0,
        "run_fetched_tikhub": 0,
        "run_checkpointed": 0,
        "run_uploaded_s3": 0,
        "run_saved_mysql": 0,
        "run_saved_mongo": 0,
        "run_skipped": 0,
        "run_errors": 0,
        "cumulative_processed": 0,
        "cumulative_fetched_tikhub": 0,
        "cumulative_checkpointed": 0,
        "cumulative_uploaded_s3": 0,
        "cumulative_saved_mysql": 0,
        "cumulative_saved_mongo": 0,
        "cumulative_skipped": 0,
        "cumulative_errors": 0,
        "pending_s3": [],
        "pending_mysql": [],
        "pending_mongo": [],
        "pending_s3_count": 0,
        "pending_mysql_count": 0,
        "pending_mongo_count": 0,
        "pending_s3_last_error": "",
        "pending_mysql_last_error": "",
        "pending_mongo_last_error": "",
        "failed_video_errors": {},
        "stopped_by_500": False,
        "checkpoint_s3_bucket": AWS_BUCKET_TRANSCRIPT,
        "checkpoint_s3_key": S3_CHECKPOINT_KEY,
        "checkpoint_s3_url": "",
        "checkpoint_backup_file": "",
        "updated_at": now_utc(),
    }


def load_checkpoint():
    default_data = empty_checkpoint()

    if not CHECKPOINT_FILE.exists():
        return default_data

    try:
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default_data

    for key, value in default_data.items():
        if key not in data:
            data[key] = value

    for key in ["pending_s3", "pending_mysql", "pending_mongo"]:
        if not isinstance(data.get(key), list):
            data[key] = []

    if not isinstance(data.get("failed_video_errors"), dict):
        data["failed_video_errors"] = {}

    data["pending_s3_count"] = len(data["pending_s3"])
    data["pending_mysql_count"] = len(data["pending_mysql"])
    data["pending_mongo_count"] = len(data["pending_mongo"])

    return data


def write_local_checkpoint(data):
    temp_file = CHECKPOINT_FILE.with_name(f"{CHECKPOINT_FILE.name}.{os.getpid()}.tmp")
    temp_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    for attempt in range(10):
        try:
            os.replace(str(temp_file), str(CHECKPOINT_FILE))
            return
        except PermissionError:
            time.sleep(0.5 * (attempt + 1))

    backup_file = CHECKPOINT_FILE.with_name(f"{CHECKPOINT_FILE.stem}.{epoch_seconds()}.backup.json")
    temp_file.replace(backup_file)
    data["checkpoint_backup_file"] = str(backup_file)
    raise PermissionError(f"Could not replace checkpoint file after retries. Backup saved to {backup_file}")


def save_checkpoint(data, upload_s3=False):
    data["updated_at"] = now_utc()
    data["pending_s3_count"] = len(data.get("pending_s3", []))
    data["pending_mysql_count"] = len(data.get("pending_mysql", []))
    data["pending_mongo_count"] = len(data.get("pending_mongo", []))
    write_local_checkpoint(data)

    if upload_s3:
        try:
            data["checkpoint_s3_url"] = upload_checkpoint_to_s3(data)
            write_local_checkpoint(data)
        except Exception as e:
            data["checkpoint_s3_upload_error"] = str(e)
            write_local_checkpoint(data)


def checkpoint_set_status(checkpoint, status, video_id="", error="", upload_s3=False):
    checkpoint["status"] = status
    checkpoint["last_video_id"] = video_id or checkpoint.get("last_video_id", "")
    checkpoint["last_error"] = error or ""
    save_checkpoint(checkpoint, upload_s3=upload_s3)


def record_failed_video(checkpoint, video_id, error):
    checkpoint["failed_video_errors"][video_id] = {
        "video_id": video_id,
        "error": str(error),
        "updated_at": now_utc(),
    }
    save_checkpoint(checkpoint)


def clear_failed_video(checkpoint, video_id):
    if video_id in checkpoint.get("failed_video_errors", {}):
        del checkpoint["failed_video_errors"][video_id]
        save_checkpoint(checkpoint)


def update_run_counts(
    checkpoint,
    run_processed,
    run_fetched_tikhub,
    run_checkpointed,
    run_uploaded_s3,
    run_saved_mysql,
    run_saved_mongo,
    run_skipped,
    run_errors,
    cumulative_processed,
    cumulative_fetched_tikhub,
    cumulative_checkpointed,
    cumulative_uploaded_s3,
    cumulative_saved_mysql,
    cumulative_saved_mongo,
    cumulative_skipped,
    cumulative_errors,
    upload_s3=False,
):
    checkpoint["run_processed"] = run_processed
    checkpoint["run_fetched_tikhub"] = run_fetched_tikhub
    checkpoint["run_checkpointed"] = run_checkpointed
    checkpoint["run_uploaded_s3"] = run_uploaded_s3
    checkpoint["run_saved_mysql"] = run_saved_mysql
    checkpoint["run_saved_mongo"] = run_saved_mongo
    checkpoint["run_skipped"] = run_skipped
    checkpoint["run_errors"] = run_errors
    checkpoint["cumulative_processed"] = cumulative_processed
    checkpoint["cumulative_fetched_tikhub"] = cumulative_fetched_tikhub
    checkpoint["cumulative_checkpointed"] = cumulative_checkpointed
    checkpoint["cumulative_uploaded_s3"] = cumulative_uploaded_s3
    checkpoint["cumulative_saved_mysql"] = cumulative_saved_mysql
    checkpoint["cumulative_saved_mongo"] = cumulative_saved_mongo
    checkpoint["cumulative_skipped"] = cumulative_skipped
    checkpoint["cumulative_errors"] = cumulative_errors
    save_checkpoint(checkpoint, upload_s3=upload_s3)


def format_seconds(seconds):
    seconds = int(seconds or 0)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def short_error(error):
    text = str(error or "").replace("\n", " ").replace("\r", " ").strip()
    return text[:300]


def print_progress(
    status,
    run_processed,
    run_fetched_tikhub,
    run_checkpointed,
    run_uploaded_s3,
    run_saved_mysql,
    run_saved_mongo,
    run_skipped,
    run_errors,
    pending_s3_count,
    pending_mysql_count,
    pending_mongo_count,
    video_id,
    started_at_epoch,
    error="",
):
    elapsed_seconds = max(epoch_seconds() - started_at_epoch, 1)
    rate_per_minute = round((run_processed / elapsed_seconds) * 60, 2)
    error_text = short_error(error)

    print(
        f"[{now_utc()}] "
        f"status={status} "
        f"processed={run_processed} "
        f"fetched_tikhub={run_fetched_tikhub} "
        f"checkpointed={run_checkpointed} "
        f"uploaded_s3={run_uploaded_s3} "
        f"saved_mysql={run_saved_mysql} "
        f"saved_mongo={run_saved_mongo} "
        f"skipped={run_skipped} "
        f"errors={run_errors} "
        f"pending_s3={pending_s3_count} "
        f"pending_mysql={pending_mysql_count} "
        f"pending_mongo={pending_mongo_count} "
        f"rate_per_minute={rate_per_minute} "
        f"elapsed={format_seconds(elapsed_seconds)} "
        f"video_id={video_id} "
        f"error={error_text}",
        flush=True,
    )


def response_json(response):
    try:
        return response.json()
    except Exception:
        return {}


def tikhub_get(endpoint, params):
    headers = {
        "Authorization": f"Bearer {TIKHUB_YT_KEY}",
        "Content-Type": "application/json",
    }

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        headers=headers,
        params=params,
        timeout=90,
    )

    if response.status_code == 401:
        headers["Authorization"] = TIKHUB_YT_KEY
        response = requests.get(
            f"{BASE_URL}{endpoint}",
            headers=headers,
            params=params,
            timeout=90,
        )

    data = response_json(response)

    if response.status_code >= 500:
        message = str(data.get("message") or response.text or f"HTTP {response.status_code} error").strip()
        raise TikHubServerError(message)

    return response.status_code, data


def get_srt_id(language_code, content, status_code):
    language_code = str(language_code or "").strip()
    content = str(content or "").strip()

    if status_code != 200:
        return 3

    if not content:
        return 3

    if language_code == "en":
        return 1

    if language_code == "a.en":
        return 2

    return 0


def get_video_captions(video_id):
    status_code, result = tikhub_get(
        GET_VIDEO_CAPTIONS_ENDPOINT,
        {
            "video_id": video_id,
            "language_code": TARGET_LANG,
            "format": SUBTITLE_FORMAT,
        },
    )

    api_message = str(result.get("message") or "").strip()
    data = result.get("data")

    if isinstance(data, dict):
        content = str(data.get("content") or "").strip()
        language_code = str(data.get("language_code") or "").strip()
        language_name = str(data.get("language_name") or "").strip()
        output_format = str(data.get("format") or SUBTITLE_FORMAT).strip()
        output_video_id = str(data.get("video_id") or video_id).strip()
    else:
        content = ""
        language_code = ""
        language_name = ""
        output_format = SUBTITLE_FORMAT
        output_video_id = video_id

    if status_code != 200 and not api_message:
        api_message = f"HTTP {status_code} error"

    srt_id = get_srt_id(language_code, content, status_code)

    return {
        "video_id": output_video_id,
        "language_code": language_code,
        "language_name": language_name,
        "format": output_format,
        "content": content,
        "srt_id": srt_id,
        "error": api_message,
        "tikhub_status_code": status_code,
        "fetched_at": now_utc(),
    }


def get_mysql_video_row(video_id):
    sql = f"""
    SELECT
        `id`,
        `{MYSQL_VIDEO_ID_COLUMN}` AS video_id,
        `{MYSQL_LANGUAGE_CODE_COLUMN}` AS language_code,
        `{MYSQL_S3_URL_COLUMN}` AS s3_url,
        `{MYSQL_ERROR_COLUMN}` AS error
    FROM `{MYSQL_TABLE}`
    WHERE `{MYSQL_VIDEO_ID_COLUMN}` = %s
    LIMIT 1
    """

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, (video_id,))
            return cursor.fetchone()
    finally:
        connection.close()


def should_skip(video_id):
    if FORCE_REPROCESS:
        return False

    row = get_mysql_video_row(video_id)

    if row and str(row.get("s3_url") or "").strip():
        return True

    return False


def chunk_list(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def bulk_upsert_mysql(items):
    if not items:
        return 0

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` (
        `{MYSQL_VIDEO_ID_COLUMN}`,
        `{MYSQL_LANGUAGE_CODE_COLUMN}`,
        `{MYSQL_S3_URL_COLUMN}`,
        `{MYSQL_ERROR_COLUMN}`
    )
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `{MYSQL_LANGUAGE_CODE_COLUMN}` = VALUES(`{MYSQL_LANGUAGE_CODE_COLUMN}`),
        `{MYSQL_S3_URL_COLUMN}` = VALUES(`{MYSQL_S3_URL_COLUMN}`),
        `{MYSQL_ERROR_COLUMN}` = VALUES(`{MYSQL_ERROR_COLUMN}`),
        `updated_at` = CURRENT_TIMESTAMP
    """

    rows = [
        (
            str(item.get("video_id") or "").strip(),
            str(item.get("language_code") or "").strip(),
            str(item.get("s3_url") or "").strip(),
            str(item.get("error") or "").strip(),
        )
        for item in items
    ]

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            for row_chunk in chunk_list(rows, MYSQL_MAX_PACKET_SAFE_CHUNK):
                cursor.executemany(sql, row_chunk)

        connection.commit()
        return len(rows)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def bulk_update_mongo(items):
    if not items:
        return 0

    operations = []

    for item in items:
        video_id = str(item.get("video_id") or "").strip()
        srt_id = int(item.get("srt_id") or 3)

        if not video_id:
            continue

        operations.append(
            UpdateOne(
                {"video_id": video_id},
                {
                    "$set": {
                        "srt_id": srt_id,
                        "srt_updated_at": now_datetime_utc(),
                    }
                },
            )
        )

    if not operations:
        return 0

    result = source_coll.bulk_write(operations, ordered=False)
    return result.modified_count + result.upserted_count + result.matched_count


def get_payload_file_path(video_id):
    safe_video_id = re.sub(r"[^A-Za-z0-9_-]", "_", video_id)
    return LOCAL_PAYLOAD_DIR / f"{safe_video_id}.json"


def write_local_payload(payload):
    LOCAL_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_id = str(payload.get("video_id") or "").strip()
    file_path = get_payload_file_path(video_id)
    temp_file = file_path.with_name(f"{file_path.name}.{os.getpid()}.tmp")
    temp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    for attempt in range(10):
        try:
            os.replace(str(temp_file), str(file_path))
            return str(file_path)
        except PermissionError:
            time.sleep(0.5 * (attempt + 1))

    backup_file = file_path.with_name(f"{file_path.stem}.{epoch_seconds()}.backup.json")
    temp_file.replace(backup_file)
    raise PermissionError(f"Could not replace payload file after retries. Backup saved to {backup_file}")


def read_local_payload(file_path):
    return json.loads(Path(file_path).read_text(encoding="utf-8"))


def delete_local_payload(file_path):
    try:
        Path(file_path).unlink(missing_ok=True)
    except Exception:
        pass


def upload_caption_payload_to_s3(item):
    video_id = str(item.get("video_id") or "").strip()
    file_path = str(item.get("payload_path") or "").strip()
    payload = read_local_payload(file_path)

    data = {
        "video_id": str(payload.get("video_id") or "").strip(),
        "language_code": str(payload.get("language_code") or "").strip(),
        "language_name": str(payload.get("language_name") or "").strip(),
        "format": str(payload.get("format") or "").strip(),
        "content": str(payload.get("content") or "").strip(),
        "error": str(payload.get("error") or "").strip(),
    }

    temp_file = Path(file_path).with_name(f"{Path(file_path).stem}.s3.{os.getpid()}.tmp")
    temp_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    try:
        s3_key = get_transcript_s3_key(video_id)
        return upload_file_json_to_s3(temp_file, AWS_BUCKET_TRANSCRIPT, s3_key)
    finally:
        temp_file.unlink(missing_ok=True)


def append_pending_s3(checkpoint, item):
    video_id = str(item.get("video_id") or "").strip()
    checkpoint["pending_s3"] = [
        existing for existing in checkpoint.get("pending_s3", [])
        if str(existing.get("video_id") or "").strip() != video_id
    ]
    checkpoint["pending_s3"].append(item)
    save_checkpoint(checkpoint)


def replace_pending_s3(checkpoint, items):
    checkpoint["pending_s3"] = items
    save_checkpoint(checkpoint)


def append_pending_mysql(checkpoint, items):
    existing = {
        str(item.get("video_id") or "").strip(): item
        for item in checkpoint.get("pending_mysql", [])
    }

    for item in items:
        video_id = str(item.get("video_id") or "").strip()
        existing[video_id] = item

    checkpoint["pending_mysql"] = list(existing.values())
    save_checkpoint(checkpoint)


def replace_pending_mysql(checkpoint, items):
    checkpoint["pending_mysql"] = items
    save_checkpoint(checkpoint)


def append_pending_mongo(checkpoint, items):
    existing = {
        str(item.get("video_id") or "").strip(): item
        for item in checkpoint.get("pending_mongo", [])
    }

    for item in items:
        video_id = str(item.get("video_id") or "").strip()
        existing[video_id] = item

    checkpoint["pending_mongo"] = list(existing.values())
    save_checkpoint(checkpoint)


def replace_pending_mongo(checkpoint, items):
    checkpoint["pending_mongo"] = items
    save_checkpoint(checkpoint)


def flush_pending_s3(checkpoint):
    uploaded_count = 0
    last_error = ""
    remaining = []
    mysql_items = []
    mongo_items = []

    for item in list(checkpoint.get("pending_s3", [])):
        video_id = str(item.get("video_id") or "").strip()

        try:
            s3_url = upload_caption_payload_to_s3(item)

            mysql_items.append(
                {
                    "video_id": video_id,
                    "language_code": item.get("language_code", ""),
                    "s3_url": s3_url,
                    "error": item.get("error", ""),
                }
            )

            mongo_items.append(
                {
                    "video_id": video_id,
                    "srt_id": item.get("srt_id", 3),
                    "payload_path": item.get("payload_path", ""),
                }
            )

            uploaded_count += 1

        except Exception as e:
            last_error = str(e)
            remaining.append(item)
            checkpoint["pending_s3_last_error"] = last_error
            record_failed_video(checkpoint, video_id, last_error)

    replace_pending_s3(checkpoint, remaining)

    if mysql_items:
        append_pending_mysql(checkpoint, mysql_items)

    if mongo_items:
        append_pending_mongo(checkpoint, mongo_items)

    if not remaining:
        checkpoint["pending_s3_last_error"] = ""
        save_checkpoint(checkpoint)

    return uploaded_count, last_error


def flush_pending_mysql(checkpoint):
    items = list(checkpoint.get("pending_mysql", []))

    if not items:
        checkpoint["pending_mysql_last_error"] = ""
        save_checkpoint(checkpoint)
        return 0, ""

    try:
        saved_count = bulk_upsert_mysql(items)
        replace_pending_mysql(checkpoint, [])
        checkpoint["pending_mysql_last_error"] = ""
        save_checkpoint(checkpoint)
        return saved_count, ""
    except Exception as e:
        error = str(e)
        checkpoint["pending_mysql_last_error"] = error
        save_checkpoint(checkpoint)

        for item in items:
            record_failed_video(checkpoint, str(item.get("video_id") or "").strip(), error)

        return 0, error


def flush_pending_mongo(checkpoint):
    items = list(checkpoint.get("pending_mongo", []))

    if not items:
        checkpoint["pending_mongo_last_error"] = ""
        save_checkpoint(checkpoint)
        return 0, ""

    try:
        saved_count = bulk_update_mongo(items)

        for item in items:
            payload_path = str(item.get("payload_path") or "").strip()

            if payload_path:
                delete_local_payload(payload_path)

            clear_failed_video(checkpoint, str(item.get("video_id") or "").strip())

        replace_pending_mongo(checkpoint, [])
        checkpoint["pending_mongo_last_error"] = ""
        save_checkpoint(checkpoint)
        return saved_count, ""

    except Exception as e:
        error = str(e)
        checkpoint["pending_mongo_last_error"] = error
        save_checkpoint(checkpoint)

        for item in items:
            record_failed_video(checkpoint, str(item.get("video_id") or "").strip(), error)

        return 0, error


def flush_all_pending(checkpoint, upload_checkpoint_s3=False):
    uploaded_s3, s3_error = flush_pending_s3(checkpoint)
    saved_mysql, mysql_error = flush_pending_mysql(checkpoint)
    saved_mongo, mongo_error = flush_pending_mongo(checkpoint)

    if upload_checkpoint_s3:
        save_checkpoint(checkpoint, upload_s3=True)

    return {
        "uploaded_s3": uploaded_s3,
        "saved_mysql": saved_mysql,
        "saved_mongo": saved_mongo,
        "error": s3_error or mysql_error or mongo_error,
    }


def build_source_query(checkpoint):
    query = {
        "video_id": {
            "$exists": True,
            "$ne": None,
        }
    }

    last_mongo_id = str(checkpoint.get("last_mongo_id") or "").strip()

    if last_mongo_id:
        query["_id"] = {
            "$gt": ObjectId(last_mongo_id),
        }

    return query


def get_next_docs(checkpoint, limit):
    query = build_source_query(checkpoint)
    projection = {
        "_id": 1,
        "video_id": 1,
        "srt_id": 1,
    }

    return list(
        source_coll.find(
            query,
            projection,
        )
        .sort("_id", 1)
        .limit(limit)
    )


def process_video(doc, checkpoint):
    video_id = str(doc.get("video_id") or "").strip()
    mongo_id = str(doc.get("_id"))

    if should_skip(video_id):
        checkpoint["last_mongo_id"] = mongo_id
        checkpoint["last_video_id"] = video_id
        checkpoint["total_seen"] = int(checkpoint.get("total_seen", 0) or 0) + 1
        save_checkpoint(checkpoint)

        return {
            "status": "SKIPPED",
            "fetched_tikhub": 0,
            "checkpointed": 0,
            "error": "",
        }

    payload = get_video_captions(video_id)
    payload["video_id"] = video_id

    payload_path = write_local_payload(payload)

    pending_item = {
        "video_id": video_id,
        "language_code": payload.get("language_code", ""),
        "srt_id": payload.get("srt_id", 3),
        "error": payload.get("error", ""),
        "tikhub_status_code": payload.get("tikhub_status_code", 0),
        "payload_path": payload_path,
        "checkpointed_at": now_utc(),
    }

    append_pending_s3(checkpoint, pending_item)

    checkpoint["last_mongo_id"] = mongo_id
    checkpoint["last_video_id"] = video_id
    checkpoint["total_seen"] = int(checkpoint.get("total_seen", 0) or 0) + 1
    save_checkpoint(checkpoint)

    return {
        "status": "CHECKPOINTED",
        "fetched_tikhub": 1,
        "checkpointed": 1,
        "error": payload.get("error", ""),
    }


def main():
    LOCAL_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)

    checkpoint = load_checkpoint()
    started_at_epoch = epoch_seconds()

    run_processed = 0
    run_fetched_tikhub = 0
    run_checkpointed = 0
    run_uploaded_s3 = 0
    run_saved_mysql = 0
    run_saved_mongo = 0
    run_skipped = 0
    run_errors = 0

    cumulative_processed = int(checkpoint.get("cumulative_processed", 0) or 0)
    cumulative_fetched_tikhub = int(checkpoint.get("cumulative_fetched_tikhub", 0) or 0)
    cumulative_checkpointed = int(checkpoint.get("cumulative_checkpointed", 0) or 0)
    cumulative_uploaded_s3 = int(checkpoint.get("cumulative_uploaded_s3", 0) or 0)
    cumulative_saved_mysql = int(checkpoint.get("cumulative_saved_mysql", 0) or 0)
    cumulative_saved_mongo = int(checkpoint.get("cumulative_saved_mongo", 0) or 0)
    cumulative_skipped = int(checkpoint.get("cumulative_skipped", 0) or 0)
    cumulative_errors = int(checkpoint.get("cumulative_errors", 0) or 0)

    checkpoint["run_started_at"] = now_utc()
    checkpoint["status"] = "START"
    save_checkpoint(checkpoint, upload_s3=True)

    print_progress(
        "START",
        run_processed,
        run_fetched_tikhub,
        run_checkpointed,
        run_uploaded_s3,
        run_saved_mysql,
        run_saved_mongo,
        run_skipped,
        run_errors,
        len(checkpoint.get("pending_s3", [])),
        len(checkpoint.get("pending_mysql", [])),
        len(checkpoint.get("pending_mongo", [])),
        checkpoint.get("last_video_id", ""),
        started_at_epoch,
    )

    last_video_id = checkpoint.get("last_video_id", "")
    last_error = checkpoint.get("last_error", "")
    flush_counter = 0
    stopped_by_500 = False

    while True:
        remaining_limit = MONGO_READ_BATCH_SIZE

        if PROCESS_DATA > 0:
            remaining_total = PROCESS_DATA - run_processed

            if remaining_total <= 0:
                break

            remaining_limit = min(remaining_limit, remaining_total)

        docs = get_next_docs(checkpoint, remaining_limit)

        if not docs:
            break

        for doc in docs:
            video_id = str(doc.get("video_id") or "").strip()
            last_video_id = video_id

            try:
                checkpoint_set_status(checkpoint, "FETCHING_TIKHUB", video_id, "")

                result = process_video(doc, checkpoint)

                run_processed += 1
                cumulative_processed += 1

                run_fetched_tikhub += int(result.get("fetched_tikhub", 0) or 0)
                cumulative_fetched_tikhub += int(result.get("fetched_tikhub", 0) or 0)

                run_checkpointed += int(result.get("checkpointed", 0) or 0)
                cumulative_checkpointed += int(result.get("checkpointed", 0) or 0)

                if result.get("status") == "SKIPPED":
                    run_skipped += 1
                    cumulative_skipped += 1
                else:
                    flush_counter += 1

                if result.get("error"):
                    run_errors += 1
                    cumulative_errors += 1
                    record_failed_video(checkpoint, video_id, result.get("error", ""))
                else:
                    clear_failed_video(checkpoint, video_id)

                status = result.get("status", "CHECKPOINTED")
                last_error = result.get("error", "")

            except TikHubServerError as e:
                stopped_by_500 = True
                last_error = str(e)
                run_errors += 1
                cumulative_errors += 1
                checkpoint["stopped_by_500"] = True
                record_failed_video(checkpoint, video_id, last_error)
                checkpoint_set_status(checkpoint, "STOPPED_TIKHUB_500", video_id, last_error, upload_s3=True)
                break

            except Exception as e:
                mongo_id = str(doc.get("_id"))
                last_error = str(e)
                run_processed += 1
                cumulative_processed += 1
                run_errors += 1
                cumulative_errors += 1
                checkpoint["last_mongo_id"] = mongo_id
                checkpoint["last_video_id"] = video_id
                checkpoint["total_seen"] = int(checkpoint.get("total_seen", 0) or 0) + 1
                status = "ERROR"
                record_failed_video(checkpoint, video_id, last_error)

            if flush_counter >= FLUSH_BATCH_SIZE:
                checkpoint_set_status(checkpoint, "FLUSHING_PENDING", video_id, last_error)
                flush_result = flush_all_pending(
                    checkpoint,
                    upload_checkpoint_s3=CHECKPOINT_UPLOAD_EVERY_FLUSH,
                )

                run_uploaded_s3 += int(flush_result.get("uploaded_s3", 0) or 0)
                cumulative_uploaded_s3 += int(flush_result.get("uploaded_s3", 0) or 0)

                run_saved_mysql += int(flush_result.get("saved_mysql", 0) or 0)
                cumulative_saved_mysql += int(flush_result.get("saved_mysql", 0) or 0)

                run_saved_mongo += int(flush_result.get("saved_mongo", 0) or 0)
                cumulative_saved_mongo += int(flush_result.get("saved_mongo", 0) or 0)

                if flush_result.get("error"):
                    run_errors += 1
                    cumulative_errors += 1
                    last_error = flush_result.get("error", "")

                flush_counter = 0
                status = "FLUSHED_BATCH"

            checkpoint_set_status(checkpoint, status, video_id, last_error)

            update_run_counts(
                checkpoint,
                run_processed,
                run_fetched_tikhub,
                run_checkpointed,
                run_uploaded_s3,
                run_saved_mysql,
                run_saved_mongo,
                run_skipped,
                run_errors,
                cumulative_processed,
                cumulative_fetched_tikhub,
                cumulative_checkpointed,
                cumulative_uploaded_s3,
                cumulative_saved_mysql,
                cumulative_saved_mongo,
                cumulative_skipped,
                cumulative_errors,
            )

            print_progress(
                status,
                run_processed,
                run_fetched_tikhub,
                run_checkpointed,
                run_uploaded_s3,
                run_saved_mysql,
                run_saved_mongo,
                run_skipped,
                run_errors,
                len(checkpoint.get("pending_s3", [])),
                len(checkpoint.get("pending_mysql", [])),
                len(checkpoint.get("pending_mongo", [])),
                video_id,
                started_at_epoch,
                last_error,
            )

            if REQUEST_SLEEP_SECONDS > 0:
                time.sleep(REQUEST_SLEEP_SECONDS)

        if stopped_by_500:
            break

    if not stopped_by_500:
        checkpoint_set_status(checkpoint, "FINAL_FLUSHING_PENDING", last_video_id, last_error)
        flush_result = flush_all_pending(checkpoint, upload_checkpoint_s3=True)

        run_uploaded_s3 += int(flush_result.get("uploaded_s3", 0) or 0)
        cumulative_uploaded_s3 += int(flush_result.get("uploaded_s3", 0) or 0)

        run_saved_mysql += int(flush_result.get("saved_mysql", 0) or 0)
        cumulative_saved_mysql += int(flush_result.get("saved_mysql", 0) or 0)

        run_saved_mongo += int(flush_result.get("saved_mongo", 0) or 0)
        cumulative_saved_mongo += int(flush_result.get("saved_mongo", 0) or 0)

        if flush_result.get("error"):
            run_errors += 1
            cumulative_errors += 1
            last_error = flush_result.get("error", "")

    final_status = "DONE"

    if stopped_by_500:
        final_status = "STOPPED_TIKHUB_500"
    elif checkpoint.get("pending_s3") or checkpoint.get("pending_mysql") or checkpoint.get("pending_mongo"):
        final_status = "DONE_WITH_PENDING"
    elif run_errors > 0:
        final_status = "DONE_WITH_ERRORS"

    checkpoint_set_status(checkpoint, final_status, last_video_id, last_error, upload_s3=True)

    update_run_counts(
        checkpoint,
        run_processed,
        run_fetched_tikhub,
        run_checkpointed,
        run_uploaded_s3,
        run_saved_mysql,
        run_saved_mongo,
        run_skipped,
        run_errors,
        cumulative_processed,
        cumulative_fetched_tikhub,
        cumulative_checkpointed,
        cumulative_uploaded_s3,
        cumulative_saved_mysql,
        cumulative_saved_mongo,
        cumulative_skipped,
        cumulative_errors,
        upload_s3=True,
    )

    print_progress(
        final_status,
        run_processed,
        run_fetched_tikhub,
        run_checkpointed,
        run_uploaded_s3,
        run_saved_mysql,
        run_saved_mongo,
        run_skipped,
        run_errors,
        len(checkpoint.get("pending_s3", [])),
        len(checkpoint.get("pending_mysql", [])),
        len(checkpoint.get("pending_mongo", [])),
        last_video_id,
        started_at_epoch,
        last_error,
    )

    print(f"Saved checkpoint file: {CHECKPOINT_FILE}")
    print(f"Saved local payload dir: {LOCAL_PAYLOAD_DIR}")
    print(f"Saved S3 checkpoint: s3://{AWS_BUCKET_TRANSCRIPT}/{S3_CHECKPOINT_KEY}")


if __name__ == "__main__":
    try:
        main()
    finally:
        mongo_client.close()
