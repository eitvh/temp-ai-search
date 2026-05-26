import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pymysql
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY_2")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")

MYSQL_HOST = os.getenv("CRAWL_DB_HOST")
MYSQL_PORT = int(os.getenv("CRAWL_DB_PORT", "3306"))
MYSQL_USER = os.getenv("CRAWL_DB_USERNAME")
MYSQL_PASSWORD = os.getenv("CRAWL_DB_PASSWORD")
MYSQL_DATABASE = os.getenv("CRAWL_DB_DATABASE")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
AWS_BUCKET_TRANSCRIPT = os.getenv("AWS_BUCKET_TRANSCRIPT", "cloudbreakr-youtube")
AWS_PREFIX_TRANSCRIPT = os.getenv("AWS_PREFIX_TRANSCRIPT", "transcript").strip("/")

SOURCE_MYSQL_TABLE = os.getenv("SOURCE_MYSQL_TABLE", "ai_yt_info_v2")
SOURCE_MYSQL_ID_COLUMN = os.getenv("SOURCE_MYSQL_ID_COLUMN", "id")
SOURCE_VIDEO_ID_COLUMN = os.getenv("SOURCE_VIDEO_ID_COLUMN", "video_id")
SOURCE_LANGUAGE_CODE_COLUMN = os.getenv("SOURCE_LANGUAGE_CODE_COLUMN", "language_code")
SOURCE_S3_URL_COLUMN = os.getenv("SOURCE_S3_URL_COLUMN", "s3_url")

SUMMARY_MYSQL_TABLE = os.getenv("SUMMARY_MYSQL_TABLE", "ai_yt_info_v2_summary")

PROCESS_DATA = int(os.getenv("PROCESS_DATA", "1"))
READ_BATCH_SIZE = int(os.getenv("READ_BATCH_SIZE", "500"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0"))
FORCE_REPROCESS = os.getenv("FORCE_REPROCESS", "false").lower() == "true"

BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_FILE = BASE_DIR / "ai_yt_info_v2_summary_checkpoint.json"

if not GEMINI_API_KEY:
    raise ValueError("Missing GEMINI_API_KEY_2 in .env")

if not MYSQL_HOST:
    raise ValueError("Missing CRAWL_DB_HOST in .env")

if not MYSQL_USER:
    raise ValueError("Missing CRAWL_DB_USERNAME in .env")

if not MYSQL_PASSWORD:
    raise ValueError("Missing CRAWL_DB_PASSWORD in .env")

if not MYSQL_DATABASE:
    raise ValueError("Missing CRAWL_DB_DATABASE in .env")

if not AWS_BUCKET_TRANSCRIPT:
    raise ValueError("Missing AWS_BUCKET_TRANSCRIPT in .env")


class StopProcess(Exception):
    pass


def validate_identifier(value, name):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value or ""):
        raise ValueError(f"Invalid {name}")
    return value


SOURCE_MYSQL_TABLE = validate_identifier(SOURCE_MYSQL_TABLE, "SOURCE_MYSQL_TABLE")
SOURCE_MYSQL_ID_COLUMN = validate_identifier(SOURCE_MYSQL_ID_COLUMN, "SOURCE_MYSQL_ID_COLUMN")
SOURCE_VIDEO_ID_COLUMN = validate_identifier(SOURCE_VIDEO_ID_COLUMN, "SOURCE_VIDEO_ID_COLUMN")
SOURCE_LANGUAGE_CODE_COLUMN = validate_identifier(SOURCE_LANGUAGE_CODE_COLUMN, "SOURCE_LANGUAGE_CODE_COLUMN")
SOURCE_S3_URL_COLUMN = validate_identifier(SOURCE_S3_URL_COLUMN, "SOURCE_S3_URL_COLUMN")
SUMMARY_MYSQL_TABLE = validate_identifier(SUMMARY_MYSQL_TABLE, "SUMMARY_MYSQL_TABLE")

gemini_client = genai.Client(api_key=GEMINI_API_KEY)


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def epoch_seconds():
    return int(time.time())


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


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def empty_checkpoint():
    return {
        "status": "NEW",
        "gemini_model": GEMINI_MODEL,
        "source_mysql_table": SOURCE_MYSQL_TABLE,
        "summary_mysql_table": SUMMARY_MYSQL_TABLE,
        "process_data": PROCESS_DATA,
        "force_reprocess": FORCE_REPROCESS,
        "last_source_id": "",
        "last_video_id": "",
        "last_error": "",
        "run_started_at": "",
        "total_seen": 0,
        "run_processed": 0,
        "run_read_s3": 0,
        "run_analyzed_gemini": 0,
        "run_saved_mysql": 0,
        "run_skipped": 0,
        "run_errors": 0,
        "cumulative_processed": 0,
        "cumulative_read_s3": 0,
        "cumulative_analyzed_gemini": 0,
        "cumulative_saved_mysql": 0,
        "cumulative_skipped": 0,
        "cumulative_errors": 0,
        "pending_mysql": [],
        "pending_mysql_count": 0,
        "failed_video_errors": {},
        "stop_reason": "",
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

    if not isinstance(data.get("failed_video_errors"), dict):
        data["failed_video_errors"] = {}

    if not isinstance(data.get("pending_mysql"), list):
        data["pending_mysql"] = []

    data["pending_mysql_count"] = len(data.get("pending_mysql", []))
    data["gemini_model"] = GEMINI_MODEL

    return data


def save_checkpoint(data):
    data["updated_at"] = now_utc()
    data["pending_mysql_count"] = len(data.get("pending_mysql", []))
    data["gemini_model"] = GEMINI_MODEL
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


def checkpoint_set_status(checkpoint, status, video_id="", error=""):
    checkpoint["status"] = status
    checkpoint["last_video_id"] = video_id or checkpoint.get("last_video_id", "")
    checkpoint["last_error"] = error or ""
    save_checkpoint(checkpoint)


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


def append_pending_mysql(checkpoint, item):
    video_id = str(item.get("video_id") or "").strip()
    existing = {
        str(existing_item.get("video_id") or "").strip(): existing_item
        for existing_item in checkpoint.get("pending_mysql", [])
    }
    existing[video_id] = item
    checkpoint["pending_mysql"] = list(existing.values())
    save_checkpoint(checkpoint)


def replace_pending_mysql(checkpoint, items):
    checkpoint["pending_mysql"] = items
    save_checkpoint(checkpoint)


def short_error(error):
    text = str(error or "").replace("\n", " ").replace("\r", " ").strip()
    return text[:300]


def format_seconds(seconds):
    seconds = int(seconds or 0)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def print_progress(
    status,
    run_processed,
    run_read_s3,
    run_analyzed_gemini,
    run_saved_mysql,
    run_skipped,
    run_errors,
    pending_mysql_count,
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
        f"read_s3={run_read_s3} "
        f"analyzed_gemini={run_analyzed_gemini} "
        f"saved_mysql={run_saved_mysql} "
        f"skipped={run_skipped} "
        f"errors={run_errors} "
        f"pending_mysql={pending_mysql_count} "
        f"rate_per_minute={rate_per_minute} "
        f"elapsed={format_seconds(elapsed_seconds)} "
        f"video_id={video_id} "
        f"error={error_text}",
        flush=True,
    )


def get_default_s3_key(video_id):
    if AWS_PREFIX_TRANSCRIPT:
        return f"{AWS_PREFIX_TRANSCRIPT}/{video_id}.json"
    return f"{video_id}.json"


def parse_s3_location(video_id, s3_url=""):
    text = str(s3_url or "").strip()

    if text.startswith("s3://"):
        parsed = urlparse(text)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        host_parts = parsed.netloc.split(".")
        bucket = host_parts[0] if host_parts else AWS_BUCKET_TRANSCRIPT
        key = parsed.path.lstrip("/")
        return bucket, key

    if text:
        return AWS_BUCKET_TRANSCRIPT, text.lstrip("/")

    return AWS_BUCKET_TRANSCRIPT, get_default_s3_key(video_id)


def read_transcript_from_s3(video_id, s3_url=""):
    bucket, key = parse_s3_location(video_id, s3_url)
    response = get_s3_client().get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    payload = json.loads(body)

    if isinstance(payload, dict):
        content = str(payload.get("content") or "").strip()
        language_code = str(payload.get("language_code") or "").strip()
        language_name = str(payload.get("language_name") or "").strip()
        transcript_format = str(payload.get("format") or "").strip()
    else:
        content = ""
        language_code = ""
        language_name = ""
        transcript_format = ""

    return {
        "bucket": bucket,
        "key": key,
        "content": content,
        "language_code": language_code,
        "language_name": language_name,
        "format": transcript_format,
    }


def extract_json_object(text):
    text = str(text or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()

    start = text.find("{")
    end = text.rfind("}")

    if start >= 0 and end >= 0 and end > start:
        text = text[start:end + 1]

    return json.loads(text)


def empty_analysis():
    return {
        "video_title": "",
        "brand_mentioned": [],
        "product_mentioned": [],
        "topic_tags": [],
        "summary": "",
        "key_topics": [],
        "is_sponsorship": 0,
        "sponsorship_name": [],
    }


def normalize_list(value):
    if isinstance(value, list):
        return value

    if value in [None, ""]:
        return []

    return [value]


def dedupe_list(items):
    output = []
    seen = set()

    for item in normalize_list(items):
        key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item).strip().lower()

        if key and key not in seen:
            seen.add(key)
            output.append(item)

    return output


def normalize_analysis(data):
    result = empty_analysis()

    if isinstance(data, dict):
        for key in result:
            if key in data:
                result[key] = data[key]

    if not isinstance(result["video_title"], str):
        result["video_title"] = str(result["video_title"] or "")

    result["brand_mentioned"] = dedupe_list(result.get("brand_mentioned"))
    result["product_mentioned"] = dedupe_list(result.get("product_mentioned"))
    result["topic_tags"] = dedupe_list(result.get("topic_tags"))
    result["key_topics"] = normalize_list(result.get("key_topics"))
    result["sponsorship_name"] = dedupe_list(result.get("sponsorship_name"))

    if not isinstance(result["summary"], str):
        result["summary"] = str(result["summary"] or "")

    sponsorship_value = result.get("is_sponsorship", 0)

    if isinstance(sponsorship_value, bool):
        result["is_sponsorship"] = 1 if sponsorship_value else 0
    else:
        result["is_sponsorship"] = 1 if str(sponsorship_value).strip().lower() in ["1", "true", "yes"] else 0

    if result["is_sponsorship"] == 0:
        result["sponsorship_name"] = []

    return result


def is_stop_error(error):
    text = str(error or "").lower()

    stop_patterns = [
        "not_found",
        "not found",
        "no longer available",
        "model",
        "quota",
        "rate limit",
        "resource_exhausted",
        "permission",
        "unauthorized",
        "authentication",
        "api key",
        "deadline",
        "timeout",
        "connection",
        "network",
        "temporarily unavailable",
        "service unavailable",
        "internal",
        "503",
        "500",
        "502",
        "504",
    ]

    return any(pattern in text for pattern in stop_patterns)


def gemini_json(prompt):
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json",
            ),
        )
        return extract_json_object(response.text)
    except Exception as e:
        raise StopProcess(str(e))


def build_summary_prompt(video_id, transcript_content):
    return f"""
You are analyzing a YouTube video transcript in SRT format.

Return ONLY valid JSON.
No markdown.
No code fences.
No explanation.
No extra text.

The transcript may be in English, Chinese, Malay, Indonesian, Korean, Japanese, Thai, Vietnamese, or mixed languages.
Always output JSON field names in English.
For topic tags, summaries, key topics, and labels, output in English unless a brand name or product name must stay in its original form.

Analyze the whole YouTube video transcript from start to finish as one complete source.
Focus only on what is actually discussed, explained, shown, demonstrated, reviewed, compared, promoted, or clearly mentioned in the transcript.

Video ID: {video_id}

IMPORTANT GROUNDING RULES:
- Use ONLY information clearly supported by the transcript.
- Do NOT hallucinate brands, products, sponsorships, claims, benefits, people, places, events, or timestamps.
- Do NOT treat a brand or product mention as sponsorship by default.
- Do NOT infer sponsorship only because a product is reviewed, recommended, compared, used, visited, eaten, shown, or mentioned.
- Sponsorship requires promotional, partnership, advertising, affiliate, gifted, paid, discount, campaign, collaboration, invited, hosted, provided, or brand-supported context.
- The transcript is in SRT format and already contains timestamps.
- For key topic timestamps, use the start timestamp from the relevant SRT subtitle block.
- Output timestamps in HH:MM:SS format.
- If the SRT timestamp includes milliseconds, remove milliseconds.
- If the SRT timestamp is MM:SS, convert it to HH:MM:SS.
- If a timestamp cannot be found, use null.
- If information is not found, return [] or "" depending on the field.
- Deduplicate repeated brands, products, tags, topics, and subtopics.
- Preserve official brand/product capitalization where possible.
- Infer video_title only from the transcript content. If a clear title cannot be inferred, return "".

TASK A: Generate Video Title

Generate a short video_title in English based only on the transcript.
The title should be useful for search and review.
Do not invent names, places, brands, or claims not supported by the transcript.

TASK B: Extract Brands Mentioned

Extract only actual commercial brands, company names, shop brands, app brands, product brands, restaurant chain brands, store chain brands, platform brands, or service brands that are explicitly mentioned in the transcript.

Rules:
- Output brand names in brand_mentioned.
- Include a brand only if the transcript explicitly says the brand name.
- Do NOT guess brands from context.
- Do NOT include tourist attractions, places, landmarks, galleries, aquariums, parks, malls, neighborhoods, districts, streets, train stations, transport exits, cities, countries, or buildings as brands.
- Do NOT include hotel, restaurant, cafe, attraction, gallery, aquarium, or mall names in brand_mentioned if they are used mainly as visited places or locations in the video.
- Include restaurant chain, store chain, or product company names only when they are clearly brands, not just locations.
- Do NOT include generic product categories as brands.
- Do NOT include food names as brands.
- Do NOT include people unless they are clearly brand/company names.
- If a brand is mentioned many times, output it once.
- If no actual brand is mentioned, return [].

TASK C: Extract Products Mentioned

Extract specific commercial product names, product lines, apps, named services, packages, plans, or named offerings mentioned in the transcript.

Rules:
- Output product names in product_mentioned.
- Include only named commercial products or named services that are clearly mentioned in the transcript.
- Do NOT include ordinary food items as products.
- Do NOT include generic food names unless they are part of a formal branded product name.
- Do NOT include generic travel or location categories unless they are part of a formal named commercial product, package, plan, or service.
- Do NOT invent products just because a brand is mentioned.
- If a product is mentioned many times, output it once.
- If no valid product name is mentioned, return [].

TASK D: Generate Topic Tags

Generate at least 20 topic tags.

Rules:
- Generate a minimum of 20 tags.
- Generate more than 20 only if the transcript clearly supports more.
- Every tag must be directly supported by the transcript.
- Each tag should be 1 to 4 words.
- Use Title Case.
- Use only letters A-Z/a-z, digits 0-9, and spaces.
- No emojis.
- No punctuation.
- No hashtags.
- No duplicate or near-duplicate tags.
- Do not output random keywords.
- Do not output only place names as tags.
- Place names can be included only when paired with a topic or activity.
- Avoid generic filler tags like Video, YouTube, Content, Trending, General Topic, Creator Content, Social Media.
- Prefer stable topic phrases over one-off words.

TASK E: Generate Summary

Create a concise but useful summary of the full video.

Rules:
- The summary must cover the whole video from beginning to end.
- The summary must be based only on the transcript.
- Keep it clear and user-friendly.
- Do not exaggerate claims.
- Do not add medical, financial, legal, or product claims that are not stated.
- Length: 2 to 5 sentences.
- If the video is a vlog, summarize the overall trip flow, main locations, activities, and ending.
- If the video is a review, explain what is being reviewed and the overall discussion.
- If the video is a tutorial, explain what the viewer learns.
- If the transcript is too short, summarize only what is clearly available.

TASK F: Extract Key Topics With Timestamps

Create a structured topic map of the video.

Rules:
- Create up to 10 key topics.
- The final output must never contain fewer than 1 key_topics unless the transcript has no meaningful content.
- Each topic must be a broad theme that groups several meaningful moments from the transcript.
- Each topic must still be specific to the video.
- Topic names should be useful as UI section headers and references for users.
- Topic names should sound like polished topic phrases, not short action labels.
- Topic names should use Title Case.
- Do not use generic topic names like Introduction, Overview, Content, Summary, Discussion, Conclusion, Speaker, Part 1, Part 2.
- Do not create topics that are only place names unless paired with a meaningful activity or theme.
- Each topic must contain up to 10 subtopics.
- Each subtopic must include subtopic, timestamp, and detail.
- The subtopic field must be a descriptive phrase, not a vague label.
- The detail field must be 2 to 4 complete sentences.
- Do not repeat the same subtopic under different topics.
- Do not repeat the same timestamp for every subtopic unless the transcript truly only provides one relevant timestamp.
- Timestamps should be spread across the video when possible.

TASK G: Detect Sponsorship

Determine whether the YouTube video is sponsored.

Output:
- is_sponsorship must be 1 or 0.
- sponsorship_name must list the brand, product, service, app, campaign, or company involved in the sponsorship.
- If is_sponsorship is 0, sponsorship_name must be [].

Set is_sponsorship = 1 only when the transcript includes clear sponsorship, advertising, partnership, affiliate, gifted, paid, discount, campaign, collaboration, invited, hosted, provided, or brand-supported context.

Strong sponsorship signals:
- sponsored by
- thanks to
- in partnership with
- partnered with
- paid partnership
- ad
- advertisement
- this video is sponsored
- sponsor of today's video
- brought to you by
- collaborated with
- collaboration with
- ambassador
- affiliate
- gifted by
- PR package
- sent by
- provided by
- hosted by
- invited by
- complimentary stay
- complimentary meal
- media invite
- press invite
- use my code
- discount code
- promo code
- referral code
- affiliate link
- link in description
- special offer
- limited offer
- exclusive deal
- get discount
- sign up using
- check out Brand
- buy from Brand
- shop Brand
- download Brand app
- book with Brand
- visit Brand website
- order from Brand
- use Brand service
- today's sponsor
- this portion is sponsored
- our sponsor

Weak signals that are NOT enough by themselves:
- Mentioning a brand
- Reviewing a product
- Comparing products
- Saying a product is good or bad
- Saying I bought this
- Saying I use this
- Mentioning where something was purchased
- Visiting a hotel, mall, restaurant, tourist attraction, gallery, or aquarium
- Showing a product or location without promotional language
- Organic recommendation without CTA, code, partnership, gifted, invited, hosted, provided, or sponsor wording

OUTPUT JSON SCHEMA EXACTLY:

{{
  "video_title": "",
  "brand_mentioned": [],
  "product_mentioned": [],
  "topic_tags": [],
  "summary": "",
  "key_topics": [
    {{
      "topic": "",
      "subtopics": [
        {{
          "subtopic": "",
          "timestamp": null,
          "detail": ""
        }}
      ]
    }}
  ],
  "is_sponsorship": 0,
  "sponsorship_name": []
}}

SRT Transcript:
{transcript_content}
""".strip()


def generate_summary(video_id, transcript_content):
    if not str(transcript_content or "").strip():
        analysis = empty_analysis()
        return analysis, "NO_TRANSCRIPT"

    prompt = build_summary_prompt(video_id, transcript_content)
    data = gemini_json(prompt)
    analysis = normalize_analysis(data)

    return analysis, ""


def json_value(value):
    return json.dumps(value if value is not None else [], ensure_ascii=False)


def get_existing_summary(video_id):
    sql = f"""
    SELECT `id`, `video_id`
    FROM `{SUMMARY_MYSQL_TABLE}`
    WHERE `video_id` = %s
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

    row = get_existing_summary(video_id)

    if row:
        return True

    return False


def save_summary_mysql(item):
    analysis = normalize_analysis(item.get("analysis", {}))

    sql = f"""
    INSERT INTO `{SUMMARY_MYSQL_TABLE}` (
        `video_id`,
        `video_title`,
        `brand_mentioned`,
        `product_mentioned`,
        `topic_tags`,
        `summary`,
        `key_topics`,
        `is_sponsorship`,
        `sponsorship_name`,
        `error`
    )
    VALUES (%s, %s, CAST(%s AS JSON), CAST(%s AS JSON), CAST(%s AS JSON), %s, CAST(%s AS JSON), %s, CAST(%s AS JSON), %s)
    ON DUPLICATE KEY UPDATE
        `video_title` = VALUES(`video_title`),
        `brand_mentioned` = VALUES(`brand_mentioned`),
        `product_mentioned` = VALUES(`product_mentioned`),
        `topic_tags` = VALUES(`topic_tags`),
        `summary` = VALUES(`summary`),
        `key_topics` = VALUES(`key_topics`),
        `is_sponsorship` = VALUES(`is_sponsorship`),
        `sponsorship_name` = VALUES(`sponsorship_name`),
        `error` = VALUES(`error`),
        `updated_at` = CURRENT_TIMESTAMP
    """

    row = (
        str(item.get("video_id") or "").strip(),
        str(analysis.get("video_title") or "").strip(),
        json_value(analysis.get("brand_mentioned", [])),
        json_value(analysis.get("product_mentioned", [])),
        json_value(analysis.get("topic_tags", [])),
        str(analysis.get("summary") or "").strip(),
        json_value(analysis.get("key_topics", [])),
        int(analysis.get("is_sponsorship") or 0),
        json_value(analysis.get("sponsorship_name", [])),
        str(item.get("error") or "").strip(),
    )

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, row)
        connection.commit()
        return 1
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def flush_pending_mysql(checkpoint):
    items = list(checkpoint.get("pending_mysql", []))

    if not items:
        return 0

    saved_count = 0
    remaining = []

    for item in items:
        try:
            saved_count += save_summary_mysql(item)
        except Exception as e:
            remaining.append(item)
            checkpoint["last_error"] = str(e)
            checkpoint["stop_reason"] = "MYSQL_ERROR"
            replace_pending_mysql(checkpoint, remaining)
            raise StopProcess(str(e))

    replace_pending_mysql(checkpoint, [])
    return saved_count


def get_next_docs(checkpoint):
    last_source_id = str(checkpoint.get("last_source_id") or "").strip()

    where_parts = [
        f"`{SOURCE_VIDEO_ID_COLUMN}` IS NOT NULL",
        f"`{SOURCE_VIDEO_ID_COLUMN}` <> ''",
        f"`{SOURCE_LANGUAGE_CODE_COLUMN}` IN ('en', 'a.en')",
        f"`{SOURCE_S3_URL_COLUMN}` IS NOT NULL",
        f"`{SOURCE_S3_URL_COLUMN}` <> ''",
    ]

    params = []

    if last_source_id:
        where_parts.append(f"`{SOURCE_MYSQL_ID_COLUMN}` > %s")
        params.append(last_source_id)

    where_sql = " AND ".join(where_parts)

    sql = f"""
    SELECT
        `{SOURCE_MYSQL_ID_COLUMN}` AS source_id,
        `{SOURCE_VIDEO_ID_COLUMN}` AS video_id,
        `{SOURCE_LANGUAGE_CODE_COLUMN}` AS language_code,
        `{SOURCE_S3_URL_COLUMN}` AS s3_url
    FROM `{SOURCE_MYSQL_TABLE}`
    WHERE {where_sql}
    ORDER BY `{SOURCE_MYSQL_ID_COLUMN}` ASC
    LIMIT %s
    """

    params.append(READ_BATCH_SIZE)

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        connection.close()


def process_video(doc, checkpoint):
    source_id = str(doc.get("source_id") or "").strip()
    video_id = str(doc.get("video_id") or "").strip()
    s3_url = str(doc.get("s3_url") or "").strip()

    if not video_id:
        return {
            "status": "SKIPPED",
            "read_s3": 0,
            "analyzed_gemini": 0,
            "saved_mysql": 0,
            "error": "Missing video_id",
        }

    if should_skip(video_id):
        checkpoint["last_source_id"] = source_id
        checkpoint["last_video_id"] = video_id
        checkpoint["total_seen"] = int(checkpoint.get("total_seen", 0) or 0) + 1
        save_checkpoint(checkpoint)

        return {
            "status": "SKIPPED",
            "read_s3": 0,
            "analyzed_gemini": 0,
            "saved_mysql": 0,
            "error": "",
        }

    s3_payload = read_transcript_from_s3(video_id, s3_url)
    content = str(s3_payload.get("content") or "").strip()

    analysis, error = generate_summary(video_id, content)

    pending_item = {
        "video_id": video_id,
        "analysis": analysis,
        "error": error,
        "checkpointed_at": now_utc(),
    }

    append_pending_mysql(checkpoint, pending_item)

    checkpoint["last_source_id"] = source_id
    checkpoint["last_video_id"] = video_id
    checkpoint["total_seen"] = int(checkpoint.get("total_seen", 0) or 0) + 1
    save_checkpoint(checkpoint)

    saved_mysql = flush_pending_mysql(checkpoint)

    return {
        "status": "PROCESSED" if not error else "PROCESSED_WITH_ERROR",
        "read_s3": 1,
        "analyzed_gemini": 1 if not error else 0,
        "saved_mysql": saved_mysql,
        "error": error,
    }


def update_run_counts(
    checkpoint,
    run_processed,
    run_read_s3,
    run_analyzed_gemini,
    run_saved_mysql,
    run_skipped,
    run_errors,
    cumulative_processed,
    cumulative_read_s3,
    cumulative_analyzed_gemini,
    cumulative_saved_mysql,
    cumulative_skipped,
    cumulative_errors,
):
    checkpoint["run_processed"] = run_processed
    checkpoint["run_read_s3"] = run_read_s3
    checkpoint["run_analyzed_gemini"] = run_analyzed_gemini
    checkpoint["run_saved_mysql"] = run_saved_mysql
    checkpoint["run_skipped"] = run_skipped
    checkpoint["run_errors"] = run_errors
    checkpoint["cumulative_processed"] = cumulative_processed
    checkpoint["cumulative_read_s3"] = cumulative_read_s3
    checkpoint["cumulative_analyzed_gemini"] = cumulative_analyzed_gemini
    checkpoint["cumulative_saved_mysql"] = cumulative_saved_mysql
    checkpoint["cumulative_skipped"] = cumulative_skipped
    checkpoint["cumulative_errors"] = cumulative_errors
    save_checkpoint(checkpoint)


def main():
    checkpoint = load_checkpoint()
    started_at_epoch = epoch_seconds()

    run_processed = 0
    run_read_s3 = 0
    run_analyzed_gemini = 0
    run_saved_mysql = 0
    run_skipped = 0
    run_errors = 0

    cumulative_processed = int(checkpoint.get("cumulative_processed", 0) or 0)
    cumulative_read_s3 = int(checkpoint.get("cumulative_read_s3", 0) or 0)
    cumulative_analyzed_gemini = int(checkpoint.get("cumulative_analyzed_gemini", 0) or 0)
    cumulative_saved_mysql = int(checkpoint.get("cumulative_saved_mysql", 0) or 0)
    cumulative_skipped = int(checkpoint.get("cumulative_skipped", 0) or 0)
    cumulative_errors = int(checkpoint.get("cumulative_errors", 0) or 0)

    checkpoint["run_started_at"] = now_utc()
    checkpoint["status"] = "START"
    checkpoint["stop_reason"] = ""
    save_checkpoint(checkpoint)

    print_progress(
        "START",
        run_processed,
        run_read_s3,
        run_analyzed_gemini,
        run_saved_mysql,
        run_skipped,
        run_errors,
        len(checkpoint.get("pending_mysql", [])),
        checkpoint.get("last_video_id", ""),
        started_at_epoch,
    )

    last_video_id = checkpoint.get("last_video_id", "")
    last_error = checkpoint.get("last_error", "")
    stopped = False

    try:
        if checkpoint.get("pending_mysql"):
            checkpoint_set_status(checkpoint, "FLUSHING_PENDING_MYSQL", last_video_id, "")
            saved = flush_pending_mysql(checkpoint)
            run_saved_mysql += saved
            cumulative_saved_mysql += saved

        while True:
            if PROCESS_DATA > 0 and run_processed >= PROCESS_DATA:
                break

            docs = get_next_docs(checkpoint)

            if not docs:
                break

            for doc in docs:
                if PROCESS_DATA > 0 and run_processed >= PROCESS_DATA:
                    break

                video_id = str(doc.get("video_id") or "").strip()
                source_id = str(doc.get("source_id") or "").strip()
                last_video_id = video_id

                try:
                    checkpoint_set_status(checkpoint, "PROCESSING", video_id, "")

                    result = process_video(doc, checkpoint)

                    run_processed += 1
                    cumulative_processed += 1

                    run_read_s3 += int(result.get("read_s3", 0) or 0)
                    cumulative_read_s3 += int(result.get("read_s3", 0) or 0)

                    run_analyzed_gemini += int(result.get("analyzed_gemini", 0) or 0)
                    cumulative_analyzed_gemini += int(result.get("analyzed_gemini", 0) or 0)

                    run_saved_mysql += int(result.get("saved_mysql", 0) or 0)
                    cumulative_saved_mysql += int(result.get("saved_mysql", 0) or 0)

                    if result.get("status") == "SKIPPED":
                        run_skipped += 1
                        cumulative_skipped += 1

                    if result.get("error"):
                        run_errors += 1
                        cumulative_errors += 1
                        record_failed_video(checkpoint, video_id, result.get("error", ""))
                    else:
                        clear_failed_video(checkpoint, video_id)

                    status = result.get("status", "PROCESSED")
                    last_error = result.get("error", "")

                except StopProcess as e:
                    last_error = str(e)
                    stopped = True
                    run_errors += 1
                    cumulative_errors += 1
                    checkpoint["last_source_id"] = source_id
                    checkpoint["last_video_id"] = video_id
                    checkpoint["stop_reason"] = "STOPPED_IMMEDIATE_ERROR"
                    record_failed_video(checkpoint, video_id, last_error)
                    status = "STOPPED_IMMEDIATE_ERROR"

                except Exception as e:
                    last_error = str(e)
                    stopped = True
                    run_errors += 1
                    cumulative_errors += 1
                    checkpoint["last_source_id"] = source_id
                    checkpoint["last_video_id"] = video_id
                    checkpoint["stop_reason"] = "STOPPED_IMMEDIATE_ERROR"
                    record_failed_video(checkpoint, video_id, last_error)
                    status = "STOPPED_IMMEDIATE_ERROR"

                checkpoint_set_status(checkpoint, status, video_id, last_error)

                update_run_counts(
                    checkpoint,
                    run_processed,
                    run_read_s3,
                    run_analyzed_gemini,
                    run_saved_mysql,
                    run_skipped,
                    run_errors,
                    cumulative_processed,
                    cumulative_read_s3,
                    cumulative_analyzed_gemini,
                    cumulative_saved_mysql,
                    cumulative_skipped,
                    cumulative_errors,
                )

                print_progress(
                    status,
                    run_processed,
                    run_read_s3,
                    run_analyzed_gemini,
                    run_saved_mysql,
                    run_skipped,
                    run_errors,
                    len(checkpoint.get("pending_mysql", [])),
                    video_id,
                    started_at_epoch,
                    last_error,
                )

                if stopped:
                    break

                if REQUEST_SLEEP_SECONDS > 0:
                    time.sleep(REQUEST_SLEEP_SECONDS)

            if stopped:
                break

    except StopProcess as e:
        last_error = str(e)
        stopped = True
        run_errors += 1
        cumulative_errors += 1
        checkpoint["stop_reason"] = "STOPPED_IMMEDIATE_ERROR"
        checkpoint_set_status(checkpoint, "STOPPED_IMMEDIATE_ERROR", last_video_id, last_error)

    final_status = "STOPPED_IMMEDIATE_ERROR" if stopped else "DONE_WITH_ERRORS" if run_errors > 0 else "DONE"

    checkpoint_set_status(checkpoint, final_status, last_video_id, last_error)

    update_run_counts(
        checkpoint,
        run_processed,
        run_read_s3,
        run_analyzed_gemini,
        run_saved_mysql,
        run_skipped,
        run_errors,
        cumulative_processed,
        cumulative_read_s3,
        cumulative_analyzed_gemini,
        cumulative_saved_mysql,
        cumulative_skipped,
        cumulative_errors,
    )

    print_progress(
        final_status,
        run_processed,
        run_read_s3,
        run_analyzed_gemini,
        run_saved_mysql,
        run_skipped,
        run_errors,
        len(checkpoint.get("pending_mysql", [])),
        last_video_id,
        started_at_epoch,
        last_error,
    )

    print(f"Saved checkpoint file: {CHECKPOINT_FILE}")
    print(f"Saved MySQL table: {SUMMARY_MYSQL_TABLE}")
    print(f"Gemini model: {GEMINI_MODEL}")


if __name__ == "__main__":
    main()
