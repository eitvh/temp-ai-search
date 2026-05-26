import json
import os
from datetime import datetime, timezone

import pymysql
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from pymongo import MongoClient, UpdateOne

load_dotenv()

CRAWL_DB_HOST = os.getenv("CRAWL_DB_HOST")
CRAWL_DB_PORT = int(os.getenv("CRAWL_DB_PORT"))
CRAWL_DB_DATABASE = os.getenv("CRAWL_DB_DATABASE")
CRAWL_DB_USERNAME = os.getenv("CRAWL_DB_USERNAME")
CRAWL_DB_PASSWORD = os.getenv("CRAWL_DB_PASSWORD")

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT"))
ELASTICSEARCH_SCHEME = os.getenv("ELASTICSEARCH_SCHEME")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASS = os.getenv("ELASTICSEARCH_PASS")

MONGO_URI_ATLAS = os.getenv("MONGO_URI_ATLAS")
MONGO_DB_ATLAS = os.getenv("MONGO_DB_ATLAS")
MONGO_COLL_ATLAS_YOUTUBE = os.getenv("MONGO_COLL_ATLAS_YOUTUBE")

INDEX_NAME = "youtube_video_details"

LOCATION_ID = 1
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

# 0 = process all matching posts
# Any number > 0 = limit total saved posts
PROCESS_DATA = 0

ES_SCROLL_BATCH_SIZE = 1000
MONGO_BULK_SIZE = 500

CHECKPOINT_FILE = os.path.join(
    os.path.dirname(__file__),
    "get-post.checkpoint.json"
)

EXCLUDED_CHANNEL_IDS = {
    "UC6of7UYhctnYmqABjUqzuxw",
    "UCiYZw0h6hA5ENlPhTZFTHTA",
    "UC0XODJg0WPxMqPA5bCGAQlw",
    "UCoAf_IAZKhnUb0AajE5VSyA",
    "UCbmZSNWyEoM2xtWhbHRPo3w",
    "UCAQsetoOYIHCbceCTJuu5sg",
    "UCW77C9aYkZv_i4Rt6cQW_kA",
    "UCU-sdeH9IMsk_IBOtIFGYFg",
    "UCpxjO7o1McAQR0XLZHxfqWg",
    "UCOsFUU8EtJGDd6-AouF_MwQ",
    "UCqz7Q8gOavVi_ZiGHePvLug",
    "UCKh2nOrPXCW4nXuhr_ZTIzg",
    "UCmMV_kEiVCf8Mgsep0Ncfqw",
    "UClNwEARJfRxBZ4hplFEHRlA",
    "UCXf8jlTSP9kp6g4ROCfgvbQ",
    "UCr_L9cZdbBU_XDsKDHBBlew",
    "UCUqHVR5kzBR4zjoof3PWP_g",
    "UCLYWo70xBDrPYJgJsxoX7Qg",
    "UCQRdJCdUVkXYPvWZ6_Omkag",
    "UCFgvtFvTFRjqhGC1XfIB02w",
    "UClRaYVzu5DIh3kRZkXIX9WA",
}

db = pymysql.connect(
    host=CRAWL_DB_HOST,
    port=CRAWL_DB_PORT,
    user=CRAWL_DB_USERNAME,
    password=CRAWL_DB_PASSWORD,
    database=CRAWL_DB_DATABASE,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

es = Elasticsearch(
    hosts=[
        {
            "host": ELASTICSEARCH_HOST,
            "port": ELASTICSEARCH_PORT,
            "scheme": ELASTICSEARCH_SCHEME,
        }
    ],
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASS),
)

mongo_client = MongoClient(MONGO_URI_ATLAS)
mongo_db = mongo_client[MONGO_DB_ATLAS]
mongo_coll = mongo_db[MONGO_COLL_ATLAS_YOUTUBE]


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def epoch_millis_to_iso(value):
    if value is None:
        return None

    dt = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {
            "location_id": LOCATION_ID,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "channel_index": 0,
            "processed_channels": 0,
            "saved_posts": 0,
            "updated_at": now_utc(),
        }

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if (
        data.get("location_id") != LOCATION_ID
        or data.get("start_date") != START_DATE
        or data.get("end_date") != END_DATE
    ):
        return {
            "location_id": LOCATION_ID,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "channel_index": 0,
            "processed_channels": 0,
            "saved_posts": 0,
            "updated_at": now_utc(),
        }

    return data


def save_checkpoint(channel_index, processed_channels, saved_posts):
    data = {
        "location_id": LOCATION_ID,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "channel_index": channel_index,
        "processed_channels": processed_channels,
        "saved_posts": saved_posts,
        "updated_at": now_utc(),
    }

    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def print_progress(
    current_channel_index,
    total_channels,
    processed_channels,
    saved_posts,
    current_channel_id="",
    current_channel_posts=0,
    status="RUNNING",
):
    progress_pct = 0

    if total_channels > 0:
        progress_pct = round((processed_channels / total_channels) * 100, 2)

    line = (
        f"[{now_utc()}] "
        f"status={status} "
        f"location_id={LOCATION_ID} "
        f"channels={processed_channels}/{total_channels} "
        f"progress={progress_pct}% "
        f"saved_posts={saved_posts} "
        f"current_channel_index={current_channel_index} "
        f"current_channel_id={current_channel_id} "
        f"current_channel_posts={current_channel_posts}"
    )

    print(line, flush=True)


def get_channel_ids():
    query = """
    SELECT DISTINCT ycl.youtube_channel_id
    FROM youtube_channel_location ycl
    INNER JOIN youtube_channel_bios ycb ON ycb.channel_id = ycl.youtube_channel_id
    WHERE ycl.locationId = %s
    AND ycl.youtube_channel_id REGEXP '^UC'
    AND ycl.youtube_channel_id NOT IN %s
    ORDER BY ycl.youtube_channel_id
    """

    with db.cursor() as cursor:
        cursor.execute(query, (LOCATION_ID, tuple(EXCLUDED_CHANNEL_IDS)))
        rows = cursor.fetchall()

    return [
        row["youtube_channel_id"]
        for row in rows
        if row.get("youtube_channel_id")
    ]


def to_mongo_doc(source):
    published_at_ts = source.get("publishedAt")

    return {
        "channel_id": source.get("channel_id"),
        "video_id": source.get("video_id"),
        "location_id": LOCATION_ID,
        "stats": {
            "publishedAt": epoch_millis_to_iso(published_at_ts),
            "publishedAt_ts": published_at_ts,
            "likeCount": source.get("likeCount"),
            "dislikeCount": source.get("dislikeCount"),
            "commentCount": source.get("commentCount"),
            "viewCount": source.get("viewCount"),
            "favoriteCount": source.get("favoriteCount"),
        },
    }


def bulk_upsert_docs(docs):
    if not docs:
        return 0

    operations = []

    for doc in docs:
        operations.append(
            UpdateOne(
                {"video_id": doc["video_id"]},
                {"$set": doc},
                upsert=True,
            )
        )

    mongo_coll.bulk_write(operations, ordered=False)

    return len(docs)


def fetch_posts_for_channel(channel_id, remaining_limit=None):
    body = {
        "size": ES_SCROLL_BATCH_SIZE,
        "_source": [
            "channel_id",
            "video_id",
            "publishedAt",
            "likeCount",
            "dislikeCount",
            "commentCount",
            "viewCount",
            "favoriteCount",
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "channel_id": [channel_id]
                        }
                    },
                    {
                        "range": {
                            "publishedAt": {
                                "format": "strict_date_optional_time",
                                "gte": f"{START_DATE}T00:00:00.000Z",
                                "lte": f"{END_DATE}T23:59:59.000Z",
                            }
                        }
                    },
                ],
                "must_not": [
                    {
                        "terms": {
                            "channel_id": list(EXCLUDED_CHANNEL_IDS)
                        }
                    }
                ],
            }
        },
        "sort": [
            {"publishedAt": "desc"},
            {"video_id": "asc"},
        ],
    }

    response = es.search(index=INDEX_NAME, body=body, scroll="2m")
    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]

    while hits:
        for hit in hits:
            yield hit["_source"]

            if remaining_limit is not None:
                remaining_limit -= 1

                if remaining_limit <= 0:
                    if scroll_id:
                        try:
                            es.clear_scroll(scroll_id=scroll_id)
                        except Exception:
                            pass

                    return

        response = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]

    if scroll_id:
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass


def main():
    checkpoint = load_checkpoint()
    channel_ids = get_channel_ids()
    total_channels = len(channel_ids)

    start_index = checkpoint.get("channel_index", 0)
    processed_channels = checkpoint.get("processed_channels", 0)
    saved_posts = checkpoint.get("saved_posts", 0)

    if start_index >= total_channels:
        print_progress(
            start_index,
            total_channels,
            processed_channels,
            saved_posts,
            status="DONE",
        )
        return

    print_progress(
        start_index,
        total_channels,
        processed_channels,
        saved_posts,
        status="START",
    )

    total_limit = None if PROCESS_DATA == 0 else PROCESS_DATA
    pending_docs = []

    for idx in range(start_index, total_channels):
        channel_id = channel_ids[idx]
        channel_post_count = 0
        remaining_limit = None

        if total_limit is not None:
            remaining_limit = total_limit - saved_posts

            if remaining_limit <= 0:
                save_checkpoint(idx, processed_channels, saved_posts)

                print_progress(
                    idx,
                    total_channels,
                    processed_channels,
                    saved_posts,
                    current_channel_id=channel_id,
                    current_channel_posts=channel_post_count,
                    status="DONE",
                )

                return

        for source in fetch_posts_for_channel(
            channel_id,
            remaining_limit=remaining_limit,
        ):
            doc = to_mongo_doc(source)
            pending_docs.append(doc)
            channel_post_count += 1

            if (
                len(pending_docs) >= MONGO_BULK_SIZE
                or (
                    total_limit is not None
                    and len(pending_docs) >= remaining_limit
                )
            ):
                inserted = bulk_upsert_docs(pending_docs)
                saved_posts += inserted
                pending_docs = []

                print_progress(
                    idx + 1,
                    total_channels,
                    processed_channels,
                    saved_posts,
                    current_channel_id=channel_id,
                    current_channel_posts=channel_post_count,
                )

            if total_limit is not None and saved_posts >= total_limit:
                if pending_docs:
                    inserted = bulk_upsert_docs(pending_docs)
                    saved_posts += inserted
                    pending_docs = []

                processed_channels += 1

                save_checkpoint(idx + 1, processed_channels, saved_posts)

                print_progress(
                    idx + 1,
                    total_channels,
                    processed_channels,
                    saved_posts,
                    current_channel_id=channel_id,
                    current_channel_posts=channel_post_count,
                    status="DONE",
                )

                return

        if pending_docs:
            inserted = bulk_upsert_docs(pending_docs)
            saved_posts += inserted
            pending_docs = []

        processed_channels += 1

        save_checkpoint(idx + 1, processed_channels, saved_posts)

        print_progress(
            idx + 1,
            total_channels,
            processed_channels,
            saved_posts,
            current_channel_id=channel_id,
            current_channel_posts=channel_post_count,
        )

    print_progress(
        total_channels,
        total_channels,
        processed_channels,
        saved_posts,
        status="DONE",
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close()
        mongo_client.close()
