import os
import sys
import time
import json
import re
import random
import threading
from typing import List, Dict, Any, Tuple, Set, Iterable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId

from google import genai
from google.genai import types as genai_types

load_dotenv()

MONGO_URI_ENV = "MONGO_URI_ATLAS_TEST"
MONGO_DB_ENV = "MONGO_DB_ATLAS_TEST"
MONGO_COLL_ENV = "MONGO_COLL_ATLAS_TEST"

LOCATION_ID = 1
MAX_DOCS = 0

MYSQL_TABLE = "_test_topic_tag_transit"
MYSQL_SOURCE_TABLE = "ig_post"

GEMINI_MODEL = "gemini-2.5-flash-lite-preview-09-2025"

MONGO_SCAN_BATCH = 500
MYSQL_CHECK_CHUNK = 400
MYSQL_UPSERT_BATCH = 200
MYSQL_SOURCE_FETCH_CHUNK = 400

CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".gemini_batch_ckpt")

MAX_MODEL_RETRIES = 9

MAX_INFLIGHT = 30
INFLIGHT_MIN = 1
INFLIGHT_STEP_DOWN = 5
INFLIGHT_STEP_UP = 2
CHUNK_QUOTA_FAIL_DOWN_THRESHOLD = 2
CHUNK_NO_QUOTA_UP_THRESHOLD = 0

MAX_RETRY_ATTEMPTS = 3

REPAIR_MYSQL_BATCH = 1000
REPAIR_REQUIRE_EXACT_10 = True

MONGO_SERVER_SELECTION_TIMEOUT_MS = 12000
MONGO_CONNECT_TIMEOUT_MS = 12000
MONGO_SOCKET_TIMEOUT_MS = 120000

MYSQL_READ_TIMEOUT = 90
MYSQL_WRITE_TIMEOUT = 90

HEARTBEAT_SECONDS = 10
FREEZE_WARN_SECONDS = 25

CKPT_REPLACE_MAX_SECONDS = 120
CKPT_REPLACE_BACKOFF_BASE = 0.2
CKPT_REPLACE_BACKOFF_MAX = 2.5

PROMPT_TEXT = (
    "You are analyzing an Instagram post caption (it may include transcript-like text).\n"
    "Return ONLY valid JSON, no markdown, no extra text.\n"
    "Even if transcript is non English, output tags in English.\n\n"
    "Task A: Generate exactly 10 topic tags in English.\n\n"
    "Goal:\n"
    "Produce high-level, stable THEMES for content discovery. Tags must be specific enough to be useful, but not random words.\n\n"
    "Hard grounding rules (no hallucination):\n"
    "- Every tag MUST be directly supported by the caption text.\n"
    "- If the caption does not clearly support a tag, DO NOT output it.\n"
    "- Do NOT invent names, roles, locations, dates, or events.\n"
    "- Do NOT copy arbitrary phrases; prefer normalized themes.\n\n"
    "Tag style rules:\n"
    "- Exactly 10 tags.\n"
    "- Each tag is 1 to 4 words.\n"
    "- Use only letters A-Z/a-z, digits 0-9, and spaces.\n"
    "- No emojis, no punctuation, no symbols, no hashtags, no non-Latin characters.\n"
    "- Use Title Case for all tags.\n"
    "- Avoid filler tags like General Topic, Lifestyle, Trending Topic, Pop Culture, Creator Content, Daily Life, Social Media, Video Content, Entertainment.\n"
    "- Avoid meta tags about the post format unless clearly central (e.g., Vlog, Tutorial, Review, Behind The Scenes).\n\n"
    "How to choose tags:\n"
    "1) First infer ONE main theme and 2 to 4 secondary themes from the caption.\n"
    "2) Prefer tags from these theme types when present:\n"
    "   - Activity or domain: Travel, Fitness, Cooking, Fashion, Music, Photography, Technology, TV Drama\n"
    "   - Place or region ONLY if explicitly stated: Tokyo, Japan, Hong Kong\n"
    "   - Media or genre ONLY if explicit: Travel Vlog, Sitcom, Kpop\n"
    "   - Food or item ONLY if explicit: Ramen, Coffee, Tailoring, Suit\n"
    "   - Occasion ONLY if explicit: Christmas, New Year\n"
    "   - Emotion or self-development ONLY if explicit and central: Mindfulness, Personal Growth\n"
    "3) If the caption is too short or vague, output broader but still grounded tags like: Daily Update, Personal Thoughts, Work Life, Family Moment, Travel Update, Food Moment, Music Clip, TV Show.\n\n"
    "Quality constraints:\n"
    "- Tags must be distinct (no near-duplicates).\n"
    "- Do not include Part One, Part Two, Episode Numbers, or similar sequencing.\n"
    "- Do not include single letters or single generic words like Nice, Happy, Good.\n"
    "- If a brand is mentioned, you may include a brand-related tag ONLY if it represents the content theme (e.g., Nike Running), otherwise omit.\n\n"
    "Task B: Detect sponsorship.\n"
    "is_sponsorship MUST be either 1 (true) or 0 (false).\n\n"
    "Goal: In real Instagram captions, sponsorship is often IMPLIED. You must not be overly conservative.\n"
    "Set is_sponsorship=1 when the caption is promoting a brand/product/service and includes a brand reference.\n\n"
    "Primary rule (most common case):\n"
    "Set is_sponsorship=1 if associated_brand is NOT empty AND the caption contains ANY promotional intent.\n\n"
    "Promotional intent includes ANY of the following (treat as strong):\n"
    "- Call to action: shop, order, buy, purchase, pre order, available now, drop, launch, link in bio, check out, tap, click, swipe, DM to order, dm me, inbox, whatsapp, book now, booking, appointment, reserve\n"
    "- Price or offer: $, %, off, discount, promo, voucher, deal, sale, limited time, special offer, free delivery, code, use code, referral\n"
    "- Product/service push: try this, must have, highly recommend, new product, new menu, new treatment, package, set, combo, best seller\n"
    "- Stock/location: available at, now at, in store, online, website, hotline\n"
    "- Brand shoutout patterns: 感謝, 多謝, thanks, thank you, shoutout followed by a brand/handle\n"
    "- Partnership words: ad, sponsored, paid partnership, collab, collaboration, partnered with, ambassador, affiliate, gifted, PR, sent me\n\n"
    "Secondary rule (when no associated_brand found):\n"
    "Set is_sponsorship=1 if brand_name is NOT empty AND the caption contains ANY of:\n"
    "- explicit partnership words (ad, sponsored, paid partnership, collab, ambassador, affiliate, gifted, PR)\n"
    "- a promo mechanic (code, discount, voucher, referral, link in bio)\n"
    "- a clear call to action (shop, order, book now, DM to order, whatsapp)\n\n"
    "Do NOT set is_sponsorship=1 only for:\n"
    "- Pure personal opinion with no promotion (e.g., 'I love Nike shoes')\n"
    "- Pure event attendance or generic tags with no selling/CTA\n\n"
    "Important bias rule:\n"
    "If there is ANY doubt and there is a brand reference (brand_name or associated_brand) PLUS any promotional intent word, choose is_sponsorship=1.\n\n"
    "Task C: Extract Brand Names.\n"
    "- Identify the formal names of brands mentioned in the caption (e.g., 'Nike', 'Disney', 'Coca-Cola').\n"
    "- These are natural language names, not handles.\n"
    "- Output as a list of strings in 'brand_name'.\n\n"
    "Task D: Extract Instagram usernames that are missing the '@' symbol and separate BRANDS vs INFLUENCERS.\n"
    "Act as a Social Media Data Miner. Your goal is to extract Instagram usernames missing '@'.\n"
    "You must be extremely strict to avoid extracting normal words, names, or titles.\n\n"
    "Candidate extraction rules:\n"
    "- Only consider tokens that are ALL LOWERCASE and contain '_' or '.' (e.g., 'elsie_lui', 'thegrand_hk').\n"
    "- Also extract if two lowercase tokens appear together (cluster pattern), e.g., 'bakerybythegrand thegrand_hk'.\n"
    "- Also extract lowercase tokens immediately after '同' or '感謝' if they look like usernames and are not dictionary words.\n\n"
    "Exclusion rules (CRITICAL):\n"
    "- If a word starts with a Capital Letter, it is a proper noun/name; DO NOT extract.\n"
    "- Ignore event names and common nouns.\n"
    "- Ignore single common first names.\n\n"
    "Now classify each extracted username into one of two lists:\n"
    "1) associated_brand: brand/company/shop/venue/product/service accounts.\n"
    "2) associated_mention: influencer/creator/personal accounts.\n\n"
    "Classification heuristics (use strict best-effort):\n"
    "- Put into associated_brand if the username contains business/brand indicators such as: hk, hkg, official, shop, store, mall, hotel, restaurant, cafe, bar, dining, studio, salon, clinic, spa, beauty, skincare, makeup, cosmetics, fashion, jewelry, watch, travel, tours, airline, bank, insurance, comms, pr, agency, media, group, ltd, co, company, brand, boutique, bakery, kitchen, grill, izakaya, ramen, pizza, coffee, tea, dessert, sports, football, adidas, nike, puma, uniqlo, disney, hermes, dior, sephora, zeiss, owndays, bvlgari.\n"
    "- Put into associated_brand if the caption context around it is promotional/brand-like: 'shop', 'link in bio', 'code', 'discount', 'book', 'reservation', 'available at', 'now at', 'menu', 'treatment', 'package', 'launch', 'drop'.\n"
    "- Put into associated_mention if the username looks like a person/creator: contains a personal name pattern, or creator indicators such as: mua, makeupartist, artist, photographer, photo, videographer, editor, stylist, hair, nails, coach, trainer, dancer, actor, singer, model, dj, yoga.\n"
    "- If uncertain, default to associated_mention unless there is a clear brand/business indicator.\n\n"
    "Output requirements:\n"
    "- Output extracted usernames WITHOUT leading '@'.\n"
    "- Each entry must be ONE token with NO spaces.\n"
    "- Allowed characters: letters a-z, digits 0-9, underscore _, dot ., and hyphen -.\n"
    "- Deduplicate per list (case-insensitive dedupe OK, output first-seen form).\n"
    "- If none, output empty list [].\n\n"
    "Output schema exactly:\n"
    "{\"tags\":[\"tag1\",...,\"tag10\"],\"is_sponsorship\":0/1,\"brand_name\":[\"name1\",...],\"associated_brand\":[\"handle1\",...],\"associated_mention\":[\"handle1\",...]}\n\n"
    "<transcript>\n{TRANSCRIPT}\n</transcript>"
)

_GENERIC_ONLY = {"hello", "hi", "hey", "welcome", "thanks", "thank", "thank you", "subscribe", "follow"}
_ALLOWED_RE = re.compile(r"^[A-Za-z0-9 ]+$")
_HANDLE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")

_status_lock = threading.Lock()
_status: Dict[str, Any] = {
    "phase": "init",
    "phase_detail": "",
    "last_activity_ts": time.time(),
    "last_progress_ts": time.time(),
    "processed_total": 0,
    "scanned_total": 0,
    "inflight": MAX_INFLIGHT,
    "failed_keys": 0,
}

def _touch_activity(phase: Optional[str] = None, detail: Optional[str] = None) -> None:
    with _status_lock:
        if phase is not None:
            _status["phase"] = phase
        if detail is not None:
            _status["phase_detail"] = detail
        _status["last_activity_ts"] = time.time()

def _touch_progress(processed_total: Optional[int] = None, scanned_total: Optional[int] = None, failed_keys: Optional[int] = None, inflight: Optional[int] = None) -> None:
    with _status_lock:
        _status["last_progress_ts"] = time.time()
        if processed_total is not None:
            _status["processed_total"] = int(processed_total)
        if scanned_total is not None:
            _status["scanned_total"] = int(scanned_total)
        if failed_keys is not None:
            _status["failed_keys"] = int(failed_keys)
        if inflight is not None:
            _status["inflight"] = int(inflight)

def _heartbeat_loop() -> None:
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        now = time.time()
        with _status_lock:
            phase = str(_status.get("phase") or "")
            detail = str(_status.get("phase_detail") or "")
            last_a = float(_status.get("last_activity_ts") or 0)
            last_p = float(_status.get("last_progress_ts") or 0)
            processed = int(_status.get("processed_total") or 0)
            scanned = int(_status.get("scanned_total") or 0)
            inflight = int(_status.get("inflight") or 0)
            fk = int(_status.get("failed_keys") or 0)
        idle_a = int(now - last_a)
        idle_p = int(now - last_p)
        if idle_p >= FREEZE_WARN_SECONDS:
            print(f"[watch] no_progress_for={idle_p}s phase={phase} detail={detail} processed={processed} scanned={scanned} inflight={inflight} failed_keys={fk}", file=sys.stderr, flush=True)
        else:
            print(f"[watch] alive phase={phase} detail={detail} processed={processed} scanned={scanned} inflight={inflight} failed_keys={fk} idle={idle_a}s", file=sys.stderr, flush=True)

def _env_host() -> str:
    return os.getenv("CRAWL_DB_HOST") or os.getenv("crawl_db_hostname") or "localhost"

def _env_port() -> int:
    raw = os.getenv("CRAWL_DB_PORT") or os.getenv("db_port") or "3306"
    return int(raw)

def _env_user() -> str:
    return os.getenv("CRAWL_DB_USERNAME") or os.getenv("crawl_db_username") or ""

def _env_pass() -> str:
    return os.getenv("CRAWL_DB_PASSWORD") or os.getenv("crawl_db_password") or ""

def _env_db() -> str:
    return os.getenv("CRAWL_DB_DATABASE") or os.getenv("crawl_db_name") or ""

def _mongo_uri() -> str:
    return (os.getenv(MONGO_URI_ENV) or "").strip()

def _mongo_db() -> str:
    return (os.getenv(MONGO_DB_ENV) or "").strip()

def _mongo_coll() -> str:
    return (os.getenv(MONGO_COLL_ENV) or "").strip()

def _connect_mysql() -> pymysql.connections.Connection:
    return pymysql.connect(
        port=_env_port(),
        user=_env_user(),
        passwd=_env_pass(),
        host=_env_host(),
        db=_env_db(),
        charset="utf8mb4",
        autocommit=True,
        use_unicode=True,
        cursorclass=DictCursor,
        connect_timeout=12,
        read_timeout=MYSQL_READ_TIMEOUT,
        write_timeout=MYSQL_WRITE_TIMEOUT,
    )

def _fmt_secs(s: float) -> str:
    s = int(s)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"

def _fmt_rate_docs_per_min(docs_per_sec: float) -> str:
    if docs_per_sec <= 0:
        return "0.0/min"
    return f"{docs_per_sec * 60.0:.1f}/min"

def _fmt_eta(remaining_docs: Optional[int], docs_per_sec: float) -> str:
    if remaining_docs is None or remaining_docs <= 0 or docs_per_sec <= 0:
        return "unknown"
    return _fmt_secs(remaining_docs / docs_per_sec)

def _is_meaningful(text: str) -> bool:
    if not isinstance(text, str):
        return False
    s = (text or "").strip()
    if not s:
        return False
    if s.lower() == "nan":
        return False
    if not any(ch.isalpha() for ch in s):
        return False
    low = s.strip().lower()
    if low in _GENERIC_ONLY:
        return False
    return True

def fix_camelcase(text: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text or "")

def _extract_json(text: str) -> Any:
    s = (text or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        pass
    starts = [i for i in (s.find("["), s.find("{")) if i != -1]
    if not starts:
        return None
    start = min(starts)
    last_sq = s.rfind("]")
    last_br = s.rfind("}")
    end = max(last_sq, last_br)
    if end != -1 and end > start:
        try:
            return json.loads(s[start:end + 1])
        except Exception:
            pass
    stack = []
    for idx in range(start, len(s)):
        ch = s[idx]
        if ch in "[{":
            stack.append(ch)
        elif ch in "]}":
            if stack and ((stack[-1] == "[" and ch == "]") or (stack[-1] == "{" and ch == "}")):
                stack.pop()
        if not stack and idx > start:
            try:
                return json.loads(s[start:idx + 1])
            except Exception:
                break
    return None

def _only_ascii_words_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"[^A-Za-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_phrase(s: str, max_words: int) -> str:
    s = _only_ascii_words_spaces(s)
    if not s:
        return ""
    parts = s.split()
    if len(parts) > max_words:
        parts = parts[:max_words]
    s = " ".join(parts).strip()
    if not s:
        return ""
    if not _ALLOWED_RE.match(s):
        return ""
    return s

def normalize_tags(tags: List[str]) -> List[str]:
    cleaned: List[str] = []
    seen = set()
    for t in tags:
        t = _normalize_phrase(str(t), 4)
        if not t:
            continue
        k = t.lower()
        if k in _GENERIC_ONLY:
            continue
        if k in seen:
            continue
        seen.add(k)
        cleaned.append(t)
        if len(cleaned) >= 10:
            break
    return cleaned

def normalize_brands(brands: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for b in brands or []:
        b2 = _normalize_phrase(str(b), 6)
        if not b2:
            continue
        k = b2.lower()
        if k in {"ad", "sponsored", "sponsor", "shop", "link", "code", "promo", "discount", "affiliate"}:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(b2)
        if len(out) >= 12:
            break
    return out

def normalize_handles(handles: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for h in handles or []:
        s = str(h or "").strip()
        if not s:
            continue
        s = s.strip("()[]{}<>\"'“”‘’.,:;!?")
        if not s:
            continue
        if not _HANDLE_RE.match(s):
            continue
        k = s.lower()
        if k in {"link", "code"}:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
        if len(out) >= 30:
            break
    return out

def parse_payload(raw: str) -> Tuple[List[str], int, List[str], List[str], List[str]]:
    obj = _extract_json(raw)
    if not isinstance(obj, dict):
        return [], 0, [], [], []
    tags = obj.get("tags")
    is_s = obj.get("is_sponsorship")
    brand_name = obj.get("brand_name")
    assoc_brand = obj.get("associated_brand")
    assoc_mention = obj.get("associated_mention")
    tags_list = tags if isinstance(tags, list) else []
    brand_list = brand_name if isinstance(brand_name, list) else []
    assoc_brand_list = assoc_brand if isinstance(assoc_brand, list) else []
    assoc_mention_list = assoc_mention if isinstance(assoc_mention, list) else []
    if isinstance(is_s, bool):
        is_s_bool = is_s
    elif isinstance(is_s, (int, float)):
        is_s_bool = int(is_s) == 1
    else:
        s = str(is_s or "").strip().lower()
        is_s_bool = s in {"1", "true", "yes", "y", "t"}
    tags_out = normalize_tags([str(x) for x in tags_list])
    brand_out = normalize_brands(brand_list)
    assoc_brand_out = normalize_handles(assoc_brand_list)
    assoc_mention_out = normalize_handles(assoc_mention_list)
    return tags_out, (1 if is_s_bool else 0), brand_out, assoc_brand_out, assoc_mention_out

def _tags_to_mysql_text(tags: Any) -> str:
    if isinstance(tags, list):
        return str(tags)
    if isinstance(tags, str):
        s = tags.strip()
        if s.startswith("[") and s.endswith("]"):
            return s
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return str(obj)
        except Exception:
            pass
    return str(tags)

def _chunks(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]

def _fetch_existing_pairs(pairs: List[Tuple[str, str]]) -> Set[Tuple[str, str]]:
    if not pairs:
        return set()
    _touch_activity("mysql", f"check_existing size={len(pairs)}")
    found: Set[Tuple[str, str]] = set()
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            for chunk in _chunks(pairs, MYSQL_CHECK_CHUNK):
                placeholders = ",".join(["(%s,%s)"] * len(chunk))
                sql = f"""
                    SELECT user_id, post_id
                    FROM `{MYSQL_TABLE}`
                    WHERE (user_id, post_id) IN ({placeholders})
                """
                params: List[Any] = []
                for u, p in chunk:
                    params.extend([u, p])
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
                for r in rows:
                    found.add((str(r.get("user_id") or ""), str(r.get("post_id") or "")))
    finally:
        conn.close()
    return found

def _mysql_fetch_content_map(pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    if not pairs:
        return {}
    _touch_activity("mysql", f"fetch_source_content size={len(pairs)}")
    out: Dict[Tuple[str, str], str] = {}
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            for chunk in _chunks(pairs, MYSQL_SOURCE_FETCH_CHUNK):
                placeholders = ",".join(["(%s,%s)"] * len(chunk))
                sql = f"""
                    SELECT
                        CAST(ig_user_id AS CHAR) AS user_id,
                        CAST(ig_post_id AS CHAR) AS post_id,
                        content
                    FROM `{MYSQL_SOURCE_TABLE}`
                    WHERE (ig_user_id, ig_post_id) IN ({placeholders})
                    ORDER BY postDate DESC
                """
                params: List[Any] = []
                for u, p in chunk:
                    params.extend([u, p])
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
                for r in rows:
                    u = str(r.get("user_id") or "").strip()
                    p = str(r.get("post_id") or "").strip()
                    c = str(r.get("content") or "")
                    if not u or not p:
                        continue
                    k = (u, p)
                    if k in out:
                        continue
                    out[k] = c
    finally:
        conn.close()
    return out

def _attach_mysql_content(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not docs:
        return []
    pairs = []
    seen = set()
    for d in docs:
        u = str(d.get("user_id") or "").strip()
        p = str(d.get("post_id") or "").strip()
        if not u or not p:
            continue
        k = (u, p)
        if k in seen:
            continue
        seen.add(k)
        pairs.append(k)
    content_map = _mysql_fetch_content_map(pairs)
    out: List[Dict[str, Any]] = []
    for d in docs:
        u = str(d.get("user_id") or "").strip()
        p = str(d.get("post_id") or "").strip()
        if not u or not p:
            continue
        nd = dict(d)
        nd["content"] = str(content_map.get((u, p)) or "")
        out.append(nd)
    return out

def _build_docs_from_pairs_with_mysql_content(pairs: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    seen = set()
    for u, p in pairs:
        u2 = str(u or "").strip()
        p2 = str(p or "").strip()
        if not u2 or not p2:
            continue
        k = (u2, p2)
        if k in seen:
            continue
        seen.add(k)
        docs.append({"_id": "", "user_id": u2, "post_id": p2})
    return _attach_mysql_content(docs)

def _upsert_topic_tags(rows: List[Tuple[str, str, str, int, Optional[str], Optional[str], Optional[str]]]):
    if not rows:
        return
    _touch_activity("mysql", f"upsert size={len(rows)}")
    sql = f"""
        INSERT INTO `{MYSQL_TABLE}` (user_id, post_id, topic_tags, is_sponsorship, brand_name, associated_brand, associated_mention)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            topic_tags = VALUES(topic_tags),
            is_sponsorship = VALUES(is_sponsorship),
            brand_name = VALUES(brand_name),
            associated_brand = VALUES(associated_brand),
            associated_mention = VALUES(associated_mention)
    """
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    finally:
        conn.close()

def _ckpt_path(*parts: str) -> str:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, *parts)

def _write_json_atomic(path: str, obj: Any, phase_label: str) -> None:
    _touch_activity("ckpt", f"{phase_label}:write_tmp")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        try:
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            pass
    start = time.time()
    attempt = 0
    while True:
        attempt += 1
        _touch_activity("ckpt", f"{phase_label}:replace attempt={attempt}")
        try:
            os.replace(tmp, path)
            _touch_activity("ckpt", f"{phase_label}:replaced")
            return
        except Exception as e:
            elapsed = time.time() - start
            if elapsed >= CKPT_REPLACE_MAX_SECONDS:
                fallback = path + ".fallback.json"
                _touch_activity("ckpt", f"{phase_label}:fallback_write")
                try:
                    with open(fallback, "w", encoding="utf-8") as ff:
                        json.dump(obj, ff, ensure_ascii=False)
                        try:
                            ff.flush()
                            os.fsync(ff.fileno())
                        except Exception:
                            pass
                    print(f"[ckpt] replace_blocked elapsed={int(elapsed)}s wrote_fallback={fallback}", file=sys.stderr, flush=True)
                except Exception as e2:
                    print(f"[ckpt] replace_blocked elapsed={int(elapsed)}s fallback_failed={str(e2)}", file=sys.stderr, flush=True)
                try:
                    os.remove(tmp)
                except Exception:
                    pass
                return
            wait = min(CKPT_REPLACE_BACKOFF_MAX, (CKPT_REPLACE_BACKOFF_BASE * (1.3 ** attempt)) + random.random() * 0.2)
            print(f"[ckpt] replace_retry attempt={attempt} sleep={wait:.2f}s err={str(e)}", file=sys.stderr, flush=True)
            time.sleep(wait)

def _load_state() -> Dict[str, Any]:
    p = _ckpt_path("state.json")
    if not os.path.exists(p):
        return {
            "failed_keys": [],
            "failed_attempts": {},
            "dead_keys": [],
            "mongo_resume_after_id": None,
            "processed_total": 0,
            "scanned_total": 0,
            "mongo_total_candidates": None,
        }
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return {
                "failed_keys": [],
                "failed_attempts": {},
                "dead_keys": [],
                "mongo_resume_after_id": None,
                "processed_total": 0,
                "scanned_total": 0,
                "mongo_total_candidates": None,
            }
        if "failed_keys" not in obj or not isinstance(obj["failed_keys"], list):
            obj["failed_keys"] = []
        if "failed_attempts" not in obj or not isinstance(obj["failed_attempts"], dict):
            obj["failed_attempts"] = {}
        if "dead_keys" not in obj or not isinstance(obj["dead_keys"], list):
            obj["dead_keys"] = []
        if "mongo_resume_after_id" not in obj:
            obj["mongo_resume_after_id"] = None
        if "processed_total" not in obj:
            obj["processed_total"] = 0
        if "scanned_total" not in obj:
            obj["scanned_total"] = 0
        if "mongo_total_candidates" not in obj:
            obj["mongo_total_candidates"] = None
        return obj
    except Exception:
        return {
            "failed_keys": [],
            "failed_attempts": {},
            "dead_keys": [],
            "mongo_resume_after_id": None,
            "processed_total": 0,
            "scanned_total": 0,
            "mongo_total_candidates": None,
        }

def _save_state(state: Dict[str, Any]) -> None:
    p = _ckpt_path("state.json")
    _write_json_atomic(p, state, "state")

def _load_repair_state() -> Dict[str, Any]:
    p = _ckpt_path("repair_state.json")
    if not os.path.exists(p):
        return {"last_user_id": None, "last_post_id": None, "repaired_ok": 0, "repaired_failed": 0, "started_at": int(time.time())}
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return {"last_user_id": None, "last_post_id": None, "repaired_ok": 0, "repaired_failed": 0, "started_at": int(time.time())}
        for k in ["last_user_id", "last_post_id", "repaired_ok", "repaired_failed", "started_at"]:
            if k not in obj:
                obj[k] = None if k in {"last_user_id", "last_post_id"} else 0
        return obj
    except Exception:
        return {"last_user_id": None, "last_post_id": None, "repaired_ok": 0, "repaired_failed": 0, "started_at": int(time.time())}

def _save_repair_state(state: Dict[str, Any]) -> None:
    p = _ckpt_path("repair_state.json")
    _write_json_atomic(p, state, "repair_state")

def _make_key(uid: str, pid: str) -> str:
    return f"{uid}::{pid}"

def _split_key(k: str) -> Tuple[str, str]:
    if "::" in k:
        a, b = k.split("::", 1)
        return a, b
    return "", ""

def _prompt_for_text(text: str) -> str:
    return PROMPT_TEXT.replace("{TRANSCRIPT}", text)

def _extract_text_from_response_obj(resp_obj: Any) -> str:
    if resp_obj is None:
        return ""
    t = getattr(resp_obj, "text", None)
    if isinstance(t, str) and t.strip():
        return t.strip()
    try:
        d = resp_obj.model_dump() if hasattr(resp_obj, "model_dump") else None
    except Exception:
        d = None
    if isinstance(d, dict):
        try:
            cands = d.get("candidates") or []
            if not cands:
                return ""
            parts = (((cands[0] or {}).get("content") or {}).get("parts") or [])
            out = []
            for p in parts:
                tx = p.get("text")
                if isinstance(tx, str) and tx.strip():
                    out.append(tx.strip())
            return "\n".join(out).strip()
        except Exception:
            return ""
    return ""

def _normalize_result_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return fix_camelcase(s)

def _process_pending_rows(rows: List[Tuple[str, str, str, int, Optional[str], Optional[str], Optional[str]]]) -> int:
    if not rows:
        return 0
    n = 0
    buf: List[Tuple[str, str, str, int, Optional[str], Optional[str], Optional[str]]] = []
    for r in rows:
        buf.append(r)
        if len(buf) >= MYSQL_UPSERT_BATCH:
            _upsert_topic_tags(buf)
            n += len(buf)
            buf = []
    if buf:
        _upsert_topic_tags(buf)
        n += len(buf)
    return n

def _ensure_client() -> genai.Client:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        raise SystemExit("Missing GEMINI_API_KEY (or GOOGLE_API_KEY).")
    return genai.Client(api_key=key)

def _is_quota_error(e: Exception) -> bool:
    m = str(e).lower()
    return ("resource_exhausted" in m) or ("429" in m) or ("quota" in m) or ("rate" in m)

def _is_transient_error(e: Exception) -> bool:
    m = str(e).lower()
    return ("deadline" in m) or ("timeout" in m) or ("unavailable" in m) or ("temporarily" in m) or ("connection" in m)

def _backoff_sleep(attempt: int) -> float:
    return min(900.0, (1.3 * (2 ** attempt)) + random.random() * 1.5)

def _mongo_client() -> MongoClient:
    uri = _mongo_uri()
    if not uri:
        raise SystemExit(f"Missing Mongo env: {MONGO_URI_ENV}")
    return MongoClient(
        uri,
        serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
        connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        socketTimeoutMS=MONGO_SOCKET_TIMEOUT_MS,
    )

_mongo_client_singleton: Optional[MongoClient] = None

def _mongo_coll_handle() -> Any:
    global _mongo_client_singleton
    uri = _mongo_uri()
    dbn = _mongo_db()
    cn = _mongo_coll()
    if not uri or not dbn or not cn:
        raise SystemExit(f"Missing Mongo envs: {MONGO_URI_ENV}, {MONGO_DB_ENV}, {MONGO_COLL_ENV}")
    if _mongo_client_singleton is None:
        _touch_activity("mongo", "connect")
        _mongo_client_singleton = _mongo_client()
    return _mongo_client_singleton[dbn][cn]

def _mongo_count_total_candidates(location_id: int) -> int:
    _touch_activity("mongo", "count_total_candidates")
    coll = _mongo_coll_handle()
    q = {
        "locationId": int(location_id),
        "user_id": {"$exists": True, "$ne": None, "$ne": ""},
        "post_id": {"$exists": True, "$ne": None, "$ne": ""}
    }
    return int(coll.count_documents(q))

def _mongo_iter_docs(location_id: int, resume_after_id: Optional[str]) -> Iterable[Dict[str, Any]]:
    _touch_activity("mongo", f"scan_iter resume_after_id={resume_after_id or ''}")
    coll = _mongo_coll_handle()
    q: Dict[str, Any] = {
        "locationId": int(location_id),
        "user_id": {"$exists": True, "$ne": None, "$ne": ""},
        "post_id": {"$exists": True, "$ne": None, "$ne": ""}
    }
    if resume_after_id:
        try:
            q["_id"] = {"$gt": ObjectId(str(resume_after_id))}
        except Exception:
            pass
    proj = {"_id": 1, "user_id": 1, "post_id": 1}
    cursor = coll.find(q, projection=proj, batch_size=5000).sort([("_id", 1)])
    for d in cursor:
        oid = d.get("_id")
        uid = str(d.get("user_id") or "").strip()
        pid = str(d.get("post_id") or "").strip()
        if not uid or not pid:
            continue
        yield {"_id": str(oid) if oid is not None else "", "user_id": uid, "post_id": pid}

def _call_model_with_retry(client: genai.Client, prompt: str) -> Tuple[str, bool]:
    last = None
    for attempt in range(MAX_MODEL_RETRIES):
        try:
            _touch_activity("model", "generate_content")
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[{"role": "user", "parts": [{"text": prompt}]}],
                config=genai_types.GenerateContentConfig(temperature=0.2),
            )
            return _extract_text_from_response_obj(resp), False
        except Exception as e:
            last = e
            if _is_quota_error(e) or _is_transient_error(e):
                wait = _backoff_sleep(attempt)
                print(f"[quota] model retry={attempt+1} sleep={int(wait)}s", file=sys.stderr, flush=True)
                time.sleep(wait)
                continue
            raise
    if last:
        return "", True
    return "", True

def _process_one_doc(client: genai.Client, d: Dict[str, Any], require_exact_10: bool) -> Dict[str, Any]:
    uid = str(d.get("user_id") or "")
    pid = str(d.get("post_id") or "")
    oid = str(d.get("_id") or "")
    content = str(d.get("content") or "")
    key = _make_key(uid, pid)
    if not uid or not pid or not _is_meaningful(content):
        return {"ok": False, "quota_fail": False, "skip_retry": True, "key": key, "oid": oid, "row": None, "reason": "empty_input"}
    prompt = _prompt_for_text(content)
    try:
        txt, quota_fail = _call_model_with_retry(client, prompt)
    except Exception as e:
        return {"ok": False, "quota_fail": _is_quota_error(e), "skip_retry": False, "key": key, "oid": oid, "row": None, "reason": "exception"}
    if not txt:
        return {"ok": False, "quota_fail": True, "skip_retry": False, "key": key, "oid": oid, "row": None, "reason": "empty_output"}
    txt = _normalize_result_text(txt)
    tags, is_s, brand_names, assoc_brand, assoc_mention = parse_payload(txt)
    if require_exact_10 and len(tags) != 10:
        return {"ok": False, "quota_fail": False, "skip_retry": False, "key": key, "oid": oid, "row": None, "reason": "not_10_tags"}
    brand_json = json.dumps(brand_names, ensure_ascii=True) if brand_names else None
    assoc_brand_json = json.dumps(assoc_brand, ensure_ascii=True) if assoc_brand else None
    assoc_mention_json = json.dumps(assoc_mention, ensure_ascii=True) if assoc_mention else None
    row = (uid, pid, _tags_to_mysql_text(tags), int(is_s), brand_json, assoc_brand_json, assoc_mention_json)
    return {"ok": True, "quota_fail": False, "skip_retry": False, "key": key, "oid": oid, "row": row, "reason": "ok"}

def _dedup_failed_keys(keys: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in keys:
        x = str(x or "").strip()
        if not x:
            continue
        xl = x.lower()
        if xl in seen:
            continue
        seen.add(xl)
        out.append(x)
    return out

def _dedup_dead_keys(keys: List[str]) -> List[str]:
    return _dedup_failed_keys(keys)

def _inc_failed_attempt(state: Dict[str, Any], key: str) -> int:
    fa = state.get("failed_attempts") or {}
    if not isinstance(fa, dict):
        fa = {}
    k = str(key or "").strip()
    if not k:
        state["failed_attempts"] = fa
        return 0
    cur = fa.get(k)
    try:
        n = int(cur) if cur is not None else 0
    except Exception:
        n = 0
    n += 1
    fa[k] = n
    state["failed_attempts"] = fa
    return n

def _should_dead_key(state: Dict[str, Any], key: str) -> bool:
    fa = state.get("failed_attempts") or {}
    if not isinstance(fa, dict):
        return False
    k = str(key or "").strip()
    if not k:
        return False
    try:
        n = int(fa.get(k) or 0)
    except Exception:
        n = 0
    return n >= MAX_RETRY_ATTEMPTS

def _move_to_dead(state: Dict[str, Any], keys: List[str]) -> None:
    dk = state.get("dead_keys") or []
    if not isinstance(dk, list):
        dk = []
    for k in keys:
        k2 = str(k or "").strip()
        if k2:
            dk.append(k2)
    state["dead_keys"] = _dedup_dead_keys(dk)

def _build_batch_from_failed_keys(state: Dict[str, Any], max_take: int) -> List[str]:
    fk = state.get("failed_keys") or []
    if not isinstance(fk, list) or not fk:
        return []
    take = fk[:max_take]
    state["failed_keys"] = fk[max_take:]
    _save_state(state)
    return [str(x) for x in take if str(x)]

def _mysql_count_bad_pairs(last_uid: Optional[str], last_pid: Optional[str]) -> int:
    where_bad = "(topic_tags IS NULL OR topic_tags='' OR topic_tags='[]' OR (LENGTH(topic_tags) - LENGTH(REPLACE(topic_tags, \"'\", \"\"))) <> 20)"
    where_resume = ""
    params: List[Any] = []
    if last_uid is not None and last_pid is not None:
        where_resume = " AND ((CAST(user_id AS CHAR) > %s) OR (CAST(user_id AS CHAR) = %s AND CAST(post_id AS CHAR) > %s))"
        params.extend([str(last_uid), str(last_uid), str(last_pid)])
    sql = f"SELECT COUNT(1) AS c FROM `{MYSQL_TABLE}` WHERE {where_bad}{where_resume}"
    _touch_activity("mysql", "count_bad_pairs")
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone() or {}
            return int(row.get("c") or 0)
    finally:
        conn.close()

def _mysql_fetch_bad_pairs(last_uid: Optional[str], last_pid: Optional[str], limit: int) -> Tuple[List[Tuple[str, str]], Optional[str], Optional[str]]:
    where_bad = "(topic_tags IS NULL OR topic_tags='' OR topic_tags='[]' OR (LENGTH(topic_tags) - LENGTH(REPLACE(topic_tags, \"'\", \"\"))) <> 20)"
    where_resume = ""
    params: List[Any] = []
    if last_uid is not None and last_pid is not None:
        where_resume = " AND ((CAST(user_id AS CHAR) > %s) OR (CAST(user_id AS CHAR) = %s AND CAST(post_id AS CHAR) > %s))"
        params.extend([str(last_uid), str(last_uid), str(last_pid)])
    sql = f"""
        SELECT CAST(user_id AS CHAR) AS user_id, CAST(post_id AS CHAR) AS post_id
        FROM `{MYSQL_TABLE}`
        WHERE {where_bad}{where_resume}
        ORDER BY CAST(user_id AS CHAR), CAST(post_id AS CHAR)
        LIMIT %s
    """
    params.append(int(limit))
    _touch_activity("mysql", f"fetch_bad_pairs limit={limit}")
    conn = _connect_mysql()
    rows: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    finally:
        conn.close()
    pairs: List[Tuple[str, str]] = []
    nlast_uid = None
    nlast_pid = None
    for r in rows:
        u = str(r.get("user_id") or "").strip()
        p = str(r.get("post_id") or "").strip()
        if not u or not p:
            continue
        pairs.append((u, p))
        nlast_uid, nlast_pid = u, p
    return pairs, nlast_uid, nlast_pid

def _run_repair(client: genai.Client) -> None:
    started = time.time()
    inflight = int(MAX_INFLIGHT)
    rs = _load_repair_state()
    last_uid = rs.get("last_user_id")
    last_pid = rs.get("last_post_id")
    repaired_ok = int(rs.get("repaired_ok") or 0)
    repaired_failed = int(rs.get("repaired_failed") or 0)

    remaining0 = _mysql_count_bad_pairs(last_uid, last_pid)
    elapsed = _fmt_secs(time.time() - started)
    print(f"[repair] start remaining={remaining0} inflight={inflight} ok={repaired_ok} failed={repaired_failed} elapsed={elapsed}", file=sys.stderr, flush=True)

    while True:
        pairs, nlast_uid, nlast_pid = _mysql_fetch_bad_pairs(last_uid, last_pid, REPAIR_MYSQL_BATCH)
        if not pairs:
            break

        work = _build_docs_from_pairs_with_mysql_content(pairs)

        idx = 0
        while idx < len(work):
            chunk_start = time.time()
            chunk_size = min(inflight, len(work) - idx)
            chunk = work[idx:idx + chunk_size]
            idx += chunk_size

            pending_rows: List[Tuple[str, str, str, int, Optional[str], Optional[str], Optional[str]]] = []
            quota_fail_count = 0
            failed_cnt = 0
            reasons: Dict[str, int] = {}

            with ThreadPoolExecutor(max_workers=inflight) as ex:
                futs = [ex.submit(_process_one_doc, client, d, REPAIR_REQUIRE_EXACT_10) for d in chunk]
                for fut in as_completed(futs):
                    r = fut.result()
                    rsn = str(r.get("reason") or "unknown")
                    reasons[rsn] = reasons.get(rsn, 0) + 1
                    if r.get("ok") and r.get("row") is not None:
                        pending_rows.append(r["row"])
                    else:
                        failed_cnt += 1
                        if r.get("quota_fail"):
                            quota_fail_count += 1

            done_rows = _process_pending_rows(pending_rows)
            repaired_ok += int(done_rows)
            repaired_failed += int(failed_cnt)

            if quota_fail_count >= CHUNK_QUOTA_FAIL_DOWN_THRESHOLD and inflight > INFLIGHT_MIN:
                inflight = max(INFLIGHT_MIN, inflight - INFLIGHT_STEP_DOWN)
                elapsed = _fmt_secs(time.time() - started)
                print(f"[quota] 429_detected chunk_quota_fails={quota_fail_count} reducing_inflight={inflight} elapsed={elapsed}", file=sys.stderr, flush=True)
            elif quota_fail_count <= CHUNK_NO_QUOTA_UP_THRESHOLD and inflight < MAX_INFLIGHT:
                inflight = min(MAX_INFLIGHT, inflight + INFLIGHT_STEP_UP)

            chunk_elapsed = time.time() - chunk_start
            chunk_docs = len(chunk)
            chunk_rate = (chunk_docs / chunk_elapsed) if chunk_elapsed > 0 else 0.0
            overall_elapsed = time.time() - started
            overall_rate = ((repaired_ok + repaired_failed) / overall_elapsed) if overall_elapsed > 0 else 0.0
            remaining = _mysql_count_bad_pairs(last_uid, last_pid)
            eta = _fmt_eta(remaining, overall_rate)
            elapsed = _fmt_secs(overall_elapsed)
            print(
                f"[repair] chunk ok={repaired_ok} failed={repaired_failed} inflight={inflight} "
                f"chunk_rate={_fmt_rate_docs_per_min(chunk_rate)} overall_rate={_fmt_rate_docs_per_min(overall_rate)} "
                f"remaining={remaining} eta={eta} reasons={json.dumps(reasons, ensure_ascii=True)} elapsed={elapsed}",
                file=sys.stderr,
                flush=True,
            )

        last_uid, last_pid = nlast_uid, nlast_pid
        rs["last_user_id"] = last_uid
        rs["last_post_id"] = last_pid
        rs["repaired_ok"] = repaired_ok
        rs["repaired_failed"] = repaired_failed
        _save_repair_state(rs)

        remaining = _mysql_count_bad_pairs(last_uid, last_pid)
        overall_elapsed = time.time() - started
        overall_rate = ((repaired_ok + repaired_failed) / overall_elapsed) if overall_elapsed > 0 else 0.0
        eta = _fmt_eta(remaining, overall_rate)
        elapsed = _fmt_secs(overall_elapsed)
        print(
            f"[repair] batch ok={repaired_ok} failed={repaired_failed} inflight={inflight} remaining={remaining} eta={eta} elapsed={elapsed}",
            file=sys.stderr,
            flush=True,
        )

    remaining_end = _mysql_count_bad_pairs(last_uid, last_pid)
    overall_elapsed = time.time() - started
    elapsed = _fmt_secs(overall_elapsed)
    print(f"[repair] done ok={repaired_ok} failed={repaired_failed} remaining={remaining_end} elapsed={elapsed}", file=sys.stderr, flush=True)

def main():
    hb = threading.Thread(target=_heartbeat_loop, daemon=True)
    hb.start()

    mode = (os.getenv("RUN_MODE") or "").strip().lower()
    if len(sys.argv) >= 2 and str(sys.argv[1] or "").strip().lower() in {"repair", "repair_bad"}:
        mode = "repair"
    if mode not in {"repair", "run", ""}:
        mode = "run"

    location_id = int(LOCATION_ID)
    max_docs = int(MAX_DOCS)
    started = time.time()
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    _touch_activity("init", "ensure_client")
    client = _ensure_client()

    if mode == "repair":
        _touch_activity("repair", "start")
        _run_repair(client)
        return

    _touch_activity("run", "load_state")
    state = _load_state()

    if state.get("mongo_total_candidates") is None:
        total = _mongo_count_total_candidates(location_id)
        state = _load_state()
        state["mongo_total_candidates"] = int(total)
        _save_state(state)

    state = _load_state()
    total_candidates = state.get("mongo_total_candidates")
    try:
        total_candidates_i = int(total_candidates) if total_candidates is not None else None
    except Exception:
        total_candidates_i = None

    processed_total = int(state.get("processed_total") or 0)
    scanned = int(state.get("scanned_total") or 0)
    inflight = int(MAX_INFLIGHT)

    _touch_progress(processed_total=processed_total, scanned_total=scanned, failed_keys=len(state.get("failed_keys") or []), inflight=inflight)

    while True:
        remaining = (max_docs - processed_total) if max_docs > 0 else None
        if remaining is not None and remaining <= 0:
            break

        _touch_activity("run", "load_state_for_retry")
        state = _load_state()
        retry_keys = _build_batch_from_failed_keys(
            state,
            min(500, remaining) if remaining is not None else 500
        )

        docs_batch: List[Dict[str, Any]] = []
        docs_kind = "scan"

        if retry_keys:
            _touch_activity("retry", f"keys={len(retry_keys)}")
            pairs = []
            for k in retry_keys:
                uid, pid = _split_key(k)
                if uid and pid:
                    pairs.append((uid, pid))
            if pairs:
                docs_batch = _build_docs_from_pairs_with_mysql_content(pairs)
                docs_kind = "retry"
            if not docs_batch:
                st = _load_state()
                for k in retry_keys:
                    _inc_failed_attempt(st, k)
                dead = [k for k in retry_keys if _should_dead_key(st, k)]
                if dead:
                    _move_to_dead(st, dead)
                    retry_keys = [k for k in retry_keys if k not in set(dead)]
                if retry_keys:
                    st["failed_keys"] = _dedup_failed_keys((st.get("failed_keys") or []) + retry_keys)
                _save_state(st)
                time.sleep(2)
                continue

        if not docs_batch:
            _touch_activity("scan", "prepare_scan_batch")
            state = _load_state()
            resume_after_id = state.get("mongo_resume_after_id")
            buf: List[Dict[str, Any]] = []
            for doc in _mongo_iter_docs(location_id, resume_after_id):
                scanned += 1
                buf.append(doc)
                _touch_activity("scan", f"scanning scanned={scanned}")
                if scanned == 1 or scanned % 5000 == 0:
                    st = _load_state()
                    st["scanned_total"] = scanned
                    _save_state(st)
                    elapsed = _fmt_secs(time.time() - started)
                    print(f"[scan] scanned={scanned} processed={processed_total} elapsed={elapsed}", file=sys.stderr, flush=True)
                if len(buf) >= MONGO_SCAN_BATCH:
                    pairs = [(d["user_id"], d["post_id"]) for d in buf]
                    existing = _fetch_existing_pairs(pairs)
                    todo = [d for d in buf if (d["user_id"], d["post_id"]) not in existing]
                    if todo:
                        docs_batch = _attach_mysql_content(todo[:500])
                        break
                    buf = []
            if not docs_batch and buf:
                pairs = [(d["user_id"], d["post_id"]) for d in buf]
                existing = _fetch_existing_pairs(pairs)
                todo = [d for d in buf if (d["user_id"], d["post_id"]) not in existing]
                if todo:
                    docs_batch = _attach_mysql_content(todo[:500])

        if not docs_batch:
            break

        if remaining is not None and len(docs_batch) > remaining:
            docs_batch = docs_batch[:remaining]

        _touch_activity("mysql", "recheck_existing_before_run")
        pairs = [(d["user_id"], d["post_id"]) for d in docs_batch]
        existing = _fetch_existing_pairs(pairs)
        docs_batch = [d for d in docs_batch if (d["user_id"], d["post_id"]) not in existing]
        if not docs_batch:
            continue

        overall_elapsed = time.time() - started
        scanned_rate = (scanned / overall_elapsed) if overall_elapsed > 0 else 0.0
        remaining_scan = None
        if total_candidates_i is not None:
            remaining_scan = max(0, int(total_candidates_i) - int(scanned))
        eta_scan = _fmt_eta(remaining_scan, scanned_rate)
        elapsed = _fmt_secs(overall_elapsed)
        st0 = _load_state()
        dead_n = len(st0.get("dead_keys") or []) if isinstance(st0.get("dead_keys"), list) else 0
        print(f"[std] start kind={docs_kind} size={len(docs_batch)} inflight={inflight} processed={processed_total} scanned={scanned} total={total_candidates_i if total_candidates_i is not None else 'unknown'} eta={eta_scan} dead_keys={dead_n} elapsed={elapsed}", file=sys.stderr, flush=True)

        idx = 0

        while idx < len(docs_batch):
            chunk_start = time.time()
            chunk_size = min(inflight, len(docs_batch) - idx)
            chunk = docs_batch[idx:idx + chunk_size]
            idx += chunk_size

            _touch_activity("std", f"process_chunk size={len(chunk)} inflight={inflight}")

            pending_rows: List[Tuple[str, str, str, int, Optional[str], Optional[str], Optional[str]]] = []
            new_failed_keys: List[str] = []
            quota_fail_count = 0
            last_oid = ""
            reasons: Dict[str, int] = {}

            with ThreadPoolExecutor(max_workers=inflight) as ex:
                futs = [ex.submit(_process_one_doc, client, d, True) for d in chunk]
                for fut in as_completed(futs):
                    r = fut.result()
                    rsn = str(r.get("reason") or "unknown")
                    reasons[rsn] = reasons.get(rsn, 0) + 1
                    if r.get("oid"):
                        last_oid = r["oid"]
                    if r.get("ok") and r.get("row") is not None:
                        pending_rows.append(r["row"])
                    else:
                        k = str(r.get("key") or "")
                        if k and not bool(r.get("skip_retry")):
                            new_failed_keys.append(k)
                        if r.get("quota_fail"):
                            quota_fail_count += 1

            done_rows = _process_pending_rows(pending_rows)
            processed_total += int(done_rows)

            st = _load_state()
            st["processed_total"] = processed_total
            st["scanned_total"] = scanned
            if last_oid:
                st["mongo_resume_after_id"] = last_oid

            dead_now: List[str] = []
            keep_fail: List[str] = []
            if new_failed_keys:
                for k in new_failed_keys:
                    _inc_failed_attempt(st, k)
                for k in new_failed_keys:
                    if _should_dead_key(st, k):
                        dead_now.append(k)
                    else:
                        keep_fail.append(k)
                if dead_now:
                    _move_to_dead(st, dead_now)
                if keep_fail:
                    st["failed_keys"] = _dedup_failed_keys((st.get("failed_keys") or []) + keep_fail)

            _save_state(st)

            st2 = _load_state()
            fk_n = len((st2.get("failed_keys") or [])) if isinstance(st2.get("failed_keys"), list) else 0
            dead_n = len((st2.get("dead_keys") or [])) if isinstance(st2.get("dead_keys"), list) else 0
            _touch_progress(processed_total=processed_total, scanned_total=scanned, failed_keys=fk_n, inflight=inflight)

            if quota_fail_count >= CHUNK_QUOTA_FAIL_DOWN_THRESHOLD and inflight > INFLIGHT_MIN:
                inflight = max(INFLIGHT_MIN, inflight - INFLIGHT_STEP_DOWN)
                elapsed = _fmt_secs(time.time() - started)
                print(f"[quota] 429_detected chunk_quota_fails={quota_fail_count} reducing_inflight={inflight} elapsed={elapsed}", file=sys.stderr, flush=True)
                _touch_progress(inflight=inflight)
            elif quota_fail_count <= CHUNK_NO_QUOTA_UP_THRESHOLD and inflight < MAX_INFLIGHT:
                inflight = min(MAX_INFLIGHT, inflight + INFLIGHT_STEP_UP)
                elapsed = _fmt_secs(time.time() - started)
                print(f"[std] stable increasing_inflight={inflight} elapsed={elapsed}", file=sys.stderr, flush=True)
                _touch_progress(inflight=inflight)

            chunk_elapsed = time.time() - chunk_start
            chunk_docs = len(chunk)
            chunk_rate = (chunk_docs / chunk_elapsed) if chunk_elapsed > 0 else 0.0
            overall_elapsed = time.time() - started
            scanned_rate = (scanned / overall_elapsed) if overall_elapsed > 0 else 0.0
            remaining_scan = None
            if total_candidates_i is not None:
                remaining_scan = max(0, int(total_candidates_i) - int(scanned))
            eta_scan = _fmt_eta(remaining_scan, scanned_rate)
            elapsed = _fmt_secs(overall_elapsed)

            if new_failed_keys or dead_now:
                print(
                    f"[std] fail_event new_failed={len(new_failed_keys)} kept_failed={len(keep_fail)} dead_now={len(dead_now)} "
                    f"failed_keys={fk_n} dead_keys={dead_n} reasons={json.dumps(reasons, ensure_ascii=True)}",
                    file=sys.stderr,
                    flush=True,
                )

            print(
                f"[std] chunk done processed={processed_total} scanned={scanned} inflight={inflight} "
                f"chunk_rate={_fmt_rate_docs_per_min(chunk_rate)} scanned_rate={_fmt_rate_docs_per_min(scanned_rate)} "
                f"eta={eta_scan} failed_keys={fk_n} dead_keys={dead_n} elapsed={elapsed}",
                file=sys.stderr,
                flush=True,
            )

        st = _load_state()
        overall_elapsed = time.time() - started
        scanned_rate = (scanned / overall_elapsed) if overall_elapsed > 0 else 0.0
        remaining_scan = None
        if total_candidates_i is not None:
            remaining_scan = max(0, int(total_candidates_i) - int(scanned))
        eta_scan = _fmt_eta(remaining_scan, scanned_rate)
        elapsed = _fmt_secs(overall_elapsed)
        fk_n = len((st.get("failed_keys") or [])) if isinstance(st.get("failed_keys"), list) else 0
        dead_n = len((st.get("dead_keys") or [])) if isinstance(st.get("dead_keys"), list) else 0
        _touch_progress(processed_total=processed_total, scanned_total=scanned, failed_keys=fk_n, inflight=inflight)
        print(
            f"[std] batch done processed={processed_total} scanned={scanned} inflight={inflight} "
            f"scanned_rate={_fmt_rate_docs_per_min(scanned_rate)} eta={eta_scan} failed_keys={fk_n} dead_keys={dead_n} elapsed={elapsed}",
            file=sys.stderr,
            flush=True,
        )

    overall_elapsed = time.time() - started
    elapsed = _fmt_secs(overall_elapsed)
    state = _load_state()
    fk = state.get("failed_keys") or []
    fk_n = len(fk) if isinstance(fk, list) else 0
    dk = state.get("dead_keys") or []
    dk_n = len(dk) if isinstance(dk, list) else 0
    print(
        f"[done] processed={int(state.get('processed_total') or 0)} scanned={int(state.get('scanned_total') or 0)} "
        f"total={int(state.get('mongo_total_candidates') or 0) if state.get('mongo_total_candidates') is not None else 'unknown'} "
        f"failed_keys={fk_n} dead_keys={dk_n} elapsed={elapsed}",
        file=sys.stderr,
        flush=True,
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr, flush=True)
        raise SystemExit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        raise SystemExit(1)
