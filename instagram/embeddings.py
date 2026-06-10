import os
import sys
import time
import json
import ast
import random
import re
from typing import List, Tuple, Dict, Any, Iterable, Optional, Set

from dotenv import load_dotenv
import pymysql
from pymysql.cursors import DictCursor
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from google import genai
from google.genai import types as genai_types

load_dotenv()

MONGO_URI_ENV = "MONGO_URI_ATLAS_TEST"
MONGO_DB_ENV = "MONGO_DB_ATLAS_TEST"
MONGO_COLL_ENV = "MONGO_COLL_ATLAS_TEST"

MYSQL_TABLE = "_test_topic_tag_transit"

GEMINI_MODEL = os.getenv("GEMINI_EMBED_MODEL", "gemini-embedding-001")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1536"))

LOCATION_ID = int(os.getenv("LOCATION_ID", "1"))

BATCH_LIMIT = int(os.getenv("EMBED_BATCH_LIMIT", "1000"))
REQ_BATCH = int(os.getenv("EMBED_REQ_BATCH", "100"))
REQ_SLEEP_SEC = float(os.getenv("GEMINI_RATE_SLEEP", "0.2"))
OVERWRITE = os.getenv("EMBED_OVERWRITE", "false").lower() == "true"

MYSQL_READ_BATCH = int(os.getenv("MYSQL_READ_BATCH", "2000"))
MYSQL_READ_TIMEOUT = int(os.getenv("MYSQL_READ_TIMEOUT", "90"))
MYSQL_WRITE_TIMEOUT = int(os.getenv("MYSQL_WRITE_TIMEOUT", "90"))

MONGO_FIND_CHUNK = int(os.getenv("MONGO_FIND_CHUNK", "500"))
MONGO_BATCHSIZE = int(os.getenv("MONGO_CURSOR_BATCH", "500"))

PAUSE_BETWEEN_BATCHES_SEC = float(os.getenv("PAUSE_BETWEEN_BATCHES_SEC", "0.5"))

MAX_TOTAL_TO_PROCESS = int(os.getenv("MAX_TOTAL_TO_PROCESS", "0"))
MAX_DOCS = int(os.getenv("MAX_DOCS", "0"))

MAX_MODEL_RETRIES = int(os.getenv("MAX_MODEL_RETRIES", "8"))
MAX_MONGO_WRITE_RETRIES = int(os.getenv("MAX_MONGO_WRITE_RETRIES", "9"))

CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ckpt_mysql_to_mongo_embeddings")
RECORD_DIR = os.path.join(CHECKPOINT_DIR, "records")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(RECORD_DIR, exist_ok=True)

STATE_PATH = os.path.join(CHECKPOINT_DIR, "state.json")

FINAL_MONGO_RETRY_SLEEP_MIN = float(os.getenv("FINAL_MONGO_RETRY_SLEEP_MIN", "5.0"))
FINAL_MONGO_RETRY_SLEEP_MAX = float(os.getenv("FINAL_MONGO_RETRY_SLEEP_MAX", "60.0"))

KEEP_INSERTED_RECORDS = int(os.getenv("KEEP_INSERTED_RECORDS", str(max(REQ_BATCH * 2, 1))))

def _env_host() -> str:
    return os.getenv("CRAWL_DB_HOST") or os.getenv("crawl_db_hostname") or "localhost"

def _env_port() -> int:
    return int(os.getenv("CRAWL_DB_PORT") or os.getenv("db_port") or "3306")

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

def _ensure_gemini_client() -> genai.Client:
    key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not key:
        raise SystemExit("Missing GEMINI_API_KEY in .env")
    return genai.Client(api_key=key)

def _ensure_mongo_collection() -> Any:
    uri = _mongo_uri()
    dbn = _mongo_db()
    cn = _mongo_coll()
    if not uri or not dbn or not cn:
        raise SystemExit(f"Missing Mongo envs: {MONGO_URI_ENV}, {MONGO_DB_ENV}, {MONGO_COLL_ENV}")
    mongo = MongoClient(uri)
    return mongo, mongo[dbn][cn]

def _write_json_atomic(path: str, obj: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        try:
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            pass
    os.replace(tmp, path)

def _load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {"last_user_id": None, "last_post_id": None, "processed_total": 0, "errors_total": 0, "missing_mongo_total": 0, "mongo_total_docs": None}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return {"last_user_id": None, "last_post_id": None, "processed_total": 0, "errors_total": 0, "missing_mongo_total": 0, "mongo_total_docs": None}
        for k in ["last_user_id", "last_post_id", "processed_total", "errors_total", "missing_mongo_total", "mongo_total_docs"]:
            if k not in obj:
                obj[k] = None if k in {"last_user_id", "last_post_id", "mongo_total_docs"} else 0
        return obj
    except Exception:
        return {"last_user_id": None, "last_post_id": None, "processed_total": 0, "errors_total": 0, "missing_mongo_total": 0, "mongo_total_docs": None}

def _save_state(state: Dict[str, Any]) -> None:
    _write_json_atomic(STATE_PATH, state)

def _is_quota_or_transient(e: Exception) -> bool:
    m = str(e).lower()
    return ("resource_exhausted" in m) or ("429" in m) or ("quota" in m) or ("rate" in m) or ("timeout" in m) or ("deadline" in m) or ("unavailable" in m) or ("temporarily" in m) or ("connection" in m)

def _is_mongo_retryable(e: Exception) -> bool:
    m = str(e).lower()
    retry_terms = [
        "timeout",
        "timed out",
        "connection",
        "connection reset",
        "network",
        "not primary",
        "node is recovering",
        "server selection",
        "temporarily",
        "interrupted",
        "shutdown",
        "primary stepped down",
        "retry",
    ]
    return any(x in m for x in retry_terms)

def _backoff_sleep(attempt: int) -> float:
    return min(120.0, (0.8 * (2 ** attempt)) + random.random() * 0.8)

def _mongo_backoff_sleep(attempt: int) -> float:
    return min(60.0, (0.8 * (2 ** attempt)) + random.random() * 0.8)

def _parse_topic_tags(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, list):
        return [str(x).strip() for x in s if str(x).strip()]
    if not isinstance(s, str):
        s = str(s)
    raw = s.strip()
    if not raw:
        return []
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [str(x).strip() for x in obj if str(x).strip()]
    except Exception:
        pass
    try:
        obj = ast.literal_eval(raw)
        if isinstance(obj, list):
            return [str(x).strip() for x in obj if str(x).strip()]
    except Exception:
        pass
    return []

def _join_tags(tags: List[str]) -> str:
    seen = set()
    out: List[str] = []
    for t in tags or []:
        t2 = str(t).strip()
        if not t2:
            continue
        k = t2.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t2)
    return ", ".join(out)

def _chunks(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]

def _fetch_mysql_rows_after(last_uid: Optional[str], last_pid: Optional[str], limit: int) -> List[Dict[str, Any]]:
    where_ok = "topic_tags IS NOT NULL AND topic_tags <> '' AND topic_tags <> '[]'"
    where_resume = ""
    params: List[Any] = []
    if last_uid is not None and last_pid is not None:
        where_resume = " AND ((CAST(user_id AS CHAR) > %s) OR (CAST(user_id AS CHAR) = %s AND CAST(post_id AS CHAR) > %s))"
        params.extend([str(last_uid), str(last_uid), str(last_pid)])
    sql = f"""
        SELECT CAST(user_id AS CHAR) AS user_id, CAST(post_id AS CHAR) AS post_id, topic_tags
        FROM `{MYSQL_TABLE}`
        WHERE {where_ok}{where_resume}
        ORDER BY CAST(user_id AS CHAR), CAST(post_id AS CHAR)
        LIMIT %s
    """
    params.append(int(limit))
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall() or [])
    finally:
        conn.close()

def _fetch_mysql_rows_for_pairs(pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    out: Dict[Tuple[str, str], str] = {}
    if not pairs:
        return out
    conn = _connect_mysql()
    try:
        with conn.cursor() as cur:
            for chunk in _chunks(pairs, 500):
                ors: List[str] = []
                params: List[Any] = []
                for uid, pid in chunk:
                    ors.append("(CAST(user_id AS CHAR) = %s AND CAST(post_id AS CHAR) = %s)")
                    params.extend([str(uid), str(pid)])
                sql = f"""
                    SELECT CAST(user_id AS CHAR) AS user_id, CAST(post_id AS CHAR) AS post_id, topic_tags
                    FROM `{MYSQL_TABLE}`
                    WHERE topic_tags IS NOT NULL AND topic_tags <> '' AND topic_tags <> '[]'
                      AND ({' OR '.join(ors)})
                """
                cur.execute(sql, tuple(params))
                rows = list(cur.fetchall() or [])
                for r in rows:
                    uid = str(r.get("user_id") or "").strip()
                    pid = str(r.get("post_id") or "").strip()
                    tags = _parse_topic_tags(r.get("topic_tags"))
                    text = _join_tags(tags)
                    if uid and pid and text:
                        out[(uid, pid)] = text
    finally:
        conn.close()
    return out

def _find_mongo_docs(coll: Any, pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], Any]:
    out: Dict[Tuple[str, str], Any] = {}
    if not pairs:
        return out
    for chunk in _chunks(pairs, MONGO_FIND_CHUNK):
        ors = [{"user_id": u, "post_id": p, "locationId": int(LOCATION_ID)} for (u, p) in chunk]
        q = {"$or": ors}
        proj = {"_id": 1, "user_id": 1, "post_id": 1, "embedding": 1}
        for d in coll.find(q, projection=proj, batch_size=MONGO_BATCHSIZE):
            u = str(d.get("user_id") or "")
            p = str(d.get("post_id") or "")
            if u and p:
                out[(u, p)] = d
    return out

def _has_valid_embedding(md: Dict[str, Any]) -> bool:
    emb = md.get("embedding")
    return isinstance(emb, list) and len(emb) == int(EMBED_DIM)

def _embed_batch(gclient: genai.Client, texts: List[str]) -> List[List[float]]:
    last: Optional[Exception] = None
    for attempt in range(MAX_MODEL_RETRIES):
        try:
            resp = gclient.models.embed_content(
                model=GEMINI_MODEL,
                contents=texts,
                config=genai_types.EmbedContentConfig(task_type="SEMANTIC_SIMILARITY", output_dimensionality=int(EMBED_DIM)),
            )
            embs = getattr(resp, "embeddings", None)
            if not isinstance(embs, list):
                raise RuntimeError("Gemini embed_content returned no embeddings")
            out: List[List[float]] = []
            for e in embs:
                vals = getattr(e, "values", None)
                if not isinstance(vals, list):
                    raise RuntimeError("Gemini embedding missing values")
                v = [float(x) for x in vals]
                if len(v) != int(EMBED_DIM):
                    raise RuntimeError(f"Embedding dim mismatch: got {len(v)} expected {int(EMBED_DIM)}")
                out.append(v)
            if len(out) != len(texts):
                raise RuntimeError(f"Embedding count mismatch: got {len(out)} expected {len(texts)}")
            return out
        except Exception as e:
            last = e
            if _is_quota_or_transient(e) and attempt < (MAX_MODEL_RETRIES - 1):
                time.sleep(_backoff_sleep(attempt))
                continue
            raise
    raise last if last else RuntimeError("Unknown embed error")

def _fmt_eta(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    if seconds <= 0:
        return "0s"
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"

def _safe_name_part(s: str) -> str:
    s = str(s or "").strip()
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "x"

def _record_path(uid: str, pid: str) -> str:
    return os.path.join(RECORD_DIR, f"{_safe_name_part(uid)}__{_safe_name_part(pid)}.json")

def _empty_record(uid: str, pid: str, mongo_id: str = "") -> Dict[str, Any]:
    return {
        "user_id": str(uid or ""),
        "post_id": str(pid or ""),
        "mongo_id": str(mongo_id or ""),
        "status": "pending",
        "embed_retry_count": 0,
        "mongo_write_retry_count": 0,
        "last_error": None,
        "text": None,
        "embedding": None,
        "updated_at": int(time.time()),
    }

def _load_record(uid: str, pid: str) -> Dict[str, Any]:
    path = _record_path(uid, pid)
    if not os.path.exists(path):
        return _empty_record(uid, pid)
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return _empty_record(uid, pid)
        out = _empty_record(uid, pid)
        out.update(obj)
        out["user_id"] = str(out.get("user_id") or uid or "")
        out["post_id"] = str(out.get("post_id") or pid or "")
        out["mongo_id"] = str(out.get("mongo_id") or "")
        return out
    except Exception:
        return _empty_record(uid, pid)

def _save_record(rec: Dict[str, Any]) -> None:
    obj = dict(rec)
    obj["mongo_id"] = str(obj.get("mongo_id") or "")
    obj["updated_at"] = int(time.time())
    _write_json_atomic(_record_path(str(obj.get("user_id") or ""), str(obj.get("post_id") or "")), obj)

def _delete_record(uid: str, pid: str) -> None:
    path = _record_path(uid, pid)
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass

def _prune_inserted_records(keep: int) -> None:
    if keep < 0:
        return
    inserted: List[Tuple[int, str, str, str]] = []
    for name in os.listdir(RECORD_DIR):
        if not name.endswith(".json"):
            continue
        path = os.path.join(RECORD_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if str(obj.get("status") or "") != "inserted":
            continue
        ts = int(obj.get("updated_at") or 0)
        uid = str(obj.get("user_id") or "")
        pid = str(obj.get("post_id") or "")
        inserted.append((ts, path, uid, pid))
    if len(inserted) <= keep:
        return
    inserted.sort(key=lambda x: (x[0], x[1]), reverse=True)
    for _, path, _, _ in inserted[keep:]:
        try:
            os.remove(path)
        except Exception:
            pass

def _record_status(uid: str, pid: str) -> str:
    return str(_load_record(uid, pid).get("status") or "pending")

def _should_skip_pair(uid: str, pid: str) -> bool:
    return _record_status(uid, pid) == "inserted"

def _iter_retry_record_pairs(limit: int, statuses: Optional[Set[str]] = None) -> List[Tuple[str, str]]:
    if statuses is None:
        statuses = {"failed_embed", "embedded", "failed_mongo_write"}
    out: List[Tuple[str, str]] = []
    for name in sorted(os.listdir(RECORD_DIR)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(RECORD_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        status = str(obj.get("status") or "")
        if status not in statuses:
            continue
        uid = str(obj.get("user_id") or "").strip()
        pid = str(obj.get("post_id") or "").strip()
        if not uid or not pid:
            continue
        out.append((uid, pid))
        if len(out) >= limit:
            break
    return out

def _count_retry_record_pairs(statuses: Optional[Set[str]] = None) -> int:
    if statuses is None:
        statuses = {"failed_embed", "embedded", "failed_mongo_write"}
    n = 0
    for name in os.listdir(RECORD_DIR):
        if not name.endswith(".json"):
            continue
        path = os.path.join(RECORD_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if str(obj.get("status") or "") in statuses:
            n += 1
    return n

def _mark_inserted_and_cleanup(rec: Dict[str, Any]) -> None:
    uid = str(rec.get("user_id") or "")
    pid = str(rec.get("post_id") or "")
    rec["status"] = "inserted"
    rec["last_error"] = None
    rec["updated_at"] = int(time.time())
    _save_record(rec)
    _prune_inserted_records(KEEP_INSERTED_RECORDS)

def _mongo_write_record_with_retry(coll: Any, rec: Dict[str, Any], max_attempts: int) -> Dict[str, Any]:
    uid = str(rec.get("user_id") or "")
    pid = str(rec.get("post_id") or "")
    mongo_id = rec.get("mongo_id")
    embedding = rec.get("embedding")
    if not uid or not pid or not mongo_id or not isinstance(embedding, list):
        rec["status"] = "failed_mongo_write"
        rec["last_error"] = "missing_embedding_or_mongo_id"
        _save_record(rec)
        return {"ok": False, "reason": "missing_embedding_or_mongo_id"}
    try:
        mongo_obj_id = ObjectId(str(mongo_id))
    except Exception:
        rec["status"] = "failed_mongo_write"
        rec["last_error"] = f"invalid_mongo_id: {mongo_id}"
        _save_record(rec)
        return {"ok": False, "reason": "invalid_mongo_id"}
    start_attempt = int(rec.get("mongo_write_retry_count") or 0)
    last_err = None
    for attempt in range(start_attempt, start_attempt + max_attempts):
        try:
            result = coll.bulk_write(
                [UpdateOne({"_id": mongo_obj_id}, {"$set": {"embedding": embedding}})],
                ordered=False,
            )
            if int(getattr(result, "matched_count", 0) or 0) < 1:
                rec["mongo_write_retry_count"] = attempt + 1
                rec["status"] = "failed_mongo_write"
                rec["last_error"] = f"no_document_matched _id={mongo_id}"
                _save_record(rec)
                print(
                    f"[mongo] write_no_match user_id={uid} post_id={pid} attempt={attempt+1} matched={getattr(result, 'matched_count', 0)} modified={getattr(result, 'modified_count', 0)}",
                    file=sys.stderr,
                    flush=True,
                )
                return {"ok": False, "reason": "no_document_matched"}
            rec["mongo_write_retry_count"] = attempt + 1
            _mark_inserted_and_cleanup(rec)
            print(
                f"[mongo] write_success user_id={uid} post_id={pid} attempt={attempt+1} matched={getattr(result, 'matched_count', 0)} modified={getattr(result, 'modified_count', 0)}",
                file=sys.stderr,
                flush=True,
            )
            return {"ok": True, "reason": "inserted"}
        except Exception as e:
            last_err = str(e)
            rec["mongo_write_retry_count"] = attempt + 1
            rec["status"] = "failed_mongo_write"
            rec["last_error"] = last_err
            _save_record(rec)
            print(f"[mongo] write_failed user_id={uid} post_id={pid} attempt={attempt+1} err={last_err}", file=sys.stderr, flush=True)
            if attempt + 1 >= start_attempt + max_attempts or not _is_mongo_retryable(e):
                break
            wait = _mongo_backoff_sleep(attempt - start_attempt)
            print(f"[mongo] write_retry user_id={uid} post_id={pid} attempt={attempt+1} sleep={wait:.1f}s", file=sys.stderr, flush=True)
            time.sleep(wait)
    rec["status"] = "failed_mongo_write"
    rec["last_error"] = last_err or "mongo_write_failed"
    _save_record(rec)
    return {"ok": False, "reason": "failed_mongo_write"}

def _process_embed_batch(gclient: genai.Client, coll: Any, items: List[Tuple[str, str, str, Any]]) -> Tuple[int, int]:
    if not items:
        return 0, 0

    ready_for_write: List[Dict[str, Any]] = []
    need_embed: List[Tuple[str, str, str, Any, Dict[str, Any]]] = []

    for uid, pid, text, mongo_id in items:
        rec = _load_record(uid, pid)
        rec["user_id"] = uid
        rec["post_id"] = pid
        rec["mongo_id"] = str(mongo_id)
        rec["text"] = text
        if rec.get("status") == "inserted":
            continue
        if rec.get("status") in {"embedded", "failed_mongo_write"} and isinstance(rec.get("embedding"), list):
            ready_for_write.append(rec)
            continue
        need_embed.append((uid, pid, text, mongo_id, rec))

    updated = 0
    errors = 0

    if need_embed:
        texts = [x[2] for x in need_embed]
        preview_keys = ", ".join([f"{uid}:{pid}" for uid, pid, _, _, _ in need_embed[:5]])
        print(f"[embed] start count={len(need_embed)} items={preview_keys}", file=sys.stderr, flush=True)
        try:
            vecs = _embed_batch(gclient, texts)
            for (uid, pid, text, mongo_id, rec), vec in zip(need_embed, vecs):
                rec["embed_retry_count"] = int(rec.get("embed_retry_count") or 0) + 1
                rec["status"] = "embedded"
                rec["last_error"] = None
                rec["text"] = text
                rec["mongo_id"] = str(mongo_id)
                rec["embedding"] = [float(x) for x in vec]
                _save_record(rec)
                ready_for_write.append(rec)
                print(f"[embed] generated user_id={uid} post_id={pid} dim={len(rec['embedding'])}", file=sys.stderr, flush=True)
        except Exception as e:
            msg = str(e)
            for uid, pid, text, mongo_id, rec in need_embed:
                rec["embed_retry_count"] = int(rec.get("embed_retry_count") or 0) + 1
                rec["status"] = "failed_embed"
                rec["last_error"] = msg
                rec["text"] = text
                rec["mongo_id"] = str(mongo_id)
                _save_record(rec)
                print(f"[embed] failed user_id={uid} post_id={pid} err={msg}", file=sys.stderr, flush=True)
            errors += len(need_embed)
            print(f"[embed_error] n={len(need_embed)} err={msg}", file=sys.stderr, flush=True)

    for rec in ready_for_write:
        r = _mongo_write_record_with_retry(coll, rec, MAX_MONGO_WRITE_RETRIES)
        if r.get("ok"):
            updated += 1
        else:
            errors += 1

    return updated, errors

def _drain_failed_mongo_writes(coll: Any) -> None:
    round_no = 0
    while True:
        remaining = _count_retry_record_pairs({"embedded", "failed_mongo_write"})
        if remaining <= 0:
            break
        round_no += 1
        pairs = _iter_retry_record_pairs(1000, {"embedded", "failed_mongo_write"})
        if not pairs:
            sleep_s = min(FINAL_MONGO_RETRY_SLEEP_MAX, FINAL_MONGO_RETRY_SLEEP_MIN + random.random() * 3.0)
            print(f"[drain] no_work remaining={remaining} sleep={sleep_s:.1f}s round={round_no}", file=sys.stderr, flush=True)
            time.sleep(sleep_s)
            continue
        ok_cnt = 0
        fail_cnt = 0
        for uid, pid in pairs:
            rec = _load_record(uid, pid)
            if rec.get("status") == "inserted":
                ok_cnt += 1
                continue
            if rec.get("status") not in {"embedded", "failed_mongo_write"} or not isinstance(rec.get("embedding"), list):
                fail_cnt += 1
                continue
            r = _mongo_write_record_with_retry(coll, rec, 1)
            if r.get("ok"):
                ok_cnt += 1
            else:
                fail_cnt += 1
        remaining2 = _count_retry_record_pairs({"embedded", "failed_mongo_write"})
        print(f"[drain] round={round_no} ok={ok_cnt} failed={fail_cnt} remaining={remaining2}", file=sys.stderr, flush=True)
        if remaining2 <= 0:
            break
        sleep_s = min(FINAL_MONGO_RETRY_SLEEP_MAX, FINAL_MONGO_RETRY_SLEEP_MIN + min(round_no, 10) * 2.0 + random.random() * 3.0)
        print(f"[drain] sleeping={sleep_s:.1f}s remaining={remaining2}", file=sys.stderr, flush=True)
        time.sleep(sleep_s)

def run() -> None:
    state = _load_state()
    processed_total = int(state.get("processed_total") or 0)
    errors_total = int(state.get("errors_total") or 0)
    missing_mongo_total = int(state.get("missing_mongo_total") or 0)
    last_uid = state.get("last_user_id")
    last_pid = state.get("last_post_id")
    mongo_total_docs = state.get("mongo_total_docs")

    gclient = _ensure_gemini_client()
    mongo_client, coll = _ensure_mongo_collection()

    started = time.time()
    processed_start = processed_total

    try:
        if mongo_total_docs is None:
            mongo_total_docs = int(coll.count_documents({"locationId": int(LOCATION_ID)}))
            state = _load_state()
            state["mongo_total_docs"] = int(mongo_total_docs)
            _save_state(state)
        else:
            try:
                mongo_total_docs = int(mongo_total_docs)
            except Exception:
                mongo_total_docs = int(coll.count_documents({"locationId": int(LOCATION_ID)}))
                state = _load_state()
                state["mongo_total_docs"] = int(mongo_total_docs)
                _save_state(state)

        prefer_retry = True

        while True:
            if MAX_DOCS and processed_total >= MAX_DOCS:
                break
            if MAX_TOTAL_TO_PROCESS and processed_total >= MAX_TOTAL_TO_PROCESS:
                break

            cap = None
            if MAX_DOCS:
                cap = MAX_DOCS - processed_total
            if MAX_TOTAL_TO_PROCESS:
                cap2 = MAX_TOTAL_TO_PROCESS - processed_total
                cap = cap2 if cap is None else min(cap, cap2)

            retry_total = _count_retry_record_pairs()
            work_items: List[Tuple[str, str, str, Any]] = []

            if prefer_retry and retry_total > 0:
                retry_pairs = _iter_retry_record_pairs(min(BATCH_LIMIT, cap) if cap is not None else BATCH_LIMIT)
                if retry_pairs:
                    mongo_docs = _find_mongo_docs(coll, retry_pairs)
                    mysql_rows_map = _fetch_mysql_rows_for_pairs(retry_pairs)
                    for uid, pid in retry_pairs:
                        md = mongo_docs.get((uid, pid))
                        if md is None:
                            missing_mongo_total += 1
                            print(f"[skip] user_id={uid} post_id={pid} reason=mongo_doc_not_found", file=sys.stderr, flush=True)
                            continue
                        rec = _load_record(uid, pid)
                        text = str(rec.get("text") or "").strip() or str(mysql_rows_map.get((uid, pid)) or "").strip()
                        if not text and rec.get("status") not in {"embedded", "failed_mongo_write"}:
                            print(f"[skip] user_id={uid} post_id={pid} reason=no_text", file=sys.stderr, flush=True)
                            continue
                        if (not OVERWRITE) and _has_valid_embedding(md):
                            rec["user_id"] = uid
                            rec["post_id"] = pid
                            rec["mongo_id"] = str(md["_id"])
                            if text:
                                rec["text"] = text
                            _mark_inserted_and_cleanup(rec)
                            print(f"[skip] user_id={uid} post_id={pid} reason=embedding_exists_in_mongo", file=sys.stderr, flush=True)
                            continue
                        print(f"[queue] user_id={uid} post_id={pid} reason=needs_embedding", file=sys.stderr, flush=True)
                        work_items.append((uid, pid, text, md["_id"]))

            prefer_retry = not prefer_retry

            if not work_items:
                take = BATCH_LIMIT
                if cap is not None:
                    take = min(take, max(0, int(cap)))
                if take <= 0:
                    break

                rows = _fetch_mysql_rows_after(last_uid, last_pid, min(MYSQL_READ_BATCH, take))
                if not rows:
                    break

                work: List[Tuple[str, str, str]] = []
                pairs: List[Tuple[str, str]] = []

                for r in rows:
                    uid = str(r.get("user_id") or "").strip()
                    pid = str(r.get("post_id") or "").strip()
                    if _should_skip_pair(uid, pid):
                        print(f"[skip] user_id={uid} post_id={pid} reason=checkpoint_inserted", file=sys.stderr, flush=True)
                        continue
                    tags = _parse_topic_tags(r.get("topic_tags"))
                    text = _join_tags(tags)
                    if not uid or not pid:
                        continue
                    if not text:
                        print(f"[skip] user_id={uid} post_id={pid} reason=no_text", file=sys.stderr, flush=True)
                        continue
                    work.append((uid, pid, text))
                    pairs.append((uid, pid))

                if not work:
                    last_uid = str(rows[-1].get("user_id") or "")
                    last_pid = str(rows[-1].get("post_id") or "")
                    state = _load_state()
                    state["last_user_id"] = last_uid
                    state["last_post_id"] = last_pid
                    state["processed_total"] = processed_total
                    state["errors_total"] = errors_total
                    state["missing_mongo_total"] = missing_mongo_total
                    _save_state(state)
                    time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
                    continue

                mongo_docs = _find_mongo_docs(coll, pairs)

                for uid, pid, text in work:
                    md = mongo_docs.get((uid, pid))
                    if md is None:
                        missing_mongo_total += 1
                        print(f"[skip] user_id={uid} post_id={pid} reason=mongo_doc_not_found", file=sys.stderr, flush=True)
                        continue
                    if (not OVERWRITE) and _has_valid_embedding(md):
                        rec = _load_record(uid, pid)
                        rec["user_id"] = uid
                        rec["post_id"] = pid
                        rec["mongo_id"] = str(md["_id"])
                        rec["text"] = text
                        _mark_inserted_and_cleanup(rec)
                        print(f"[skip] user_id={uid} post_id={pid} reason=embedding_exists_in_mongo", file=sys.stderr, flush=True)
                        continue
                    print(f"[queue] user_id={uid} post_id={pid} reason=needs_embedding", file=sys.stderr, flush=True)
                    work_items.append((uid, pid, text, md["_id"]))

                last_uid = str(rows[-1].get("user_id") or "")
                last_pid = str(rows[-1].get("post_id") or "")

                state = _load_state()
                state["last_user_id"] = last_uid
                state["last_post_id"] = last_pid
                state["processed_total"] = processed_total
                state["errors_total"] = errors_total
                state["missing_mongo_total"] = missing_mongo_total
                state["mongo_total_docs"] = int(mongo_total_docs)
                _save_state(state)

            if not work_items:
                retry_total2 = _count_retry_record_pairs()
                if retry_total2 > 0:
                    time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
                    continue
                if MAX_DOCS and processed_total >= MAX_DOCS:
                    break
                if MAX_TOTAL_TO_PROCESS and processed_total >= MAX_TOTAL_TO_PROCESS:
                    break
                if last_uid is None and last_pid is None:
                    break
                time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
                rows = _fetch_mysql_rows_after(last_uid, last_pid, 1)
                if not rows:
                    break
                continue

            if MAX_DOCS:
                remaining_docs = MAX_DOCS - processed_total
                if remaining_docs <= 0:
                    break
                if len(work_items) > remaining_docs:
                    work_items = work_items[:remaining_docs]

            if MAX_TOTAL_TO_PROCESS:
                remaining_docs2 = MAX_TOTAL_TO_PROCESS - processed_total
                if remaining_docs2 <= 0:
                    break
                if len(work_items) > remaining_docs2:
                    work_items = work_items[:remaining_docs2]

            updated_this_batch = 0
            errors_this_batch = 0

            for batch in _chunks(work_items, REQ_BATCH):
                updated_n, errors_n = _process_embed_batch(gclient, coll, batch)
                updated_this_batch += updated_n
                errors_this_batch += errors_n
                errors_total += errors_n
                time.sleep(REQ_SLEEP_SEC)

            processed_total += updated_this_batch

            state = _load_state()
            state["last_user_id"] = last_uid
            state["last_post_id"] = last_pid
            state["processed_total"] = processed_total
            state["errors_total"] = errors_total
            state["missing_mongo_total"] = missing_mongo_total
            state["mongo_total_docs"] = int(mongo_total_docs)
            _save_state(state)

            elapsed = time.time() - started
            done_this_run = processed_total - processed_start
            rate = (done_this_run / elapsed) if elapsed > 0 else 0.0

            target_total = int(mongo_total_docs)
            if MAX_DOCS:
                target_total = min(target_total, int(MAX_DOCS))
            if MAX_TOTAL_TO_PROCESS:
                target_total = min(target_total, int(MAX_TOTAL_TO_PROCESS))

            remaining_est = max(0, target_total - processed_total)
            eta = None if rate <= 0 else (remaining_est / rate)
            retry_total3 = _count_retry_record_pairs()

            print(
                f"[progress] updated={updated_this_batch} errors={errors_this_batch} "
                f"processed_total={processed_total}/{target_total} rate_docs_s={rate:.3f} eta={_fmt_eta(eta)} "
                f"errors_total={errors_total} missing_mongo_total={missing_mongo_total} retry_candidates={retry_total3} last={last_uid}:{last_pid}",
                file=sys.stderr,
                flush=True,
            )

            time.sleep(PAUSE_BETWEEN_BATCHES_SEC)

        _drain_failed_mongo_writes(coll)

        elapsed = time.time() - started
        done_this_run = processed_total - processed_start
        rate = (done_this_run / elapsed) if elapsed > 0 else 0.0
        target_total = int(mongo_total_docs)
        if MAX_DOCS:
            target_total = min(target_total, int(MAX_DOCS))
        if MAX_TOTAL_TO_PROCESS:
            target_total = min(target_total, int(MAX_TOTAL_TO_PROCESS))
        remaining_est = max(0, target_total - processed_total)
        eta = None if rate <= 0 else (remaining_est / rate)
        retry_total4 = _count_retry_record_pairs()

        print(
            f"[done] processed_total={processed_total}/{target_total} rate_docs_s={rate:.3f} eta={_fmt_eta(eta)} errors_total={errors_total} missing_mongo_total={missing_mongo_total} retry_candidates={retry_total4}",
            file=sys.stderr,
            flush=True,
        )

    finally:
        try:
            mongo_client.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as e:
        print(f"Fatal: {str(e)}", file=sys.stderr, flush=True)
        raise SystemExit(1)
