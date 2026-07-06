from flask import Flask, request, jsonify, render_template_string
import time
import requests
import os
import json
import re
import base64
import hmac
import hashlib
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# PrefixMiddleware to support mounting under /fb reverse proxy path
class PrefixMiddleware(object):
    def __init__(self, wsgi_app, prefix=''):
        self.wsgi_app = wsgi_app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        path_info = environ.get('PATH_INFO', '')
        if path_info.startswith(self.prefix):
            environ['PATH_INFO'] = path_info[len(self.prefix):]
            environ['SCRIPT_NAME'] = self.prefix
        return self.wsgi_app(environ, start_response)

app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix='/fb')


# Instagram & FB credentials mapping
# Instagram & FB credentials mapping
IG_ACCESS_TOKEN        = os.getenv("IG_ACCESS_TOKEN") or os.getenv("PAGE_ACCESS_TOKEN")
IG_BUSINESS_ACCOUNT_ID = os.getenv("IG_BUSINESS_ACCOUNT_ID") or os.getenv("IG_USER_ID")
IG_APP_ID              = os.getenv("IG_APP_ID") or os.getenv("APP_ID")
IG_APP_SECRET          = os.getenv("IG_APP_SECRET") or os.getenv("APP_SECRET")
WEBHOOK_VERIFY_TOKEN   = os.getenv("WEBHOOK_VERIFY_TOKEN") or os.getenv("VERIFY_TOKEN")
PAGE_ID                = os.getenv("PAGE_ID")
GRAPH_API_VERSION      = os.getenv("GRAPH_API_VERSION", "v19.0")

# Strict startup validation check (fails loudly if any required var is missing)
missing_vars = []
if not IG_ACCESS_TOKEN: missing_vars.append("IG_ACCESS_TOKEN / PAGE_ACCESS_TOKEN")
if not WEBHOOK_VERIFY_TOKEN: missing_vars.append("WEBHOOK_VERIFY_TOKEN / VERIFY_TOKEN")
if not PAGE_ID: missing_vars.append("PAGE_ID")

if missing_vars:
    raise RuntimeError(f"Startup failed: Missing required environment variables: {', '.join(missing_vars)}")

if not IG_BUSINESS_ACCOUNT_ID:
    print("[Warning] IG_BUSINESS_ACCOUNT_ID / IG_USER_ID is not configured in .env. Will attempt auto-discovery.")

# Map back to existing script variable names
PAGE_ACCESS_TOKEN      = IG_ACCESS_TOKEN
IG_USER_ID             = IG_BUSINESS_ACCOUNT_ID
VERIFY_TOKEN           = WEBHOOK_VERIFY_TOKEN
GRAPH_URL              = f"https://graph.facebook.com/{GRAPH_API_VERSION}"


BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_FILE     = os.path.join(BASE_DIR, "keywords.json")
AUTOMATIONS_FILE  = os.path.join(BASE_DIR, "automations.json")
REPLIED_FILE      = os.path.join(BASE_DIR, "replied.json")
MAX_REPLIED_STORE = 5000   # cap stored IDs to avoid unbounded growth

import datetime

# Configuration, state, and orders paths
persistent_dir = os.getenv("PERSISTENT_DIR")
if not persistent_dir and os.path.exists("/data"):
    persistent_dir = "/data"

def get_flow_path(filename):
    if persistent_dir:
        return os.path.join(persistent_dir, filename)
    # Match the fallback behavior in Node.js app.js
    if os.path.exists(os.path.join(BASE_DIR, "app.py")):
        return os.path.join(BASE_DIR, filename)
    return os.path.join(BASE_DIR, "..", filename)

CONV_STATE_PATH = get_flow_path("conversation_state.json")
ORDER_FLOW_CONFIG_PATH = get_flow_path("order_flow_config.json")
ORDERS_PATH = get_flow_path("orders.json")

# SQLite Core setup
import sqlite3
import threading

db_lock = threading.Lock()
DB_FILE = get_flow_path("ig_automation.db")

def get_db_conn():
    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn

def db_execute(query, params=(), commit=False):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if commit:
                conn.commit()
                return cursor.lastrowid
            return cursor.fetchall()
        finally:
            conn.close()

def db_execute_script(script):
    with db_lock:
        conn = get_db_conn()
        try:
            conn.executescript(script)
            conn.commit()
        finally:
            conn.close()

def init_sqlite_db():
    schema = """
    CREATE TABLE IF NOT EXISTS ig_automations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        reply TEXT,
        action TEXT,
        dm_message TEXT,
        trigger_type TEXT,
        scope TEXT,
        post_ids TEXT,
        thumbnail TEXT,
        keyword_type TEXT,
        keywords TEXT,
        active INTEGER,
        delay_seconds INTEGER,
        link_url TEXT,
        follow_up_message TEXT,
        ask_follow INTEGER,
        follow_prompt TEXT,
        email_capture INTEGER,
        email_prompt TEXT
    );
    CREATE TABLE IF NOT EXISTS ig_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT,
        user_id TEXT,
        username TEXT,
        automation_name TEXT,
        captured_at REAL
    );
    CREATE TABLE IF NOT EXISTS ig_replied (
        comment_id TEXT PRIMARY KEY,
        timestamp REAL
    );
    CREATE TABLE IF NOT EXISTS ig_messages_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recipient_id TEXT,
        text TEXT,
        status TEXT,
        sent_at REAL,
        is_automated INTEGER,
        is_private_reply INTEGER,
        automation_name TEXT
    );
    CREATE TABLE IF NOT EXISTS ig_messages_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT,
        scheduled_at REAL,
        processed INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ig_scheduled_posts (
        id TEXT PRIMARY KEY,
        media_url TEXT,
        caption TEXT,
        scheduled_time REAL,
        status TEXT,
        error TEXT
    );
    CREATE TABLE IF NOT EXISTS ig_user_interactions (
        user_id TEXT PRIMARY KEY,
        last_interaction REAL
    );
    CREATE TABLE IF NOT EXISTS ig_welcomed (
        user_id TEXT PRIMARY KEY,
        welcomed_at REAL
    );
    CREATE TABLE IF NOT EXISTS ig_stats (
        key TEXT PRIMARY KEY,
        value INTEGER
    );
    CREATE TABLE IF NOT EXISTS ig_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    CREATE TABLE IF NOT EXISTS ig_flows (
        flow_key TEXT PRIMARY KEY,
        steps TEXT
    );
    CREATE TABLE IF NOT EXISTS ig_conv_state (
        user_id TEXT PRIMARY KEY,
        step TEXT,
        answers TEXT,
        updatedAt REAL
    );
    CREATE TABLE IF NOT EXISTS ig_token_health (
        key TEXT PRIMARY KEY,
        status TEXT,
        expires_at REAL,
        last_check REAL,
        scopes TEXT,
        error TEXT
    );
    """
    db_execute_script(schema)
    _migrate_legacy_json_files()

def _migrate_legacy_json_files():
    # 1. ig_automations.json
    if os.path.exists(IG_AUTOMATIONS_FILE):
        try:
            with open(IG_AUTOMATIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for rule in data:
                    active = 1 if rule.get("active") is not False else 0
                    db_execute(
                        "INSERT INTO ig_automations (name, reply, action, dm_message, trigger_type, scope, post_ids, thumbnail, keyword_type, keywords, active, delay_seconds, link_url, follow_up_message, ask_follow, follow_prompt, email_capture, email_prompt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            rule.get("name"),
                            rule.get("reply"),
                            rule.get("action"),
                            rule.get("dm_message"),
                            rule.get("trigger_type"),
                            rule.get("scope"),
                            json.dumps(rule.get("post_ids") or []),
                            rule.get("thumbnail"),
                            rule.get("keyword_type"),
                            json.dumps(rule.get("keywords") or []),
                            active,
                            rule.get("delay_seconds") or 0,
                            rule.get("link_url"),
                            rule.get("follow_up_message"),
                            1 if rule.get("ask_follow") else 0,
                            rule.get("follow_prompt"),
                            1 if rule.get("email_capture") else 0,
                            rule.get("email_prompt")
                        ),
                        commit=True
                    )
            os.rename(IG_AUTOMATIONS_FILE, IG_AUTOMATIONS_FILE + ".bak")
            print("[Migration] Migrated ig_automations.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_automations: {e}")

    # 2. ig_leads.json
    if os.path.exists(IG_LEADS_FILE):
        try:
            with open(IG_LEADS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for lead in data:
                    db_execute(
                        "INSERT INTO ig_leads (email, user_id, username, automation_name, captured_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            lead.get("email"),
                            lead.get("user_id"),
                            lead.get("username"),
                            lead.get("automation_name"),
                            lead.get("captured_at") or time.time()
                        ),
                        commit=True
                    )
            os.rename(IG_LEADS_FILE, IG_LEADS_FILE + ".bak")
            print("[Migration] Migrated ig_leads.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_leads: {e}")

    # 3. ig_stats.json
    if os.path.exists(IG_STATS_FILE):
        try:
            with open(IG_STATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for k, v in data.items():
                    if k == "dms_today_date":
                        db_execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES (?, ?)", (k, str(v)), commit=True)
                    else:
                        db_execute("INSERT OR REPLACE INTO ig_stats (key, value) VALUES (?, ?)", (k, int(v or 0)), commit=True)
            os.rename(IG_STATS_FILE, IG_STATS_FILE + ".bak")
            print("[Migration] Migrated ig_stats.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_stats: {e}")

    # 4. ig_settings.json
    if os.path.exists(IG_SETTINGS_FILE):
        try:
            with open(IG_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                db_execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES ('general_settings', ?)", (json.dumps(data),), commit=True)
            os.rename(IG_SETTINGS_FILE, IG_SETTINGS_FILE + ".bak")
            print("[Migration] Migrated ig_settings.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_settings: {e}")

    # 5. ig_scheduled_posts.json
    if os.path.exists(IG_SCHEDULED_POSTS_FILE):
        try:
            with open(IG_SCHEDULED_POSTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for post in data:
                    db_execute(
                        "INSERT OR REPLACE INTO ig_scheduled_posts (id, media_url, caption, scheduled_time, status, error) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            post.get("id"),
                            post.get("media_url"),
                            post.get("caption"),
                            post.get("scheduled_time"),
                            post.get("status"),
                            post.get("error")
                        ),
                        commit=True
                    )
            os.rename(IG_SCHEDULED_POSTS_FILE, IG_SCHEDULED_POSTS_FILE + ".bak")
            print("[Migration] Migrated ig_scheduled_posts.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_scheduled_posts: {e}")

    # 6. ig_flows.json
    if os.path.exists(IG_FLOWS_FILE):
        try:
            with open(IG_FLOWS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for k, v in data.items():
                    db_execute("INSERT OR REPLACE INTO ig_flows (flow_key, steps) VALUES (?, ?)", (k, json.dumps(v)), commit=True)
            os.rename(IG_FLOWS_FILE, IG_FLOWS_FILE + ".bak")
            print("[Migration] Migrated ig_flows.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_flows: {e}")

    # 7. ig_replied.json
    if os.path.exists(IG_REPLIED_FILE):
        try:
            with open(IG_REPLIED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for cid in data:
                    db_execute("INSERT OR REPLACE INTO ig_replied (comment_id, timestamp) VALUES (?, ?)", (cid, time.time()), commit=True)
            os.rename(IG_REPLIED_FILE, IG_REPLIED_FILE + ".bak")
            print("[Migration] Migrated ig_replied.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_replied: {e}")

    # 8. ig_welcomed.json
    if os.path.exists(IG_WELCOMED_FILE):
        try:
            with open(IG_WELCOMED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for uid in data:
                    db_execute("INSERT OR REPLACE INTO ig_welcomed (user_id, welcomed_at) VALUES (?, ?)", (uid, time.time()), commit=True)
            os.rename(IG_WELCOMED_FILE, IG_WELCOMED_FILE + ".bak")
            print("[Migration] Migrated ig_welcomed.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_welcomed: {e}")

    # 9. ig_messages_queue.json
    if os.path.exists(IG_MESSAGES_QUEUE_FILE):
        try:
            with open(IG_MESSAGES_QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    db_execute(
                        "INSERT INTO ig_messages_queue (payload, scheduled_at, processed) VALUES (?, ?, 0)",
                        (json.dumps(item), item.get("scheduled_at") or time.time()),
                        commit=True
                    )
            os.rename(IG_MESSAGES_QUEUE_FILE, IG_MESSAGES_QUEUE_FILE + ".bak")
            print("[Migration] Migrated ig_messages_queue.json successfully.")
        except Exception as e:
            print(f"[Migration Error] ig_messages_queue: {e}")


def load_conv_state():
    if not os.path.exists(CONV_STATE_PATH):
        return {}
    try:
        with open(CONV_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        now = int(time.time() * 1000)
        one_day_ms = 24 * 60 * 60 * 1000
        updated = False
        for jid in list(data.keys()):
            entry = data[jid]
            if entry and (not entry.get("updatedAt") or now - entry.get("updatedAt") > one_day_ms):
                del data[jid]
                updated = True
        if updated:
            save_conv_state(data)
        return data
    except Exception as e:
        print(f"Error loading conv state: {e}", flush=True)
        return {}

def save_conv_state(state):
    try:
        with open(CONV_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving conv state: {e}", flush=True)
        return False

def load_order_flow_config():
    if not os.path.exists(ORDER_FLOW_CONFIG_PATH):
        return {"enabled": False}
    try:
        with open(ORDER_FLOW_CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading order flow config: {e}", flush=True)
        return {"enabled": False}

def load_orders():
    if not os.path.exists(ORDERS_PATH):
        return []
    try:
        with open(ORDERS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"Error loading orders: {e}", flush=True)
    return []

def save_orders(orders):
    try:
        with open(ORDERS_PATH, "w", encoding="utf-8") as f:
            json.dump(orders, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving orders: {e}", flush=True)
        return False

# Instagram automation — separate storage (does not touch Facebook data)
IG_AUTOMATIONS_FILE = os.path.join(BASE_DIR, "ig_automations.json")
IG_KEYWORDS_FILE    = os.path.join(BASE_DIR, "ig_keywords.json")
IG_REPLIED_FILE     = os.path.join(BASE_DIR, "ig_replied.json")
IG_STATS_FILE       = os.path.join(BASE_DIR, "ig_stats.json")
IG_WELCOMED_FILE    = os.path.join(BASE_DIR, "ig_welcomed.json")
IG_SETTINGS_FILE    = os.path.join(BASE_DIR, "ig_settings.json")
IG_MESSAGES_LOG_FILE    = os.path.join(BASE_DIR, "ig_messages_log.json")
IG_MESSAGES_QUEUE_FILE  = os.path.join(BASE_DIR, "ig_messages_queue.json")
IG_LEADS_FILE           = os.path.join(BASE_DIR, "ig_leads.json")
IG_LINK_PAGES_FILE      = os.path.join(BASE_DIR, "ig_link_pages.json")
IG_SCHEDULED_POSTS_FILE = os.path.join(BASE_DIR, "ig_scheduled_posts.json")
IG_FLOWS_FILE           = os.path.join(BASE_DIR, "ig_flows.json")


# ── Post cache (avoids hitting Facebook API on every UI load) ─────────────────
_posts_cache      = []
_posts_cache_time = 0
POSTS_CACHE_TTL   = 300   # seconds — refresh every 5 minutes


def load_keywords():
    if os.path.exists(KEYWORDS_FILE):
        with open(KEYWORDS_FILE) as f:
            return json.load(f)
    return {"grass": "contact us on 9895138430"}

def save_keywords(data):
    with open(KEYWORDS_FILE, "w") as f:
        json.dump(data, f)


# ── Default automation rules — seeded on fresh deploy ────────────────────────
# These are the fallback rules written to automations.json when the file is
# missing (e.g. after a Railway redeploy wipes the ephemeral filesystem).
# Admins can override via the dashboard; those changes are saved back to disk.
DEFAULT_FB_AUTOMATIONS = [
    {
        "name": "bottle cleaner",
        "active": True,
        "scope": "specific",
        "post_ids": ["657207910809297_122182279484894047"],
        "action": "both",
        "keyword_type": "any",
        "keywords": [],
        "reply": "Thanks for your interest! 🧹 Check the link in bio for this amazing bottle cleaner.",
        "dm_message": "Hi! Thanks for commenting on our bottle cleaner Reel 🙌 Here is the product link: https://radikikktiktok.shop/",
        "thumbnail": ""
    },
    {
        "name": "all posts fallback",
        "active": True,
        "scope": "all",
        "post_ids": [],
        "action": "both",
        "keyword_type": "any",
        "keywords": [],
        "reply": "Thanks for commenting! 🙌 Check our page for amazing products.",
        "dm_message": "Hi! Thanks for commenting on our page 😊 Visit us at: https://radikikktiktok.shop/",
        "thumbnail": ""
    }
]

def load_automations():
    existing = []
    if os.path.exists(AUTOMATIONS_FILE):
        try:
            with open(AUTOMATIONS_FILE) as f:
                existing = json.load(f) or []
        except Exception:
            existing = []

    # Always ensure default rules are present (merge by name, don't duplicate)
    existing_names = {r.get("name") for r in existing}
    changed = False
    for default_rule in DEFAULT_FB_AUTOMATIONS:
        if default_rule["name"] not in existing_names:
            existing.append(default_rule)
            changed = True

    if changed:
        save_automations(existing)

    return existing

def save_automations(data):
    with open(AUTOMATIONS_FILE, "w") as f:
        json.dump(data, f)


# ── In-memory replied tracking (works on PythonAnywhere free plan) ────────────
# Persists to a simple text file for restarts; in-memory for speed & safety
_replied_set = set()

def _load_replied_from_file():
    global _replied_set
    try:
        if os.path.exists(REPLIED_FILE):
            with open(REPLIED_FILE) as f:
                _replied_set = set(json.load(f))
            print(f"[Replied] Loaded {len(_replied_set)} IDs from file")
    except Exception as e:
        print(f"[Replied] Load error: {e}")
        _replied_set = set()

def _save_replied_to_file():
    try:
        ids = list(_replied_set)[-MAX_REPLIED_STORE:]
        with open(REPLIED_FILE, "w") as f:
            json.dump(ids, f)
    except Exception as e:
        print(f"[Replied] Save error (non-critical): {e}")

def already_replied(comment_id):
    return comment_id in _replied_set

def mark_replied(comment_id):
    _replied_set.add(comment_id)
    if len(_replied_set) > MAX_REPLIED_STORE:
        # trim oldest — convert to list, keep last N
        trimmed = list(_replied_set)[-MAX_REPLIED_STORE:]
        _replied_set.clear()
        _replied_set.update(trimmed)
    _save_replied_to_file()


def fetch_page_posts(force=False):
    global _posts_cache, _posts_cache_time
    global PAGE_ACCESS_TOKEN
    # Return cached posts if still fresh
    if not force and _posts_cache and (time.time() - _posts_cache_time) < POSTS_CACHE_TTL:
        print("[Cache] Returning cached posts")
        return _posts_cache
    print("[Cache] Fetching fresh posts from Facebook...")
    
    tokens_to_try = [PAGE_ACCESS_TOKEN]
    tokens_to_try = [t for t in tokens_to_try if t]
    
    last_error = None
    for token in tokens_to_try:
        try:
            resp = requests.get(
                f"https://graph.facebook.com/v19.0/{PAGE_ID}/posts",
                params={
                    "fields": "id,message,story,created_time,full_picture,attachments{media_type,media}",
                    "limit":  20,
                    "access_token": token,
                },
                timeout=10,
            )
            data = resp.json()
            if resp.status_code != 200 or "error" in data:
                err_msg = data.get("error", {}).get("message", f"HTTP {resp.status_code}")
                raise Exception(err_msg)
            
            # If we reached here, it succeeded! Update global config if needed
            if PAGE_ACCESS_TOKEN != token:
                print("[Token Recovery] PAGE_ACCESS_TOKEN was outdated. Recovered using verified fallback token.")
                PAGE_ACCESS_TOKEN = token
                
            posts = []
            for item in data.get("data", []):
                thumbnail = item.get("full_picture", "")
                attachments = item.get("attachments", {}).get("data", [])
                media_type = "post"
                if attachments:
                    att = attachments[0]
                    media_type = att.get("media_type", "post")
                    if not thumbnail:
                        thumbnail = att.get("media", {}).get("image", {}).get("src", "")
                posts.append({
                    "id":         item["id"],
                    "message":    item.get("message") or item.get("story") or "No caption",
                    "created":    item.get("created_time", "")[:10],
                    "thumbnail":  thumbnail,
                    "media_type": media_type,
                })
            _posts_cache      = posts
            _posts_cache_time = time.time()
            return posts
        except Exception as e:
            last_error = e
            print(f"[fetch_page_posts] Attempt with token {token[:15]}... failed: {e}")
            continue
            
    # If all tokens failed, raise the last error
    raise last_error

def subscribe_page():
    url  = f"https://graph.facebook.com/v19.0/{PAGE_ID}/subscribed_apps"
    resp = requests.post(url, data={"subscribed_fields": "feed,messages", "access_token": PAGE_ACCESS_TOKEN})
    body = resp.json()
    if body.get("success"):
        print("[Facebook] Page subscribed OK")
    else:
        print("[Facebook] Subscription failed", body)

def reply_to_comment(comment_id, message):
    resp = requests.post(
        f"https://graph.facebook.com/v19.0/{comment_id}/comments",
        data={"message": message, "access_token": PAGE_ACCESS_TOKEN}
    )
    result = resp.json()
    if "id" in result:
        print(f"  [Comment Reply sent] ✅ → {message}")
    else:
        print(f"  [Comment Reply failed] ❌ → {result}")

def send_dm(user_id, message):
    """
    Send a private DM via Messenger.
    NOTE: This only works if the user has previously messaged your Page.
    Facebook does not allow cold DMs to commenters without prior interaction.
    """
    print(f"  [DM] Attempting to send to user_id={user_id}")
    resp = requests.post(
        f"https://graph.facebook.com/v19.0/me/messages",
        params={"access_token": PAGE_ACCESS_TOKEN},
        json={
            "recipient":      {"id": user_id},
            "message":        {"text": message},
            "messaging_type": "RESPONSE",
        },
        timeout=8,
    )
    result = resp.json()
    if "message_id" in result or "recipient_id" in result:
        print(f"  [DM sent ✅]")
    else:
        print(f"  [DM failed ❌] {result.get('error', {}).get('message', result)}")

def send_private_reply(comment_id, message):
    """
    Send a private DM reply directly to a Facebook Page comment.
    Works for regular Page post comments.
    Falls back to a public Messenger CTA comment for Reels (where private_replies is unsupported).
    """
    print(f"  [Private Reply] Attempting to send to comment_id={comment_id}")
    resp = requests.post(
        f"https://graph.facebook.com/v19.0/{comment_id}/private_replies",
        data={"message": message, "access_token": PAGE_ACCESS_TOKEN},
        timeout=8
    )
    result = resp.json()
    if result.get("success") or "id" in result:
        print(f"  [Private Reply sent ✅]")
        return

    err = result.get("error", {})
    print(f"  [Private Reply failed ❌] {err.get('message', result)}")

    # Fallback: post a public comment with a Messenger CTA link
    # (private_replies is not supported on Reel comments by Meta's API)
    if err.get("code") == 100 and err.get("error_subcode") == 33:
        print(f"  [Fallback] Posting public CTA comment (Reel private_replies not supported)")
        cta_message = f"We sent you a message! Tap here to get more details \u27a1 m.me/radikikk"
        cta_resp = requests.post(
            f"https://graph.facebook.com/v19.0/{comment_id}/comments",
            data={"message": cta_message, "access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        cta_result = cta_resp.json()
        if "id" in cta_result:
            print(f"  [Fallback CTA comment posted ✅] ID: {cta_result['id']}")
        else:
            print(f"  [Fallback CTA comment failed ❌] {cta_result}")


def handle_comment(value):
    if value.get("verb") != "add" or value.get("item") != "comment":
        return
    comment_id   = value.get("comment_id", "")
    comment_text = value.get("message", "").lower().strip()
    post_id      = value.get("post_id", "")
    user_id      = value.get("from", {}).get("id", "")
    from_name    = value.get("from", {}).get("name", "unknown")
    created_time = value.get("created_time", 0)
    print(f"[COMMENT] id={comment_id} from={from_name} text={comment_text}")

    # ── Skip comments made by the Page itself ─────────────────────────────────
    if user_id == PAGE_ID:
        print(f"[SKIP] Comment is from our own Page — ignoring")
        return

    # ── Duplicate guard ───────────────────────────────────────────────────────
    # Use user_id+post_id+text as dedup key because Facebook sends same
    # comment with different comment_ids in multiple webhook events
    dedup_key = f"{user_id}:{post_id}:{comment_text}"
    if already_replied(dedup_key):
        print(f"[SKIP] Already replied to this comment (dedup_key={dedup_key})")
        return

    automations = load_automations()
    for auto in automations:
        if not auto.get("active", True):
            continue
        scope = auto.get("scope", "all")
        if scope == "specific":
            clean_saved_ids = [pid.split("_")[-1] for pid in auto.get("post_ids", [])]
            clean_webhook_pid = post_id.split("_")[-1]
            if clean_webhook_pid not in clean_saved_ids:
                continue
        kw_type = auto.get("keyword_type", "any")
        matched = False
        if kw_type == "any":
            matched = True
        else:
            for kw in auto.get("keywords", []):
                if kw.lower() in comment_text:
                    matched = True
                    break

        if matched:
            print(f"  Auto '{auto['name']}' matched")
            action = auto.get("action", "comment")

            if action in ("comment", "both") and auto.get("reply"):
                reply_to_comment(comment_id, auto["reply"])

            if action in ("dm", "both") and auto.get("dm_message"):
                send_private_reply(comment_id, auto["dm_message"])

            mark_replied(dedup_key)   # ← prevent future duplicates
            break
    print("  ---")


# ══════════════════════════════════════════════════════════════════════════════
# Instagram automation (SuperProfile-style — separate from Facebook)
# Uses the same PAGE_ACCESS_TOKEN from .env
# ══════════════════════════════════════════════════════════════════════════════

_ig_media_cache      = []
_ig_media_cache_time = 0
_ig_replied_set      = set()


# ── SQLite Database Access Adapters ──
def load_ig_keywords():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM ig_settings WHERE key = 'keywords'")
        row = cursor.fetchone()
        if row:
            return json.loads(row["value"])
        return {}
    except Exception as e:
        print(f"Error loading IG keywords: {e}")
        return {}
    finally:
        conn.close()

def save_ig_keywords(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES ('keywords', ?)", (json.dumps(data),))
            conn.commit()
        except Exception as e:
            print(f"Error saving IG keywords: {e}")
        finally:
            conn.close()

def load_ig_automations():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ig_automations")
        rows = cursor.fetchall()
        rules = []
        for r in rows:
            rules.append({
                "name": r["name"],
                "reply": r["reply"],
                "action": r["action"],
                "dm_message": r["dm_message"],
                "trigger_type": r["trigger_type"],
                "scope": r["scope"],
                "post_ids": json.loads(r["post_ids"] or "[]"),
                "thumbnail": r["thumbnail"],
                "keyword_type": r["keyword_type"],
                "keywords": json.loads(r["keywords"] or "[]"),
                "active": bool(r["active"]),
                "delay_seconds": r["delay_seconds"],
                "link_url": r["link_url"],
                "follow_up_message": r["follow_up_message"],
                "ask_follow": bool(r["ask_follow"]),
                "follow_prompt": r["follow_prompt"],
                "email_capture": bool(r["email_capture"]),
                "email_prompt": r["email_prompt"]
            })
        return rules
    except Exception as e:
        print(f"Error loading IG automations: {e}")
        return []
    finally:
        conn.close()

def save_ig_automations(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_automations")
            for rule in data:
                cursor.execute(
                    "INSERT INTO ig_automations (name, reply, action, dm_message, trigger_type, scope, post_ids, thumbnail, keyword_type, keywords, active, delay_seconds, link_url, follow_up_message, ask_follow, follow_prompt, email_capture, email_prompt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        rule.get("name"),
                        rule.get("reply"),
                        rule.get("action"),
                        rule.get("dm_message"),
                        rule.get("trigger_type"),
                        rule.get("scope"),
                        json.dumps(rule.get("post_ids") or []),
                        rule.get("thumbnail"),
                        rule.get("keyword_type"),
                        json.dumps(rule.get("keywords") or []),
                        1 if rule.get("active") else 0,
                        rule.get("delay_seconds") or 0,
                        rule.get("link_url"),
                        rule.get("follow_up_message"),
                        1 if rule.get("ask_follow") else 0,
                        rule.get("follow_prompt"),
                        1 if rule.get("email_capture") else 0,
                        rule.get("email_prompt")
                    )
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving IG automations: {e}")
        finally:
            conn.close()

def load_ig_stats():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM ig_stats")
        rows = cursor.fetchall()
        stats = {"comment_replies": 0, "dms_sent": 0, "story_replies": 0, "live_replies": 0,
                 "dm_triggers": 0, "mentions_handled": 0, "dms_today": 0, "dms_today_date": ""}
        for r in rows:
            if r["key"] in stats:
                stats[r["key"]] = int(r["value"] or 0)
        # Fetch dms_today_date from settings
        cursor.execute("SELECT value FROM ig_settings WHERE key = 'dms_today_date'")
        date_row = cursor.fetchone()
        if date_row:
            stats["dms_today_date"] = date_row["value"]
        return stats
    except Exception as e:
        print(f"Error loading IG stats: {e}")
        return {"comment_replies": 0, "dms_sent": 0, "story_replies": 0, "live_replies": 0,
                 "dm_triggers": 0, "mentions_handled": 0, "dms_today": 0, "dms_today_date": ""}
    finally:
        conn.close()

def save_ig_stats(stats):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            for k, v in stats.items():
                if k == "dms_today_date":
                    cursor.execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES (?, ?)", (k, str(v)))
                else:
                    cursor.execute("INSERT OR REPLACE INTO ig_stats (key, value) VALUES (?, ?)", (k, int(v or 0)))
            conn.commit()
        except Exception as e:
            print(f"Error saving IG stats: {e}")
        finally:
            conn.close()

def bump_ig_stat(key):
    stats = load_ig_stats()
    stats[key] = stats.get(key, 0) + 1
    save_ig_stats(stats)

def _load_ig_replied_from_file():
    pass

def _save_ig_replied_to_file():
    pass

def ig_already_replied(key):
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM ig_messages_log WHERE comment_id = ?", (key,))
        return cursor.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()

def ig_mark_replied(key):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO ig_messages_log (comment_id, timestamp, success) VALUES (?, ?, 1)", (key, time.time()))
            conn.commit()
        except Exception as e:
            print(f"Error marking replied: {e}")
        finally:
            conn.close()

def _load_ig_welcomed():
    pass

def _save_ig_welcomed():
    pass

def ig_already_welcomed(user_id):
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM ig_welcomed WHERE user_id = ?", (str(user_id),))
        return cursor.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()

def ig_mark_welcomed(user_id):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO ig_welcomed (user_id, welcomed_at) VALUES (?, ?)", (str(user_id), time.time()))
            conn.commit()
        except Exception as e:
            print(f"Error marking welcomed: {e}")
        finally:
            conn.close()

def load_ig_settings():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM ig_settings WHERE key = 'general_settings'")
        row = cursor.fetchone()
        if row:
            return json.loads(row["value"])
        return {"daily_dm_cap": 200}
    except Exception as e:
        print(f"Error loading settings: {e}")
        return {"daily_dm_cap": 200}
    finally:
        conn.close()

def save_ig_settings(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES ('general_settings', ?)", (json.dumps(data),))
            conn.commit()
        except Exception as e:
            print(f"Error saving settings: {e}")
        finally:
            conn.close()

def load_ig_messages_log():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT comment_id, timestamp, success FROM ig_messages_log")
        rows = cursor.fetchall()
        return [{"comment_id": r["comment_id"], "timestamp": r["timestamp"], "success": bool(r["success"])} for r in rows]
    except Exception as e:
        print(f"Error loading message logs: {e}")
        return []
    finally:
        conn.close()

def save_ig_messages_log(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_messages_log")
            for entry in data:
                cursor.execute(
                    "INSERT OR REPLACE INTO ig_messages_log (comment_id, timestamp, success) VALUES (?, ?, ?)",
                    (entry.get("comment_id"), entry.get("timestamp"), 1 if entry.get("success") else 0)
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving message logs: {e}")
        finally:
            conn.close()

def load_ig_messages_queue():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT payload FROM ig_messages_queue WHERE processed = 0")
        rows = cursor.fetchall()
        return [json.loads(r["payload"]) for r in rows]
    except Exception as e:
        print(f"Error loading message queue: {e}")
        return []
    finally:
        conn.close()

def save_ig_messages_queue(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_messages_queue WHERE processed = 0")
            for item in data:
                cursor.execute(
                    "INSERT INTO ig_messages_queue (payload, scheduled_at, processed) VALUES (?, ?, 0)",
                    (json.dumps(item), item.get("scheduled_at") or time.time())
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving message queue: {e}")
        finally:
            conn.close()

def load_ig_leads():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT email, user_id, username, automation_name, captured_at FROM ig_leads")
        rows = cursor.fetchall()
        return [{
            "email": r["email"],
            "user_id": r["user_id"],
            "username": r["username"],
            "automation_name": r["automation_name"],
            "captured_at": r["captured_at"]
        } for r in rows]
    except Exception as e:
        print(f"Error loading leads: {e}")
        return []
    finally:
        conn.close()

def save_ig_leads(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_leads")
            for lead in data:
                cursor.execute(
                    "INSERT INTO ig_leads (email, user_id, username, automation_name, captured_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        lead.get("email"),
                        lead.get("user_id"),
                        lead.get("username"),
                        lead.get("automation_name"),
                        lead.get("captured_at") or time.time()
                    )
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving leads: {e}")
        finally:
            conn.close()

def load_ig_link_pages():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM ig_settings WHERE key = 'link_pages'")
        row = cursor.fetchone()
        if row:
            return json.loads(row["value"])
        return {}
    except Exception as e:
        print(f"Error loading link pages: {e}")
        return {}
    finally:
        conn.close()

def save_ig_link_pages(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO ig_settings (key, value) VALUES ('link_pages', ?)", (json.dumps(data),))
            conn.commit()
        except Exception as e:
            print(f"Error saving link pages: {e}")
        finally:
            conn.close()

def load_ig_scheduled_posts():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, media_url, caption, scheduled_time, status, error FROM ig_scheduled_posts")
        rows = cursor.fetchall()
        return [{
            "id": r["id"],
            "media_url": r["media_url"],
            "caption": r["caption"],
            "scheduled_time": r["scheduled_time"],
            "status": r["status"],
            "error": r["error"]
        } for r in rows]
    except Exception as e:
        print(f"Error loading scheduled posts: {e}")
        return []
    finally:
        conn.close()

def save_ig_scheduled_posts(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_scheduled_posts")
            for post in data:
                cursor.execute(
                    "INSERT OR REPLACE INTO ig_scheduled_posts (id, media_url, caption, scheduled_time, status, error) VALUES (?, ?, ?, ?, ?, ?)",
                    (post.get("id"), post.get("media_url"), post.get("caption"), post.get("scheduled_time"), post.get("status"), post.get("error"))
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving scheduled posts: {e}")
        finally:
            conn.close()

def load_ig_flows():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT flow_key, steps FROM ig_flows")
        rows = cursor.fetchall()
        flows = {}
        for r in rows:
            flows[r["flow_key"]] = json.loads(r["steps"])
        return flows
    except Exception as e:
        print(f"Error loading flows: {e}")
        return {}
    finally:
        conn.close()

def save_ig_flows(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_flows")
            for k, v in data.items():
                cursor.execute("INSERT OR REPLACE INTO ig_flows (flow_key, steps) VALUES (?, ?)", (k, json.dumps(v)))
            conn.commit()
        except Exception as e:
            print(f"Error saving flows: {e}")
        finally:
            conn.close()



def load_ig_conv_state():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, step, answers, updatedAt FROM ig_conv_state")
        rows = cursor.fetchall()
        data = {}
        now = int(time.time() * 1000)
        one_day_ms = 24 * 60 * 60 * 1000
        for r in rows:
            uid = r["user_id"]
            u_time = r["updatedAt"]
            if u_time and now - u_time > one_day_ms:
                with db_lock:
                    conn2 = get_db_conn()
                    try:
                        cursor2 = conn2.cursor()
                        cursor2.execute("DELETE FROM ig_conv_state WHERE user_id = ?", (uid,))
                        conn2.commit()
                    finally:
                        conn2.close()
            else:
                data[uid] = {
                    "step": r["step"],
                    "answers": json.loads(r["answers"] or "{}"),
                    "updatedAt": u_time
                }
        return data
    except Exception as e:
        print(f"Error loading conversation states: {e}")
        return {}
    finally:
        conn.close()

def save_ig_conv_state(data):
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ig_conv_state")
            for k, v in data.items():
                cursor.execute(
                    "INSERT OR REPLACE INTO ig_conv_state (user_id, step, answers, updatedAt) VALUES (?, ?, ?, ?)",
                    (k, v.get("step"), json.dumps(v.get("answers") or {}), v.get("updatedAt") or int(time.time() * 1000))
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving conversation states: {e}")
        finally:
            conn.close()

def is_positive_reply(text):
    return text.lower().strip() in ("yes", "y", "yep", "yeah", "sure", "ok", "okay", "agree", "1")

def is_negative_reply(text):
    return text.lower().strip() in ("no", "n", "nope", "nah", "cancel", "stop", "2")

def is_valid_email(text):
    return bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", text.strip()))

def capture_ig_lead(user_id, username, email=None, phone=None, automation_name=None):
    leads = load_ig_leads()
    new_lead = {
        "user_id": str(user_id),
        "username": username or "",
        "email": email or "",
        "phone": phone or "",
        "automation_name": automation_name or "",
        "captured_at": time.time()
    }
    leads.append(new_lead)
    save_ig_leads(leads)
    print(f"[Lead Captured] {new_lead}", flush=True)
    
    # Sync with external CRM Webhook if configured
    settings = load_ig_settings()
    crm_webhook = settings.get("crm_webhook_url")
    if crm_webhook:
        try:
            requests.post(crm_webhook, json=new_lead, timeout=5)
            print(f"[CRM Sync] Lead successfully pushed to CRM: {crm_webhook}", flush=True)
        except Exception as e:
            print(f"[CRM Sync Error] Failed to push to CRM: {e}", flush=True)


# ── Daily DM cap enforcement ───────────────────────────────────────────────
def daily_cap_ok() -> bool:
    """Return True if today's DM count is below the configured cap."""
    stats = load_ig_stats()
    today = time.strftime("%Y-%m-%d")
    if stats.get("dms_today_date") != today:
        stats["dms_today"] = 0
        stats["dms_today_date"] = today
        save_ig_stats(stats)
    cap = load_ig_settings().get("daily_dm_cap", 200)
    return stats.get("dms_today", 0) < cap

def bump_daily_dm():
    """Increment today's DM counter (resets on new calendar day)."""
    stats = load_ig_stats()
    today = time.strftime("%Y-%m-%d")
    if stats.get("dms_today_date") != today:
        stats["dms_today"] = 0
        stats["dms_today_date"] = today
    stats["dms_today"] = stats.get("dms_today", 0) + 1
    save_ig_stats(stats)


def discover_ig_user_id():
    global IG_USER_ID
    if IG_USER_ID:
        return IG_USER_ID
    try:
        resp = requests.get(
            f"{GRAPH_URL}/{PAGE_ID}",
            params={"fields": "instagram_business_account", "access_token": PAGE_ACCESS_TOKEN},
            timeout=10,
        )
        data  = resp.json()
        ig_id = data.get("instagram_business_account", {}).get("id")
        if ig_id:
            IG_USER_ID = ig_id
            print(f"[Instagram] Discovered IG_USER_ID={ig_id}")
        else:
            print(f"[Instagram] No Business account linked: {data}")
    except Exception as e:
        print(f"[Instagram] Discovery error: {e}")
    return IG_USER_ID


def fetch_ig_media(force=False):
    global _ig_media_cache, _ig_media_cache_time
    global PAGE_ACCESS_TOKEN
    if not force and _ig_media_cache and (time.time() - _ig_media_cache_time) < POSTS_CACHE_TTL:
        return _ig_media_cache
    if not IG_USER_ID:
        raise Exception("IG_USER_ID is not configured")
        
    tokens_to_try = [PAGE_ACCESS_TOKEN]
    tokens_to_try = [t for t in tokens_to_try if t]
    
    last_error = None
    for token in tokens_to_try:
        try:
            resp = requests.get(
                f"{GRAPH_URL}/{IG_USER_ID}/media",
                params={
                    "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink",
                    "limit":  24,
                    "access_token": token,
                },
                timeout=10,
            )
            data = resp.json()
            if resp.status_code != 200 or "error" in data:
                err_msg = data.get("error", {}).get("message", f"HTTP {resp.status_code}")
                raise Exception(err_msg)
            
            # Success! Update global config if needed
            if PAGE_ACCESS_TOKEN != token:
                print("[Token Recovery] PAGE_ACCESS_TOKEN was outdated. Recovered using verified fallback token.")
                PAGE_ACCESS_TOKEN = token
                
            media = []
            for item in data.get("data", []):
                mtype = item.get("media_type", "IMAGE").lower()
                media.append({
                    "id":         item["id"],
                    "message":    item.get("caption") or "No caption",
                    "created":    item.get("timestamp", "")[:10],
                    "thumbnail":  item.get("thumbnail_url") or item.get("media_url", ""),
                    "media_type": mtype,
                })
            _ig_media_cache      = media
            _ig_media_cache_time = time.time()
            return media
        except Exception as e:
            last_error = e
            print(f"[fetch_ig_media] Attempt with token {token[:15]}... failed: {e}")
            continue
            
    raise last_error


def subscribe_instagram():
    if not IG_USER_ID:
        print("[Instagram] Skipping subscription — no IG_USER_ID")
        return
    resp = requests.post(
        f"{GRAPH_URL}/{IG_USER_ID}/subscribed_apps",
        data={
            "subscribed_fields": "comments,live_comments,messages,mentions",
            "access_token": PAGE_ACCESS_TOKEN,
        },
        timeout=10,
    )
    body = resp.json()
    if body.get("success"):
        print("[Instagram] Account subscribed OK")
    else:
        print("[Instagram] Subscription failed", body)


def personalize_ig_message(template, username=""):
    msg = (template or "").replace("{username}", username or "there")
    return msg


def build_ig_dm_body(auto, username=""):
    parts = []
    if auto.get("ask_follow") and auto.get("follow_prompt"):
        parts.append(auto["follow_prompt"])
    dm = personalize_ig_message(auto.get("dm_message", ""), username)
    if dm:
        parts.append(dm)
    if auto.get("link_url"):
        parts.append(auto["link_url"])
    return "\n\n".join(p for p in parts if p)


def transcode_to_ogg_opus(base64_data):
    if "," in base64_data:
        base64_data = base64_data.split(",")[1]
    input_bytes = base64.b64decode(base64_data)
    
    import tempfile
    import subprocess
    
    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as infile:
        infile.write(input_bytes)
        in_path = infile.name
        
    out_path = in_path.replace(".webm", ".ogg")
    
    try:
        cmd = ["ffmpeg", "-y", "-i", in_path, "-vn", "-c:a", "libopus", "-b:a", "16k", "-ac", "1", "-ar", "16000", "-avoid_negative_ts", "make_zero", "-f", "ogg", out_path]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        with open(out_path, "rb") as outfile:
            transcoded_bytes = outfile.read()
        transcoded_b64 = base64.b64encode(transcoded_bytes).decode("utf-8")
        
        try: os.remove(in_path)
        except: pass
        try: os.remove(out_path)
        except: pass
        return transcoded_b64
    except Exception as e:
        print(f"[Python Audio Transcode] FFmpeg failed or not found: {e}", flush=True)
        try: os.remove(in_path)
        except: pass
        try: os.remove(out_path)
        except: pass
        return base64_data


def upload_wa_media(phone_id, token, base64_data, mime_type="image/jpeg"):
    url = f"https://graph.facebook.com/v19.0/{phone_id}/media"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    if "," in base64_data:
        base64_data = base64_data.split(",")[1]
    file_bytes = base64.b64decode(base64_data)
    ext = "jpg"
    if "png" in mime_type: ext = "png"
    elif "webp" in mime_type: ext = "webp"
    elif "ogg" in mime_type: ext = "ogg"
    elif "opus" in mime_type: ext = "opus"
    
    files = {
        "file": (f"media_file.{ext}", file_bytes, mime_type)
    }
    data = {
        "messaging_product": "whatsapp"
    }
    try:
        r = requests.post(url, headers=headers, files=files, data=data, timeout=20)
        r.raise_for_status()
        return r.json().get("id")
    except requests.exceptions.HTTPError as err:
        print(f"Meta Media API Error Response: {err.response.text}", flush=True)
        raise err


def send_official_wa_message(to_number, text=None, image_base64=None, voice_base64=None):
    token = os.getenv("PAGE_ACCESS_TOKEN")
    phone_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
    if not token or not phone_id:
        raise Exception("WhatsApp Meta API credentials (WHATSAPP_PHONE_NUMBER_ID) missing.")
    url = f"https://graph.facebook.com/v19.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    clean_to = re.sub(r"\D", "", to_number)
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": clean_to
    }
    if image_base64 and isinstance(image_base64, str) and len(image_base64) > 100:
        mime = "image/jpeg"
        if "image/png" in image_base64:
            mime = "image/png"
        elif "image/webp" in image_base64:
            mime = "image/webp"
        media_id = upload_wa_media(phone_id, token, image_base64, mime)
        payload["type"] = "image"
        payload["image"] = {"id": media_id}
        if text:
            payload["image"]["caption"] = text
            
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as err:
            print(f"Meta API Error Response: {err.response.text}", flush=True)
            raise err
            
    elif voice_base64 and isinstance(voice_base64, str) and len(voice_base64) > 100:
        transcoded_b64 = transcode_to_ogg_opus(voice_base64)
        media_id = upload_wa_media(phone_id, token, transcoded_b64, "audio/ogg; codecs=opus")
        payload["type"] = "audio"
        payload["audio"] = {"id": media_id}
        
        try:
            r1 = requests.post(url, headers=headers, json=payload, timeout=15)
            r1.raise_for_status()
            
            # If text is also specified, send it as a second message
            if text:
                text_payload = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": clean_to,
                    "type": "text",
                    "text": {"body": text, "preview_url": True}
                }
                r2 = requests.post(url, headers=headers, json=text_payload, timeout=15)
                r2.raise_for_status()
                
            return r1.json()
        except requests.exceptions.HTTPError as err:
            print(f"Meta API Error Response: {err.response.text}", flush=True)
            raise err
            
    else:
        payload["type"] = "text"
        payload["text"] = {"body": text, "preview_url": True}
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as err:
            print(f"Meta API Error Response: {err.response.text}", flush=True)
            raise err


def send_official_wa_interactive_buttons(to_number, body_text, buttons):
    token = os.getenv("PAGE_ACCESS_TOKEN")
    phone_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
    if not token or not phone_id:
        return
    url = f"https://graph.facebook.com/v19.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    clean_to = re.sub(r"\D", "", to_number)
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": clean_to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": body_text
            },
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {
                            "id": btn["id"],
                            "title": btn["title"][:20]
                        }
                    }
                    for btn in buttons
                ]
            }
        }
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[Python Interactive Button Send Failed]: {e}", flush=True)
        return send_official_wa_message(to_number, text=body_text)


def handle_official_wa_message(msg, contact):
    sender_wa_id = msg.get("from")
    sender_name = contact.get("profile", {}).get("name", "WhatsApp User")
    msg_type = msg.get("type")
    
    if msg_type not in ["text", "button", "interactive"] or not sender_wa_id:
        return
        
    text = ""
    if msg_type == "text":
        text = msg.get("text", {}).get("body", "").strip()
    elif msg_type == "button":
        text = msg.get("button", {}).get("payload", "").strip()
    elif msg_type == "interactive":
        text = msg.get("interactive", {}).get("button_reply", {}).get("id", "").strip()
        
    if not text:
        return
        
    print(f"[Official WhatsApp Message] from={sender_name} ({sender_wa_id}) text={text}", flush=True)
    
    # ── Conversation State Interception ──
    order_flow_config = load_order_flow_config()
    conv_state = load_conv_state()
    user_state = conv_state.get(sender_wa_id)
    
    if user_state and order_flow_config.get("enabled", True):
        # Handle escape hatch
        lower_input = text.lower().strip()
        if lower_input in ["cancel", "restart"]:
            if sender_wa_id in conv_state:
                del conv_state[sender_wa_id]
                save_conv_state(conv_state)
            send_official_wa_message(sender_wa_id, text="No problem, flow cancelled. Message us again anytime!")
            return
            
        if user_state.get("step") == "awaiting_second_message":
            user_state["step"] = "awaiting_payment_choice"
            user_state["updatedAt"] = int(time.time() * 1000)
            conv_state[sender_wa_id] = user_state
            save_conv_state(conv_state)
            
            time.sleep(1.2)
            choice_text = (
                f"How would you like to pay?\n\n"
                f"*1* - {order_flow_config.get('cod_label')}\n"
                f"*2* - {order_flow_config.get('online_label')}\n\n"
                f"Just reply with 1 or 2."
            )
            buttons = [
                {"id": "order_cod", "title": order_flow_config.get("cod_label")},
                {"id": "order_online", "title": order_flow_config.get("online_label")}
            ]
            send_official_wa_interactive_buttons(sender_wa_id, choice_text, buttons)
            return

        if user_state.get("step") == "awaiting_payment_choice":
            lower = text.lower().strip()
            is_cod = lower in ["1", "cod", "order_cod"] or "cash" in lower
            is_online = lower in ["2", "online", "order_online"] or "online" in lower
            
            if is_cod or is_online:
                user_state["paymentMethod"] = "cod" if is_cod else "online"
                user_state["step"] = "asking_question_0"
                user_state["updatedAt"] = int(time.time() * 1000)
                conv_state[sender_wa_id] = user_state
                save_conv_state(conv_state)
                
                first_question = order_flow_config.get("questions", [])[0]
                time.sleep(1.0)
                send_official_wa_message(sender_wa_id, text=first_question.get("prompt"))
            else:
                send_official_wa_message(
                    sender_wa_id, 
                    text=f"Sorry, I didn't get that. Please reply with *1* for {order_flow_config.get('cod_label')} or *2* for {order_flow_config.get('online_label')}."
                )
            return

        if user_state.get("step", "").startswith("asking_question_"):
            try:
                current_idx = int(user_state["step"].replace("asking_question_", ""))
            except:
                current_idx = 0
            questions = order_flow_config.get("questions", [])
            current_question = questions[current_idx]
            
            user_state["answers"][current_question["key"]] = text
            next_idx = current_idx + 1
            
            if next_idx < len(questions):
                user_state["step"] = f"asking_question_{next_idx}"
                user_state["updatedAt"] = int(time.time() * 1000)
                conv_state[sender_wa_id] = user_state
                save_conv_state(conv_state)
                
                time.sleep(0.8)
                send_official_wa_message(sender_wa_id, text=questions[next_idx].get("prompt"))
            else:
                payment_method = user_state.get("paymentMethod")
                template = order_flow_config.get("cod_confirmation_template" if payment_method == "cod" else "online_confirmation_template")
                
                final_text = template.replace("{payment_link}", order_flow_config.get("payment_link", ""))
                for key, val in user_state.get("answers", {}).items():
                    final_text = final_text.replace(f"{{{key}}}", val)
                    
                time.sleep(1.0)
                send_official_wa_message(sender_wa_id, text=final_text)
                print(f"Order flow completed for {sender_name} ({payment_method}).", flush=True)

                # Send order notification to owner 916282444918
                try:
                    answers_text = "\n".join([f"*{k}*: {v}" for k, v in user_state.get("answers", {}).items()])
                    owner_notification = (
                        f"📦 *New Order Received!*\n\n"
                        f"*Customer*: {sender_name} ({sender_wa_id})\n"
                        f"*Payment Mode*: {'Cash on Delivery (COD)' if payment_method == 'cod' else 'Online Payment'}\n\n"
                        f"*Details*:\n{answers_text}"
                    )
                    send_official_wa_message("916282444918", text=owner_notification)
                    print("Owner notification sent to 916282444918.", flush=True)
                except Exception as owner_err:
                    print(f"Failed to send order notification to owner: {owner_err}", flush=True)

                orders = load_orders()
                orders.append({
                    "jid": sender_wa_id,
                    "name": sender_name,
                    "paymentMethod": payment_method,
                    "answers": user_state.get("answers"),
                    "matchedKeywordPattern": user_state.get("matchedKeywordPattern"),
                    "completedAt": datetime.datetime.now().isoformat()
                })
                save_orders(orders)
                
                if sender_wa_id in conv_state:
                    del conv_state[sender_wa_id]
                    save_conv_state(conv_state)
            return
    
    # Load keywords
    kw_path = os.getenv("KEYWORDS_PATH")
    if not kw_path:
        for p in ["/data/keywords.json", "keywords.json", "../keywords.json"]:
            if os.path.exists(p):
                kw_path = p
                break
        if not kw_path:
            kw_path = os.path.join(BASE_DIR, "keywords.json")
    if not os.path.exists(kw_path):
        return
    try:
        with open(kw_path, "r", encoding="utf-8") as f:
            kw_map = json.load(f)
    except Exception as e:
        print(f"Error loading WhatsApp keywords: {e}")
        return
        
    clean_text = text.lower().strip()
    for kw_pattern, rule_data in kw_map.items():
        keywords = [k.strip().lower() for k in kw_pattern.split(",") if k.strip()]
        is_match = False
        for kw in keywords:
            if clean_text == kw:
                is_match = True
                break
            pattern = ""
            if kw and kw[0].isalnum():
                pattern += r"\b"
            pattern += re.escape(kw)
            if kw and kw[-1].isalnum():
                pattern += r"\b"
            try:
                if re.search(pattern, clean_text, re.IGNORECASE):
                    is_match = True
                    break
            except Exception:
                if kw in clean_text:
                    is_match = True
                    break
        if is_match:
            print(f"[Official WhatsApp Match] Pattern: {kw_pattern}", flush=True)
            reply_text = ""
            reply_image = None
            reply_voice = None
            if isinstance(rule_data, str):
                reply_text = rule_data
            elif isinstance(rule_data, dict):
                reply_text = rule_data.get("text", "")
                reply_image = rule_data.get("image")
                reply_voice = rule_data.get("voice")
                use_order_flow = rule_data.get("useOrderFlow", False)
            else:
                use_order_flow = False
                
            try:
                send_official_wa_message(
                    to_number=sender_wa_id,
                    text=reply_text,
                    image_base64=reply_image,
                    voice_base64=reply_voice
                )
                print(f"[Official WhatsApp Sent] To: {sender_wa_id}", flush=True)
                
                if use_order_flow and order_flow_config.get("enabled", True):
                    conv_state = load_conv_state()
                    conv_state[sender_wa_id] = {
                        "step": "awaiting_second_message",
                        "matchedKeywordPattern": kw_pattern,
                        "paymentMethod": None,
                        "answers": {},
                        "updatedAt": int(time.time() * 1000)
                    }
                    save_conv_state(conv_state)
                    print(f"Initialized order flow in awaiting_second_message state for {sender_name}.", flush=True)
                    
            except Exception as err:
                print(f"Failed to send official WhatsApp reply: {err}", flush=True)
            break


def reply_to_ig_comment(comment_id, message):
    resp = requests.post(
        f"{GRAPH_URL}/{comment_id}/replies",
        data={"message": message, "access_token": PAGE_ACCESS_TOKEN},
        timeout=8,
    )
    result = resp.json()
    if "id" in result:
        print(f"  [IG Comment Reply sent] ✅")
        bump_ig_stat("comment_replies")
    else:
        print(f"  [IG Comment Reply failed] ❌ → {result}")


IG_USER_INTERACTIONS_FILE = os.path.join(BASE_DIR, "ig_user_interactions.json")

def load_ig_user_interactions():
    if os.path.exists(IG_USER_INTERACTIONS_FILE):
        try:
            with open(IG_USER_INTERACTIONS_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_ig_user_interactions(data):
    try:
        with open(IG_USER_INTERACTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving user interactions: {e}")

def record_user_interaction(user_id):
    if not user_id:
        return
    interactions = load_ig_user_interactions()
    interactions[str(user_id)] = time.time()
    save_ig_user_interactions(interactions)

def get_last_user_interaction_time(user_id):
    interactions = load_ig_user_interactions()
    return interactions.get(str(user_id))

def queue_ig_message(recipient_id, text, comment_id=None, is_private_reply=False, automation_name=None, delay=0, quick_replies=None):
    queue = load_ig_messages_queue()
    queue.append({
        "recipient_id": recipient_id,
        "text": text,
        "comment_id": comment_id,
        "is_private_reply": is_private_reply,
        "automation_name": automation_name or "manual",
        "queued_at": time.time(),
        "delay": delay,
        "quick_replies": quick_replies
    })
    save_ig_messages_queue(queue)
    print(f"[Queue] Queued message to {recipient_id}: {text[:30]}...", flush=True)

def perform_ig_dm_send_with_buttons(user_id, text, quick_replies_list, tag=None) -> tuple[bool, str]:
    if not IG_USER_ID:
        return False, "IG_USER_ID not set"
    try:
        meta_replies = []
        for r in quick_replies_list:
            meta_replies.append({
                "content_type": "text",
                "title": r.get("title")[:20],
                "payload": r.get("payload")
            })
        
        json_payload = {
            "recipient": {"id": user_id},
            "message": {
                "text": text,
                "quick_replies": meta_replies
            }
        }
        if tag:
            json_payload["tag"] = tag
            json_payload["messaging_type"] = "MESSAGE_TAG"
            
        resp = requests.post(
            f"{GRAPH_URL}/{IG_USER_ID}/messages",
            params={"access_token": PAGE_ACCESS_TOKEN},
            json=json_payload,
            timeout=8,
        )
        result = resp.json()
        if "message_id" in result:
            bump_ig_stat("dms_sent")
            bump_daily_dm()
            return True, ""
        else:
            return False, result.get("error", {}).get("message", str(result))
    except Exception as e:
        return False, str(e)

def start_ig_flow(user_id, flow_key, username=""):
    flows = load_ig_flows()
    flow = flows.get(flow_key)
    if not flow:
        print(f"[Flow Error] Flow {flow_key} not found.")
        return False
    send_ig_flow_step(user_id, flow_key, "start", username)
    return True

def send_ig_flow_step(user_id, flow_key, step_id, username=""):
    flows = load_ig_flows()
    flow = flows.get(flow_key)
    if not flow:
        return
    step = flow.get(step_id)
    if not step:
        return
    
    text = personalize_ig_message(step.get("text", ""), username)
    replies = step.get("quick_replies", [])
    meta_replies = []
    for r in replies:
        meta_replies.append({
            "title": r.get("title"),
            "payload": f"flow:{flow_key}:{r.get('next_step')}"
        })
        
    if meta_replies:
        queue_ig_message(
            recipient_id=user_id,
            text=text,
            automation_name=f"flow:{flow_key}",
            quick_replies=meta_replies
        )
    else:
        queue_ig_message(
            recipient_id=user_id,
            text=text,
            automation_name=f"flow:{flow_key}"
        )
        
    if step.get("end"):
        conv_state = load_ig_conv_state()
        if str(user_id) in conv_state:
            del conv_state[str(user_id)]
        save_ig_conv_state(conv_state)
    else:
        conv_state = load_ig_conv_state()
        conv_state[str(user_id)] = {
            "step": f"flow_step:{step_id}",
            "automation_name": f"flow:{flow_key}",
            "flow_key": flow_key,
            "updatedAt": int(time.time() * 1000)
        }
        save_ig_conv_state(conv_state)

def perform_ig_private_reply_send(comment_id, message) -> tuple[bool, str]:
    if not IG_USER_ID:
        return False, "IG_USER_ID not set"
    if not daily_cap_ok():
        return False, "Daily DM cap reached"
    try:
        resp = requests.post(
            f"{GRAPH_URL}/{IG_USER_ID}/messages",
            params={"access_token": PAGE_ACCESS_TOKEN},
            json={"recipient": {"comment_id": comment_id}, "message": {"text": message}},
            timeout=8,
        )
        result = resp.json()
        if "message_id" in result:
            bump_ig_stat("dms_sent")
            bump_daily_dm()
            return True, ""
        else:
            return False, result.get("error", {}).get("message", str(result))
    except Exception as e:
        return False, str(e)

def perform_ig_dm_send(user_id, message, tag=None) -> tuple[bool, str]:
    if not IG_USER_ID:
        return False, "IG_USER_ID not set"
    if not daily_cap_ok():
        return False, "Daily DM cap reached"
    try:
        json_payload = {"recipient": {"id": user_id}, "message": {"text": message}}
        if tag:
            json_payload["tag"] = tag
            json_payload["messaging_type"] = "MESSAGE_TAG"
        resp = requests.post(
            f"{GRAPH_URL}/{IG_USER_ID}/messages",
            params={"access_token": PAGE_ACCESS_TOKEN},
            json=json_payload,
            timeout=8,
        )
        result = resp.json()
        if "message_id" in result:
            bump_ig_stat("dms_sent")
            bump_daily_dm()
            return True, ""
        else:
            return False, result.get("error", {}).get("message", str(result))
    except Exception as e:
        return False, str(e)

def send_ig_private_reply(comment_id, message, recipient_id=None, automation_name=None, delay=0):
    queue_ig_message(
        recipient_id=recipient_id,
        text=message,
        comment_id=comment_id,
        is_private_reply=True,
        automation_name=automation_name,
        delay=delay
    )

def send_ig_dm(user_id, message, automation_name=None, delay=0):
    queue_ig_message(
        recipient_id=user_id,
        text=message,
        comment_id=None,
        is_private_reply=False,
        automation_name=automation_name,
        delay=delay
    )

import random
def ig_queue_worker():
    print("[IG Worker] Started Instagram queue worker thread.", flush=True)
    while True:
        try:
            queue = load_ig_messages_queue()
            if not queue:
                time.sleep(3)
                continue
                
            # Count successful DMs sent in the last 1 hour
            logs = load_ig_messages_log()
            one_hour_ago = time.time() - 3600
            sent_in_last_hour = sum(1 for log in logs if log.get("sent_at", 0) >= one_hour_ago and log.get("status") == "success")
            
            if sent_in_last_hour >= 200:
                print(f"[IG Worker] Rolling hourly rate limit reached ({sent_in_last_hour}/200). Waiting 30s...", flush=True)
                time.sleep(30)
                continue
                
            # Process the first task
            msg_task = queue[0]
            recipient_id = msg_task.get("recipient_id")
            is_private_reply = msg_task.get("is_private_reply", False)
            automation_name = msg_task.get("automation_name", "manual")
            
            # Rule 2: One private reply per comment, within 7 days.
            # (Enforced during queue processing or during queuing, we will double check comment timestamp)
            comment_id = msg_task.get("comment_id")
            
            # Rule 3: One automated DM per user per 24-hour period from a comment or story trigger
            if automation_name != "manual" and recipient_id:
                twenty_four_hours_ago = time.time() - 86400
                already_sent_24h = any(
                    log.get("recipient_id") == recipient_id and 
                    log.get("sent_at", 0) >= twenty_four_hours_ago and 
                    log.get("status") == "success" and
                    log.get("is_automated", True)
                    for log in logs
                )
                if already_sent_24h:
                    print(f"[IG Worker] 🚫 24h automated DM rule check failed for user {recipient_id}. Skipping.", flush=True)
                    queue.pop(0)
                    save_ig_messages_queue(queue)
                    logs.append({
                        "recipient_id": recipient_id,
                        "text": msg_task.get("text"),
                        "status": "skipped_24h_limit",
                        "sent_at": time.time(),
                        "is_automated": True
                    })
                    save_ig_messages_log(logs)
                    continue

            # Rule 4: 24-hour standard messaging window + tags compliance
            tag_to_use = None
            if not is_private_reply and recipient_id:
                last_interaction_time = get_last_user_interaction_time(recipient_id)
                now = time.time()
                is_outside_24h = last_interaction_time is None or (now - last_interaction_time > 86400)
                
                if is_outside_24h:
                    if automation_name == "manual":
                        if last_interaction_time is not None and (now - last_interaction_time <= 604800):
                            tag_to_use = "HUMAN_AGENT"
                            print(f"[IG Worker] User {recipient_id} outside 24h but inside 7-day manual window. Applying HUMAN_AGENT tag.", flush=True)
                        else:
                            print(f"[IG Worker] 🚫 User {recipient_id} outside 7-day manual window. Skipping manual reply.", flush=True)
                            queue.pop(0)
                            save_ig_messages_queue(queue)
                            logs.append({
                                "recipient_id": recipient_id,
                                "text": msg_task.get("text"),
                                "status": "outside_manual_7day_window",
                                "sent_at": time.time(),
                                "is_automated": False
                            })
                            save_ig_messages_log(logs)
                            continue
                    else:
                        print(f"[IG Worker] 🚫 User {recipient_id} outside 24h automated window. Skipping automated reply.", flush=True)
                        queue.pop(0)
                        save_ig_messages_queue(queue)
                        logs.append({
                            "recipient_id": recipient_id,
                            "text": msg_task.get("text"),
                            "status": "outside_messaging_window",
                            "sent_at": time.time(),
                            "is_automated": True
                        })
                        save_ig_messages_log(logs)
                        continue

            # Human-like delay: 1-4 seconds
            rand_delay = random.uniform(1.0, 4.0)
            task_delay = float(msg_task.get("delay") or 0)
            total_delay = rand_delay + task_delay
            
            print(f"[IG Worker] Waiting {total_delay:.1f}s before sending...", flush=True)
            time.sleep(total_delay)
            
            text = msg_task.get("text")
            success = False
            error_message = ""
            
            if is_private_reply and comment_id:
                success, error_message = perform_ig_private_reply_send(comment_id, text)
            elif msg_task.get("quick_replies"):
                success, error_message = perform_ig_dm_send_with_buttons(recipient_id, text, msg_task["quick_replies"], tag=tag_to_use)
            else:
                success, error_message = perform_ig_dm_send(recipient_id, text, tag=tag_to_use)
                
            logs.append({
                "recipient_id": recipient_id,
                "text": text,
                "status": "success" if success else f"failed: {error_message}",
                "sent_at": time.time(),
                "is_automated": automation_name != "manual",
                "is_private_reply": is_private_reply
            })
            save_ig_messages_log(logs)
            
            queue.pop(0)
            save_ig_messages_queue(queue)
            
        except Exception as e:
            print(f"[IG Worker] Exception in loop: {e}", flush=True)
            time.sleep(5)


def publish_ig_media(media_url, caption):
    if not IG_USER_ID:
        return False, "IG_USER_ID not configured"
    try:
        is_video = any(ext in media_url.lower() for ext in (".mp4", ".mov", ".avi", ".m4v"))
        payload = {
            "caption": caption,
            "access_token": PAGE_ACCESS_TOKEN
        }
        if is_video:
            payload["video_url"] = media_url
            payload["media_type"] = "VIDEO"
        else:
            payload["image_url"] = media_url
            
        resp = requests.post(
            f"{GRAPH_URL}/{IG_USER_ID}/media",
            json=payload,
            timeout=15
        )
        res1 = resp.json()
        creation_id = res1.get("id")
        if not creation_id:
            return False, f"Failed container creation: {res1.get('error', {}).get('message', str(res1))}"
            
        resp_pub = requests.post(
            f"{GRAPH_URL}/{IG_USER_ID}/media_publish",
            json={
                "creation_id": creation_id,
                "access_token": PAGE_ACCESS_TOKEN
            },
            timeout=15
        )
        res2 = resp_pub.json()
        media_id = res2.get("id")
        if not media_id:
            return False, f"Failed publish: {res2.get('error', {}).get('message', str(res2))}"
        return True, media_id
    except Exception as e:
        return False, str(e)

def ig_scheduler_worker():
    print("[IG Scheduler] Started content scheduler thread.", flush=True)
    while True:
        try:
            posts = load_ig_scheduled_posts()
            changed = False
            now = time.time()
            for post in posts:
                if post.get("status") == "scheduled" and post.get("scheduled_time", 9999999999) <= now:
                    print(f"[IG Scheduler] Publishing scheduled post {post.get('id')}...", flush=True)
                    post["status"] = "publishing"
                    save_ig_scheduled_posts(posts)
                    
                    success, result = publish_ig_media(post.get("media_url"), post.get("caption", ""))
                    if success:
                        post["status"] = "published"
                        post["media_id"] = result
                        post["published_at"] = time.time()
                        print(f"[IG Scheduler] Successfully published post! Media ID: {result}", flush=True)
                    else:
                        post["status"] = "failed"
                        post["error"] = result
                        print(f"[IG Scheduler] Failed to publish: {result}", flush=True)
                    changed = True
            if changed:
                save_ig_scheduled_posts(posts)
        except Exception as e:
            print(f"[IG Scheduler] Error in loop: {e}", flush=True)
        time.sleep(15)




def _ig_keyword_match(auto, text):
    if auto.get("keyword_type", "any") == "any":
        return True
    return any(kw.lower() in text for kw in auto.get("keywords", []))


def _ig_scope_match(auto, media_id):
    if auto.get("scope", "all") == "all":
        return True
    return media_id in auto.get("post_ids", [])


def run_ig_automations(trigger_type, text, media_id="", comment_id="", user_id="", username=""):
    text = (text or "").lower().strip()
    for auto in load_ig_automations():
        if not auto.get("active", True):
            continue
        if auto.get("trigger_type", "comment") != trigger_type:
            continue
        if trigger_type in ("comment", "live") and not _ig_scope_match(auto, media_id):
            continue
        if not _ig_keyword_match(auto, text):
            continue

        print(f"  IG Auto '{auto['name']}' matched ({trigger_type})", flush=True)

        # Welcome DM: only fire once per user (first-time detection)
        if trigger_type == "welcome" and user_id:
            if ig_already_welcomed(user_id):
                print(f"  [SKIP] Welcome DM already sent to {user_id}", flush=True)
                continue
            ig_mark_welcomed(user_id)

        # Optional send delay — passed to queue instead of sleeping synchronously
        delay = int(auto.get("delay_seconds", 0))

        action = auto.get("action", "both")

        if action in ("comment", "both") and auto.get("reply") and comment_id:
            reply_to_ig_comment(comment_id, personalize_ig_message(auto["reply"], username))

        if action == "flow" and auto.get("link_url") and user_id:
            start_ig_flow(user_id, auto["link_url"], username)
            return True

        if action in ("dm", "both") and auto.get("dm_message"):
            dm_body = build_ig_dm_body(auto, username)
            
            if auto.get("email_capture") and auto.get("email_prompt") and user_id:
                if comment_id:
                    send_ig_private_reply(comment_id, dm_body, recipient_id=user_id, automation_name=auto.get("name"), delay=delay)
                else:
                    send_ig_dm(user_id, dm_body, automation_name=auto.get("name"), delay=delay)
                send_ig_dm(user_id, auto["email_prompt"], automation_name=auto.get("name"), delay=delay + 1)
                
                conv_state = load_ig_conv_state()
                conv_state[str(user_id)] = {
                    "step": "awaiting_email",
                    "automation_name": auto.get("name"),
                    "updatedAt": int(time.time() * 1000)
                }
                save_ig_conv_state(conv_state)
                
            elif auto.get("follow_up_message") and user_id:
                if comment_id:
                    send_ig_private_reply(comment_id, dm_body, recipient_id=user_id, automation_name=auto.get("name"), delay=delay)
                else:
                    send_ig_dm(user_id, dm_body, automation_name=auto.get("name"), delay=delay)
                
                # Sequence drip campaign delay
                f_delay = int(auto.get("follow_up_delay_seconds") or 3600)
                send_ig_dm(user_id, auto["follow_up_message"], automation_name=auto.get("name"), delay=delay + f_delay)
                
                conv_state = load_ig_conv_state()
                conv_state[str(user_id)] = {
                    "step": "awaiting_followup",
                    "automation_name": auto.get("name"),
                    "updatedAt": int(time.time() * 1000)
                }
                save_ig_conv_state(conv_state)
            else:
                if comment_id:
                    send_ig_private_reply(comment_id, dm_body, recipient_id=user_id, automation_name=auto.get("name"), delay=delay)
                elif user_id:
                    send_ig_dm(user_id, dm_body, automation_name=auto.get("name"), delay=delay)

        return True
    return False


def save_last_tester(user_id, username=None):
    if not user_id:
        return
    try:
        data = {"user_id": user_id, "username": username or "", "time": time.time()}
        with open(os.path.join(BASE_DIR, "ig_last_tester.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"[IG Tester] Saved last tester: ID={user_id}, username=@{username}")
    except Exception as e:
        print(f"[save_last_tester error] {e}")


def load_last_tester():
    try:
        path = os.path.join(BASE_DIR, "ig_last_tester.json")
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return None


import datetime

def is_comment_within_7_days(created_time) -> bool:
    if not created_time:
        return True
    try:
        if isinstance(created_time, (int, float)):
            epoch = float(created_time)
        else:
            try:
                epoch = float(created_time)
            except ValueError:
                # Parse ISO 8601 formats
                clean_dt = created_time.split("+")[0].split(".")[0]
                dt = datetime.datetime.strptime(clean_dt, "%Y-%m-%dT%H:%M:%S")
                epoch = dt.timestamp()
        return (time.time() - epoch) <= (7 * 86400)
    except Exception as e:
        print(f"[Comment Date Check] Warning parsing date '{created_time}': {e}", flush=True)
        return True

def handle_ig_comment(value, trigger_type="comment"):
    comment_id = value.get("comment_id") or value.get("id", "")
    text       = (value.get("text") or "").lower().strip()
    media_id   = value.get("media", {}).get("id", "")
    user_id    = value.get("from", {}).get("id", "")
    username   = value.get("from", {}).get("username", "")
    if not comment_id:
        return
    print(f"[IG {trigger_type.upper()}] id={comment_id} from=@{username} text={text}", flush=True)

    if IG_USER_ID and str(user_id) == str(IG_USER_ID):
        print("[SKIP] Own account comment", flush=True)
        return

    # Rule 2: private reply must be sent within 7 days of comment
    created_time = value.get("created_time")
    if not is_comment_within_7_days(created_time):
        print(f"[SKIP] Comment is older than 7 days (created_time={created_time})", flush=True)
        return

    # Auto-hide spam comments
    settings = load_ig_settings()
    spam_words_str = settings.get("spam_keywords", "")
    if spam_words_str:
        spam_words = [w.strip().lower() for w in spam_words_str.split(",") if w.strip()]
        if any(w in text for w in spam_words):
            print(f"[Auto-Moderation] ⚠️ Hiding comment {comment_id} due to spam match: '{text}'", flush=True)
            try:
                requests.post(
                    f"{GRAPH_URL}/{comment_id}",
                    data={"hide": True, "access_token": PAGE_ACCESS_TOKEN},
                    timeout=8
                )
            except Exception as e:
                print(f"[Auto-Moderation Error] Failed to hide comment: {e}", flush=True)
            return

    save_last_tester(user_id, username)
    record_user_interaction(user_id)

    dedup_key = f"ig:{trigger_type}:{user_id}:{media_id}:{text}"
    if ig_already_replied(dedup_key):
        print(f"[SKIP] Already handled (dedup_key={dedup_key})", flush=True)
        return

    if run_ig_automations(trigger_type, text, media_id, comment_id, user_id, username):
        if trigger_type == "live":
            bump_ig_stat("live_replies")
        ig_mark_replied(dedup_key)
    print("  ---", flush=True)


def handle_ig_messaging(event):
    sender_id = event.get("sender", {}).get("id", "")
    recipient_id = event.get("recipient", {}).get("id", "")
    message   = event.get("message", {}) or {}
    is_echo   = message.get("is_echo", False)
    
    # Identify actual user (follower) ID to avoid registering the bot itself
    actual_user_id = recipient_id if is_echo else sender_id
    if actual_user_id and str(actual_user_id) != str(IG_USER_ID):
        save_last_tester(actual_user_id, "")
        if not is_echo:
            record_user_interaction(actual_user_id)
        
    if not sender_id or not message:
        return

    text = (message.get("text") or "").lower().strip()
    reply_to = message.get("reply_to") or {}

    postback_payload = ""
    if event.get("postback"):
        postback_payload = event.get("postback", {}).get("payload", "")
    elif message.get("quick_reply"):
        postback_payload = message.get("quick_reply", {}).get("payload", "")

    if postback_payload.startswith("flow:"):
        parts = postback_payload.split(":")
        if len(parts) >= 3:
            flow_key = parts[1]
            next_step = parts[2]
            print(f"[Flow Route] Routing user {sender_id} in flow {flow_key} to step {next_step}", flush=True)
            send_ig_flow_step(sender_id, flow_key, next_step)
            return

    # Identify story mention
    is_story_mention = False
    attachments = message.get("attachments", []) or []
    for att in attachments:
        if att.get("type") == "story_mention":
            is_story_mention = True
            break
    if message.get("story_mention"):
        is_story_mention = True

    if is_story_mention:
        print(f"[IG STORY MENTION] from={sender_id}", flush=True)
        dedup_key = f"ig:story_mention:{sender_id}:{message.get('mid')}"
        if ig_already_replied(dedup_key):
            return
        if run_ig_automations("story_mention", text, user_id=sender_id):
            bump_ig_stat("story_replies")
            ig_mark_replied(dedup_key)
        return

    if reply_to.get("story"):
        print(f"[IG STORY REPLY] from={sender_id} text={text}", flush=True)
        dedup_key = f"ig:story:{sender_id}:{text}"
        if ig_already_replied(dedup_key):
            return
        if run_ig_automations("story", text, user_id=sender_id):
            bump_ig_stat("story_replies")
            ig_mark_replied(dedup_key)
        return

    if text:
        if not is_echo:
            print(f"[IG DM] from={sender_id} text={text}", flush=True)
            
            # ── State Interception ──
            conv_state = load_ig_conv_state()
            user_state = conv_state.get(str(sender_id))
            
            if user_state:
                step = user_state.get("step")
                auto_name = user_state.get("automation_name")
                auto = next((a for a in load_ig_automations() if a.get("name") == auto_name), None)
                
                if step == "awaiting_email" and auto:
                    if is_valid_email(text):
                        email = text.strip()
                        capture_ig_lead(user_id=sender_id, username="", email=email, automation_name=auto_name)
                        confirm_msg = auto.get("email_success_message") or f"Thank you! We've saved your email: {email}"
                        send_ig_dm(sender_id, confirm_msg, automation_name=auto_name)
                        del conv_state[str(sender_id)]
                        save_ig_conv_state(conv_state)
                        return
                    else:
                        if text.lower() == "cancel":
                            send_ig_dm(sender_id, "Email capture cancelled.", automation_name=auto_name)
                            del conv_state[str(sender_id)]
                            save_ig_conv_state(conv_state)
                        else:
                            retry_prompt = auto.get("email_retry_prompt") or "That doesn't look like a valid email. Please reply with a valid email address or type 'cancel' to skip."
                            send_ig_dm(sender_id, retry_prompt, automation_name=auto_name)
                        return
                
                elif step == "awaiting_followup" and auto:
                    if is_positive_reply(text):
                        pos_reply = auto.get("branch_yes_reply") or "Awesome! Glad to hear that."
                        send_ig_dm(sender_id, pos_reply, automation_name=auto_name)
                    elif is_negative_reply(text):
                        neg_reply = auto.get("branch_no_reply") or "No problem. Let us know if you change your mind."
                        send_ig_dm(sender_id, neg_reply, automation_name=auto_name)
                    else:
                        print(f"[IG State] User sent non-matching text in awaiting_followup. Clearing state.", flush=True)
                    
                    del conv_state[str(sender_id)]
                    save_ig_conv_state(conv_state)
                    return
            
            dedup_key = f"ig:dm:{sender_id}:{text}"
            if ig_already_replied(dedup_key):
                return
            if run_ig_automations("dm", text, user_id=sender_id):
                bump_ig_stat("dm_triggers")
                ig_mark_replied(dedup_key)
                return
            for kw, reply in load_ig_keywords().items():
                if kw.lower() in text:
                    send_ig_dm(sender_id, reply)
                    bump_ig_stat("dm_triggers")
                    ig_mark_replied(dedup_key)
                    return


def handle_ig_mention(value):
    """
    Handle @mention of our IG account in someone else's post/story.
    Webhook field: mentions / mention_tag
    """
    media_id = value.get("media_id", "")
    user_id  = value.get("from", {}).get("id", "")
    username = value.get("from", {}).get("username", "")
    text     = (value.get("text") or "").lower().strip()
    print(f"[IG MENTION] from=@{username} media={media_id} text={text}", flush=True)

    if IG_USER_ID and str(user_id) == str(IG_USER_ID):
        print("[SKIP] Own account mention", flush=True)
        return

    save_last_tester(user_id, username)
    record_user_interaction(user_id)

    dedup_key = f"ig:mention:{user_id}:{media_id}"
    if ig_already_replied(dedup_key):
        print(f"[SKIP] Already handled mention (dedup_key={dedup_key})", flush=True)
        return

    if run_ig_automations("mention", text, media_id=media_id, user_id=user_id, username=username):
        bump_ig_stat("mentions_handled")
        ig_mark_replied(dedup_key)
    print("  ---", flush=True)


HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>AutoReply Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f2f5;color:#1c1e21}
    header{background:#fff;border-bottom:1px solid #e4e6eb;padding:14px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
    .logo{display:flex;align-items:center;gap:10px;font-size:18px;font-weight:700;color:#1877f2}
    .container{max-width:900px;margin:28px auto;padding:0 16px}
    .card{background:#fff;border-radius:14px;padding:22px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
    .card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px}
    .card-header h2{font-size:16px;font-weight:600}
    .stats{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px}
    .stat-box{background:#fff;border-radius:12px;padding:18px;box-shadow:0 1px 3px rgba(0,0,0,0.08);text-align:center}
    .stat-num{font-size:28px;font-weight:700;color:#1877f2}
    .stat-label{font-size:12px;color:#65676b;margin-top:4px}
    .auto-item{border:1px solid #e4e6eb;border-radius:12px;padding:16px;margin-bottom:12px;display:flex;align-items:center;gap:14px}
    .auto-thumb{width:52px;height:52px;border-radius:8px;object-fit:cover;flex-shrink:0}
    .auto-thumb-ph{width:52px;height:52px;border-radius:8px;background:#e8f0fe;display:flex;align-items:center;justify-content:center;font-size:22px;flex-shrink:0}
    .auto-info{flex:1;min-width:0}
    .auto-name{font-weight:600;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .auto-meta{font-size:12px;color:#65676b;margin-top:3px;display:flex;flex-wrap:wrap;gap:6px;align-items:center}
    .auto-actions{display:flex;align-items:center;gap:8px;flex-shrink:0}
    .pill{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600}
    .pill-blue{background:#e7f3ff;color:#1877f2}
    .pill-purple{background:#f3e8ff;color:#7c3aed}
    .pill-green{background:#f0fdf4;color:#16a34a}
    .empty-state{text-align:center;padding:40px 20px;color:#65676b}
    .empty-state .icon{font-size:40px;margin-bottom:10px}
    .btn{padding:7px 14px;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;transition:opacity .2s}
    .btn:hover{opacity:.85}
    .btn-primary{background:#1877f2;color:#fff;padding:9px 18px;font-size:14px}
    .btn-danger{background:#ff4d4f;color:#fff}
    .btn-edit{background:#e7f3ff;color:#1877f2}
    .toggle{position:relative;display:inline-block;width:40px;height:22px}
    .toggle input{opacity:0;width:0;height:0}
    .slider{position:absolute;cursor:pointer;inset:0;background:#ccd0d5;border-radius:22px;transition:.3s}
    .slider:before{position:absolute;content:"";height:16px;width:16px;left:3px;bottom:3px;background:white;border-radius:50%;transition:.3s}
    input:checked+.slider{background:#1877f2}
    input:checked+.slider:before{transform:translateX(18px)}
    .overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:100;align-items:center;justify-content:center}
    .overlay.open{display:flex}
    .modal{background:#fff;border-radius:18px;width:92%;max-width:520px;max-height:92vh;overflow-y:auto;animation:pop .2s ease}
    @keyframes pop{from{transform:scale(.95);opacity:0}to{transform:scale(1);opacity:1}}
    .modal-header{padding:20px 20px 0;display:flex;align-items:center;justify-content:space-between}
    .modal-header h3{font-size:18px;font-weight:700}
    .modal-close{background:none;border:none;font-size:24px;cursor:pointer;color:#65676b}
    .modal-body{padding:20px}
    .step{display:none}
    .step.active{display:block}
    .step-title{font-size:15px;font-weight:600;margin-bottom:5px}
    .step-sub{font-size:13px;color:#65676b;margin-bottom:16px}
    /* Step indicator */
    .step-dots{display:flex;justify-content:center;gap:8px;margin-bottom:20px}
    .step-dot{width:8px;height:8px;border-radius:50%;background:#e4e6eb;transition:background .2s}
    .step-dot.active{background:#1877f2;width:24px;border-radius:4px}
    /* Grid cards */
    .option-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:4px}
    .option-card{border:2px solid #e4e6eb;border-radius:12px;padding:16px 12px;text-align:center;cursor:pointer;transition:all .2s}
    .option-card:hover{border-color:#1877f2;background:#f8fbff}
    .option-card.selected{border-color:#1877f2;background:#e7f3ff}
    .option-card .oc-icon{font-size:26px;margin-bottom:6px}
    .option-card .oc-label{font-size:13px;font-weight:600}
    .option-card .oc-desc{font-size:11px;color:#65676b;margin-top:2px}
    /* Action picker — 3 cols */
    .action-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:4px}
    /* Posts grid */
    .posts-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px;max-height:280px;overflow-y:auto}
    .post-card{border:2px solid #e4e6eb;border-radius:10px;overflow:hidden;cursor:pointer;position:relative;transition:border-color .2s}
    .post-card:hover{border-color:#1877f2}
    .post-card.selected{border-color:#1877f2;box-shadow:0 0 0 3px rgba(24,119,242,.15)}
    .post-card img,.post-thumb-ph{width:100%;aspect-ratio:1;object-fit:cover;display:block}
    .post-thumb-ph{background:#e8f0fe;display:flex;align-items:center;justify-content:center;font-size:24px}
    .post-caption{font-size:11px;color:#65676b;padding:5px 7px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .post-check{position:absolute;top:5px;left:5px;background:#1877f2;color:#fff;border-radius:50%;width:20px;height:20px;display:none;align-items:center;justify-content:center;font-size:11px;font-weight:700}
    .post-card.selected .post-check{display:flex}
    /* Inputs */
    .input-group{margin-bottom:14px}
    .input-group label{display:block;font-size:13px;font-weight:600;margin-bottom:6px}
    .input-group input,.input-group textarea{width:100%;padding:10px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:14px;outline:none;font-family:inherit;resize:none}
    .input-group input:focus,.input-group textarea:focus{border-color:#1877f2}
    .section-divider{border:none;border-top:1px solid #e4e6eb;margin:16px 0}
    .section-label{font-size:12px;font-weight:700;color:#65676b;text-transform:uppercase;letter-spacing:.6px;margin-bottom:10px}
    .kw-tags{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
    .kw-tag{background:#e7f3ff;color:#1877f2;border-radius:20px;padding:4px 10px;font-size:12px;display:flex;align-items:center;gap:5px}
    .kw-tag button{background:none;border:none;cursor:pointer;color:#1877f2;font-size:14px;line-height:1}
    .modal-footer{padding:0 20px 20px;display:flex;justify-content:space-between;gap:10px}
    .btn-outline{background:#f0f2f5;color:#050505;padding:9px 18px;border:none;border-radius:8px;cursor:pointer;font-size:14px;font-weight:600}
    .loading{text-align:center;padding:24px;color:#65676b;font-size:13px}
    table{width:100%;border-collapse:collapse}
    th{text-align:left;font-size:12px;color:#65676b;padding:8px 10px;border-bottom:1px solid #e4e6eb;text-transform:uppercase;letter-spacing:.5px}
    td{padding:10px;font-size:13px;border-bottom:1px solid #f0f2f5;vertical-align:middle}
    tr:last-child td{border-bottom:none}
    .tag{background:#e7f3ff;color:#1877f2;border-radius:6px;padding:2px 7px;font-size:11px;font-family:monospace}
    .notice{background:#fff8e6;border:1px solid #ffe58f;border-radius:8px;padding:10px 14px;font-size:12px;color:#7c5e00;margin-bottom:14px}
    .platform-tabs{display:flex;gap:4px;background:#f0f2f5;border-radius:10px;padding:4px}
    .platform-tab{padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;color:#65676b;text-decoration:none;transition:all .2s}
    .platform-tab.active{background:#fff;color:#1877f2;box-shadow:0 1px 3px rgba(0,0,0,.08)}
    .platform-tab:hover:not(.active){color:#050505}
  </style>
</head>
<body>

<header>
  <div class="logo">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="#1877f2"><path d="M18 2h-3a5 5 0 00-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 011-1h3z"/></svg>
    AutoReply
  </div>
  <div style="display:flex;align-items:center;gap:16px">
    <!-- Removed platform tabs since Instagram is handled by SuperProfile -->
    <button class="btn btn-primary" onclick="openModal()">+ Create Automation</button>
  </div>
</header>

<div class="container">

  <div class="stats">
    <div class="stat-box">
      <div class="stat-num">{{ automations|length }}</div>
      <div class="stat-label">Total Automations</div>
    </div>
    <div class="stat-box">
      <div class="stat-num">{{ automations|selectattr('active')|list|length }}</div>
      <div class="stat-label">Active</div>
    </div>
    <div class="stat-box">
      <div class="stat-num">{{ keywords|length }}</div>
      <div class="stat-label">Global Keywords</div>
    </div>
  </div>

  <div class="card">
    <div class="card-header"><h2>Comment Automations</h2></div>
    {% for auto in automations %}
    <div class="auto-item">
      {% if auto.get('thumbnail') %}
      <img src="{{ auto['thumbnail'] }}" class="auto-thumb">
      {% else %}
      <div class="auto-thumb-ph">🎬</div>
      {% endif %}
      <div class="auto-info">
        <div class="auto-name">{{ auto['name'] }}</div>
        <div class="auto-meta">
          {% if auto.get('scope') == 'all' %}<span class="pill pill-blue">📢 All posts</span>
          {% else %}<span class="pill pill-blue">📌 {{ auto.get('post_ids',[])|length }} post(s)</span>{% endif %}

          {% if auto.get('keyword_type') == 'any' %}<span class="pill pill-green">💬 Any comment</span>
          {% else %}<span class="pill pill-green">🔑 {{ auto.get('keywords',[])|join(', ') }}</span>{% endif %}

          {% set action = auto.get('action','comment') %}
          {% if action == 'comment' %}<span class="pill pill-blue">💬 Comment Reply</span>
          {% elif action == 'dm' %}<span class="pill pill-purple">✉️ DM Only</span>
          {% else %}<span class="pill pill-blue">💬 Reply</span><span class="pill pill-purple">✉️ DM</span>{% endif %}
        </div>
      </div>
      <div class="auto-actions">
        <label class="toggle">
          <input type="checkbox" {{ 'checked' if auto.get('active', True) else '' }}
            onchange="toggleAuto({{ loop.index0 }}, this.checked)">
          <span class="slider"></span>
        </label>
        <button class="btn btn-edit" onclick='editAuto({{ loop.index0 }}, {{ auto|tojson }})'>Edit</button>
        <button class="btn btn-danger" onclick="deleteAuto({{ loop.index0 }})">Delete</button>
      </div>
    </div>
    {% else %}
    <div class="empty-state">
      <div class="icon">🤖</div>
      <p>No automations yet.<br>Click <strong>+ Create Automation</strong> to get started.</p>
    </div>
    {% endfor %}
  </div>

  <div class="card">
    <div class="card-header"><h2>Global Keywords</h2></div>
    <table>
      <thead><tr><th>Keyword</th><th>Reply</th><th></th></tr></thead>
      <tbody>
        {% for kw, reply in keywords.items() %}
        <tr>
          <td><span class="tag">{{ kw }}</span></td>
          <td>{{ reply }}</td>
          <td><button class="btn btn-danger" onclick="deleteKeyword('{{ kw }}')">Delete</button></td>
        </tr>
        {% else %}
        <tr><td colspan="3" style="text-align:center;color:#65676b;padding:20px;font-size:13px">No global keywords</td></tr>
        {% endfor %}
      </tbody>
    </table>
    <div style="display:flex;gap:10px;margin-top:14px;flex-wrap:wrap">
      <input type="text" id="g-kw" placeholder="Keyword" style="flex:1;min-width:120px;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:13px;outline:none">
      <input type="text" id="g-reply" placeholder="Reply message" style="flex:2;min-width:160px;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:13px;outline:none">
      <button class="btn btn-primary" onclick="addGlobalKeyword()">+ Add</button>
    </div>
  </div>

</div>

<!-- MODAL -->
<div class="overlay" id="modal-overlay">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Create Automation</h3>
      <button class="modal-close" onclick="closeModal()">×</button>
    </div>
    <div class="modal-body">

      <!-- Step dots -->
      <div class="step-dots">
        <div class="step-dot active" id="dot-1"></div>
        <div class="step-dot" id="dot-2"></div>
        <div class="step-dot" id="dot-3"></div>
        <div class="step-dot" id="dot-4"></div>
        <div class="step-dot" id="dot-5"></div>
      </div>

      <!-- Step 1: Scope -->
      <div class="step active" id="step-1">
        <div class="step-title">Which posts should trigger this?</div>
        <div class="step-sub">Choose whether to apply to all posts or specific ones.</div>
        <div class="option-grid">
          <div class="option-card" id="scope-all" onclick="selectScope('all')">
            <div class="oc-icon">📢</div>
            <div class="oc-label">All Posts</div>
            <div class="oc-desc">Apply to every post & reel</div>
          </div>
          <div class="option-card" id="scope-specific" onclick="selectScope('specific')">
            <div class="oc-icon">📌</div>
            <div class="oc-label">Specific Posts</div>
            <div class="oc-desc">Pick individual posts or reels</div>
          </div>
        </div>
      </div>

      <!-- Step 2: Pick posts -->
      <div class="step" id="step-2">
        <div class="step-title">Select Posts / Reels</div>
        <div class="step-sub">Tap to select the posts you want to automate.</div>
        <div id="posts-grid-modal" class="posts-grid"><div class="loading">Loading...</div></div>
        <div style="font-size:12px;color:#65676b" id="post-select-count"></div>
      </div>

      <!-- Step 3: Keywords -->
      <div class="step" id="step-3">
        <div class="step-title">When should it trigger?</div>
        <div class="step-sub">Choose what comment triggers the automation.</div>
        <div class="option-grid">
          <div class="option-card" id="kw-any" onclick="selectKwType('any')">
            <div class="oc-icon">💬</div>
            <div class="oc-label">Any Comment</div>
            <div class="oc-desc">Reply to every comment</div>
          </div>
          <div class="option-card" id="kw-specific" onclick="selectKwType('specific')">
            <div class="oc-icon">🔑</div>
            <div class="oc-label">Specific Keywords</div>
            <div class="oc-desc">Only matching words</div>
          </div>
        </div>
        <div id="kw-input-area" style="display:none;margin-top:14px">
          <div class="input-group" style="margin-bottom:8px">
            <label>Add Keywords</label>
            <div style="display:flex;gap:8px">
              <input type="text" id="kw-input" placeholder="e.g. price, grass, buy" onkeydown="if(event.key==='Enter')addKwTag()">
              <button class="btn btn-primary" style="flex-shrink:0;padding:9px 14px" onclick="addKwTag()">Add</button>
            </div>
          </div>
          <div class="kw-tags" id="kw-tags"></div>
        </div>
      </div>

      <!-- Step 4: Action type -->
      <div class="step" id="step-4">
        <div class="step-title">What action should happen?</div>
        <div class="step-sub">Choose how to respond when triggered.</div>
        <div class="action-grid">
          <div class="option-card" id="action-comment" onclick="selectAction('comment')">
            <div class="oc-icon">💬</div>
            <div class="oc-label">Comment Reply</div>
            <div class="oc-desc">Reply publicly on the comment</div>
          </div>
          <div class="option-card" id="action-dm" onclick="selectAction('dm')">
            <div class="oc-icon">✉️</div>
            <div class="oc-label">DM Only</div>
            <div class="oc-desc">Send a private message</div>
          </div>
          <div class="option-card" id="action-both" onclick="selectAction('both')">
            <div class="oc-icon">🔔</div>
            <div class="oc-label">Both</div>
            <div class="oc-desc">Comment reply + DM</div>
          </div>
        </div>
      </div>

      <!-- Step 5: Messages & Name -->
      <div class="step" id="step-5">
        <div class="step-title">Set your messages</div>
        <div class="step-sub">Configure what gets sent when triggered.</div>

        <div class="input-group">
          <label>Automation Name</label>
          <input type="text" id="auto-name" placeholder="e.g. Grass Reel Reply">
        </div>

        <div id="comment-reply-section">
          <hr class="section-divider">
          <div class="section-label">💬 Comment Reply</div>
          <div class="input-group">
            <label>Public Reply Message</label>
            <textarea id="auto-reply" rows="3" placeholder="e.g. Thanks for your comment! Contact us on 9895138430"></textarea>
          </div>
        </div>

        <div id="dm-section" style="display:none">
          <hr class="section-divider">
          <div class="section-label">✉️ DM Message</div>
          <div class="notice">⚠️ Facebook only allows DMs if the user has messaged your Page before (within 24h). This may not work for all commenters.</div>
          <div class="input-group">
            <label>Private DM Message</label>
            <textarea id="auto-dm" rows="3" placeholder="e.g. Hi! Thanks for your interest. Here are our details..."></textarea>
          </div>
        </div>

      </div>

    </div>
    <div class="modal-footer">
      <button class="btn-outline" id="btn-back" onclick="prevStep()" style="display:none">← Back</button>
      <button class="btn btn-primary" id="btn-next" onclick="nextStep()">Next →</button>
    </div>
  </div>
</div>

<script>
let currentStep = 1;
let totalSteps  = 5;
let selectedScope   = null;
let selectedPostIds = {};
let selectedKwType  = null;
let selectedAction  = null;
let keywords        = [];
let postsLoaded     = false;
let editingIdx      = -1;

function openModal(autoData, idx) {
  editingIdx = (idx !== undefined) ? idx : -1;
  currentStep = 1;
  selectedScope = null; selectedPostIds = {}; selectedKwType = null; selectedAction = null; keywords = [];

  document.querySelectorAll('.option-card').forEach(c => c.classList.remove('selected'));
  document.getElementById('kw-tags').innerHTML = '';
  document.getElementById('kw-input-area').style.display = 'none';
  document.getElementById('auto-name').value  = '';
  document.getElementById('auto-reply').value = '';
  document.getElementById('auto-dm').value    = '';

  if (autoData) {
    document.getElementById('modal-title').textContent = 'Edit Automation';
    selectedScope  = autoData.scope || 'all';
    selectedKwType = autoData.keyword_type || 'any';
    selectedAction = autoData.action || 'comment';
    keywords       = autoData.keywords || [];

    document.getElementById('scope-' + selectedScope).classList.add('selected');
    document.getElementById('kw-' + selectedKwType).classList.add('selected');
    document.getElementById('action-' + selectedAction).classList.add('selected');
    if (selectedKwType === 'specific') document.getElementById('kw-input-area').style.display = 'block';
    renderTags();
    updateMessageSections();

    document.getElementById('auto-name').value  = autoData.name || '';
    document.getElementById('auto-reply').value = autoData.reply || '';
    document.getElementById('auto-dm').value    = autoData.dm_message || '';

    if (autoData.post_ids) {
      autoData.post_ids.forEach(pid => {
        selectedPostIds[pid] = {id: pid, thumbnail: autoData.thumbnail || '', message: ''};
      });
    }
  } else {
    document.getElementById('modal-title').textContent = 'Create Automation';
  }

  showStep(1);
  document.getElementById('modal-overlay').classList.add('open');
}

function editAuto(idx, autoData) {
  postsLoaded = false;
  openModal(autoData, idx);
}

function closeModal() { document.getElementById('modal-overlay').classList.remove('open'); }

function showStep(n) {
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById('step-' + n).classList.add('active');
  document.getElementById('btn-back').style.display = n > 1 ? 'block' : 'none';
  document.getElementById('btn-next').textContent = n === totalSteps ? (editingIdx >= 0 ? '✓ Save Changes' : '✓ Save') : 'Next →';
  for (let i = 1; i <= totalSteps; i++) {
    const dot = document.getElementById('dot-' + i);
    dot.classList.toggle('active', i === n);
  }
  const titles = ['Choose Scope','Select Posts','Keyword Trigger','Action Type','Messages'];
  const prefix = editingIdx >= 0 ? 'Edit: ' : '';
  document.getElementById('modal-title').textContent = prefix + titles[n-1];
  if (n === 5) updateMessageSections();
}

function selectScope(s) {
  selectedScope = s;
  document.querySelectorAll('.option-card[id^="scope-"]').forEach(c => c.classList.remove('selected'));
  document.getElementById('scope-' + s).classList.add('selected');
}

function selectKwType(t) {
  selectedKwType = t;
  document.querySelectorAll('.option-card[id^="kw-"]').forEach(c => c.classList.remove('selected'));
  document.getElementById('kw-' + t).classList.add('selected');
  document.getElementById('kw-input-area').style.display = t === 'specific' ? 'block' : 'none';
}

function selectAction(a) {
  selectedAction = a;
  document.querySelectorAll('.option-card[id^="action-"]').forEach(c => c.classList.remove('selected'));
  document.getElementById('action-' + a).classList.add('selected');
}

function updateMessageSections() {
  const showComment = selectedAction === 'comment' || selectedAction === 'both';
  const showDm      = selectedAction === 'dm' || selectedAction === 'both';
  document.getElementById('comment-reply-section').style.display = showComment ? 'block' : 'none';
  document.getElementById('dm-section').style.display = showDm ? 'block' : 'none';
}

function addKwTag() {
  const val = document.getElementById('kw-input').value.trim();
  if (!val || keywords.includes(val)) return;
  keywords.push(val);
  document.getElementById('kw-input').value = '';
  renderTags();
}

function removeTag(i) { keywords.splice(i, 1); renderTags(); }

function renderTags() {
  document.getElementById('kw-tags').innerHTML = keywords.map((k, i) =>
    `<span class="kw-tag">${k}<button onclick="removeTag(${i})">×</button></span>`
  ).join('');
}

async function loadPostsGrid() {
  const grid = document.getElementById('posts-grid-modal');
  grid.innerHTML = '<div class="loading">Loading posts...</div>';
  try {
    const res  = await fetch('/ui/fetch-posts');
    const data = await res.json();
    postsLoaded = true;
    grid.innerHTML = '';
    if (data.error) {
      grid.innerHTML = `<div style="color:#ef4444;padding:20px;text-align:center;font-weight:600">Error: ${data.error}</div>`;
      return;
    }
    const posts = data.posts || [];
    if (posts.length === 0) {
      grid.innerHTML = '<div style="color:#65676b;padding:20px;text-align:center">No posts found on this Page.</div>';
      return;
    }
    posts.forEach(post => {
      const div = document.createElement('div');
      div.className = 'post-card' + (selectedPostIds[post.id] ? ' selected' : '');
      div.innerHTML = `
        <div class="post-check">✓</div>
        ${post.thumbnail ? `<img src="${post.thumbnail}" onerror="this.style.display='none'">` : `<div class="post-thumb-ph">📄</div>`}
        <div class="post-caption">${post.message.substring(0,40)}</div>`;
      div.onclick = () => {
        if (selectedPostIds[post.id]) { delete selectedPostIds[post.id]; div.classList.remove('selected'); }
        else { selectedPostIds[post.id] = post; div.classList.add('selected'); }
        const n = Object.keys(selectedPostIds).length;
        document.getElementById('post-select-count').textContent = n ? n + ' selected' : '';
      };
      grid.appendChild(div);
    });
    const n = Object.keys(selectedPostIds).length;
    document.getElementById('post-select-count').textContent = n ? n + ' selected' : '';
  } catch (err) {
    grid.innerHTML = `<div style="color:#ef4444;padding:20px;text-align:center;font-weight:600">Request failed: ${err.message}</div>`;
  }
}

async function nextStep() {
  if (currentStep === 1) {
    if (!selectedScope) return alert('Please choose a scope');
    if (selectedScope === 'specific') { if (!postsLoaded) await loadPostsGrid(); currentStep = 2; }
    else currentStep = 3;
  } else if (currentStep === 2) {
    if (Object.keys(selectedPostIds).length === 0) return alert('Please select at least one post');
    currentStep = 3;
  } else if (currentStep === 3) {
    if (!selectedKwType) return alert('Please choose a keyword trigger');
    if (selectedKwType === 'specific' && keywords.length === 0) return alert('Please add at least one keyword');
    currentStep = 4;
  } else if (currentStep === 4) {
    if (!selectedAction) return alert('Please choose an action');
    updateMessageSections();
    currentStep = 5;
  } else if (currentStep === 5) {
    const name  = document.getElementById('auto-name').value.trim();
    const reply = document.getElementById('auto-reply').value.trim();
    const dm    = document.getElementById('auto-dm').value.trim();
    if (!name) return alert('Please enter a name');
    if ((selectedAction === 'comment' || selectedAction === 'both') && !reply) return alert('Please enter a comment reply message');
    if ((selectedAction === 'dm' || selectedAction === 'both') && !dm) return alert('Please enter a DM message');
    const posts = Object.values(selectedPostIds);
    const payload = {
      name, reply, action: selectedAction, dm_message: dm,
      scope:        selectedScope,
      post_ids:     Object.keys(selectedPostIds),
      thumbnail:    posts.length > 0 ? (posts[0].thumbnail || '') : '',
      keyword_type: selectedKwType,
      keywords:     keywords,
      active:       true,
    };
    const url    = editingIdx >= 0 ? '/ui/automations/' + editingIdx : '/ui/automations';
    const method = editingIdx >= 0 ? 'PUT' : 'POST';
    await fetch(url, { method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
    closeModal();
    location.reload();
    return;
  }
  showStep(currentStep);
}

function prevStep() {
  if (currentStep === 3 && selectedScope === 'all') currentStep = 1;
  else currentStep--;
  showStep(currentStep);
}

async function toggleAuto(idx, active) {
  await fetch('/ui/automations/' + idx + '/toggle', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({active})
  });
}

async function deleteAuto(idx) {
  if (!confirm('Delete this automation?')) return;
  await fetch('/ui/automations/' + idx, {method: 'DELETE'});
  location.reload();
}

async function addGlobalKeyword() {
  const kw    = document.getElementById('g-kw').value.trim();
  const reply = document.getElementById('g-reply').value.trim();
  if (!kw || !reply) return alert('Enter both keyword and reply');
  await fetch('/ui/keywords', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({keyword: kw, reply}) });
  location.reload();
}

async function deleteKeyword(kw) {
  if (!confirm('Delete "' + kw + '"?')) return;
  await fetch('/ui/keywords/' + encodeURIComponent(kw), {method: 'DELETE'});
  location.reload();
}

document.getElementById('modal-overlay').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});
</script>
</body>
</html>
"""


INSTAGRAM_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Instagram AutoDM — AutoReply</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:linear-gradient(135deg,#fdf2f8 0%,#faf5ff 50%,#f0f2f5 100%);color:#1c1e21;min-height:100vh}
    header{background:#fff;border-bottom:1px solid #e4e6eb;padding:14px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
    .logo{display:flex;align-items:center;gap:10px;font-size:18px;font-weight:700;background:linear-gradient(45deg,#f09433,#e6683c,#dc2743,#cc2366,#bc1888);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
    .container{max-width:960px;margin:28px auto;padding:0 16px}
    .hero{background:linear-gradient(135deg,#833ab4,#fd1d1d,#fcb045);border-radius:16px;padding:24px 28px;color:#fff;margin-bottom:20px}
    .hero h1{font-size:22px;font-weight:700;margin-bottom:6px}
    .hero p{font-size:14px;opacity:.92;line-height:1.5}
    .feature-chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
    .chip{background:rgba(255,255,255,.2);border-radius:20px;padding:5px 12px;font-size:11px;font-weight:600}
    .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
    @media(max-width:700px){.stats{grid-template-columns:repeat(2,1fr)}}
    .stat-box{background:#fff;border-radius:12px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,0.08);text-align:center}
    .stat-num{font-size:24px;font-weight:700;background:linear-gradient(45deg,#833ab4,#fd1d1d);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
    .stat-label{font-size:11px;color:#65676b;margin-top:4px}
    .card{background:#fff;border-radius:14px;padding:22px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
    .card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;flex-wrap:wrap;gap:10px}
    .card-header h2{font-size:16px;font-weight:600}
    .auto-item{border:1px solid #fce7f3;border-radius:12px;padding:16px;margin-bottom:12px;display:flex;align-items:center;gap:14px}
    .auto-thumb{width:52px;height:52px;border-radius:8px;object-fit:cover;flex-shrink:0}
    .auto-thumb-ph{width:52px;height:52px;border-radius:8px;background:linear-gradient(135deg,#fdf2f8,#fae8ff);display:flex;align-items:center;justify-content:center;font-size:22px;flex-shrink:0}
    .auto-info{flex:1;min-width:0}
    .auto-name{font-weight:600;font-size:14px}
    .auto-meta{font-size:12px;color:#65676b;margin-top:4px;display:flex;flex-wrap:wrap;gap:6px}
    .auto-actions{display:flex;align-items:center;gap:8px;flex-shrink:0}
    .pill{display:inline-flex;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600}
    .pill-pink{background:#fdf2f8;color:#db2777}
    .pill-purple{background:#f3e8ff;color:#7c3aed}
    .pill-green{background:#f0fdf4;color:#16a34a}
    .pill-orange{background:#fff7ed;color:#ea580c}
    .pill-blue{background:#e7f3ff;color:#1877f2}
    .empty-state{text-align:center;padding:40px 20px;color:#65676b}
    .btn{padding:7px 14px;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600}
    .btn-primary{background:linear-gradient(45deg,#833ab4,#fd1d1d);color:#fff;padding:9px 18px;font-size:14px}
    .btn-danger{background:#ff4d4f;color:#fff}
    .btn-edit{background:#fdf2f8;color:#db2777}
    .toggle{position:relative;display:inline-block;width:40px;height:22px}
    .toggle input{opacity:0;width:0;height:0}
    .slider{position:absolute;cursor:pointer;inset:0;background:#ccd0d5;border-radius:22px;transition:.3s}
    .slider:before{position:absolute;content:"";height:16px;width:16px;left:3px;bottom:3px;background:white;border-radius:50%;transition:.3s}
    input:checked+.slider{background:linear-gradient(45deg,#833ab4,#fd1d1d)}
    input:checked+.slider:before{transform:translateX(18px)}
    .platform-tabs{display:flex;gap:4px;background:#f0f2f5;border-radius:10px;padding:4px}
    .platform-tab{padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;color:#65676b;text-decoration:none}
    .platform-tab.active{background:#fff;color:#db2777;box-shadow:0 1px 3px rgba(0,0,0,.08)}
    .overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:100;align-items:center;justify-content:center}
    .overlay.open{display:flex}
    .modal{background:#fff;border-radius:18px;width:94%;max-width:560px;max-height:92vh;overflow-y:auto}
    .modal-header{padding:20px 20px 0;display:flex;justify-content:space-between;align-items:center}
    .modal-header h3{font-size:18px;font-weight:700}
    .modal-close{background:none;border:none;font-size:24px;cursor:pointer;color:#65676b}
    .modal-body{padding:20px}
    .step{display:none}.step.active{display:block}
    .step-title{font-size:15px;font-weight:600;margin-bottom:5px}
    .step-sub{font-size:13px;color:#65676b;margin-bottom:16px}
    .step-dots{display:flex;justify-content:center;gap:8px;margin-bottom:20px}
    .step-dot{width:8px;height:8px;border-radius:50%;background:#e4e6eb;transition:.2s}
    .step-dot.active{background:linear-gradient(45deg,#833ab4,#fd1d1d);width:24px;border-radius:4px}
    .option-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .option-grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
    @media(max-width:500px){.option-grid-3{grid-template-columns:1fr 1fr}}
    .option-card{border:2px solid #e4e6eb;border-radius:12px;padding:14px 10px;text-align:center;cursor:pointer;transition:.2s}
    .option-card:hover,.option-card.selected{border-color:#db2777;background:#fdf2f8}
    .oc-icon{font-size:24px;margin-bottom:4px}
    .oc-label{font-size:12px;font-weight:600}
    .oc-desc{font-size:10px;color:#65676b;margin-top:2px}
    .posts-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;max-height:260px;overflow-y:auto;margin-bottom:10px}
    .post-card{border:2px solid #e4e6eb;border-radius:10px;overflow:hidden;cursor:pointer;position:relative}
    .post-card.selected{border-color:#db2777}
    .post-card img,.post-thumb-ph{width:100%;aspect-ratio:1;object-fit:cover;display:block}
    .post-thumb-ph{background:#fdf2f8;display:flex;align-items:center;justify-content:center;font-size:22px}
    .post-caption{font-size:10px;padding:4px 6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#65676b}
    .post-check{position:absolute;top:5px;left:5px;background:#db2777;color:#fff;border-radius:50%;width:18px;height:18px;display:none;align-items:center;justify-content:center;font-size:10px}
    .post-card.selected .post-check{display:flex}
    .input-group{margin-bottom:14px}
    .input-group label{display:block;font-size:13px;font-weight:600;margin-bottom:6px}
    .input-group input,.input-group textarea{width:100%;padding:10px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:14px;font-family:inherit;resize:none}
    .input-group input:focus,.input-group textarea:focus{border-color:#db2777;outline:none}
    .check-row{display:flex;align-items:flex-start;gap:10px;margin-bottom:12px;padding:12px;background:#fafafa;border-radius:10px}
    .check-row input{margin-top:3px}
    .check-row label{font-size:13px;font-weight:500;cursor:pointer}
    .check-row small{display:block;color:#65676b;font-size:11px;margin-top:2px}
    .kw-tags{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
    .kw-tag{background:#fdf2f8;color:#db2777;border-radius:20px;padding:4px 10px;font-size:12px;display:flex;gap:5px;align-items:center}
    .kw-tag button{background:none;border:none;cursor:pointer;color:#db2777}
    .modal-footer{padding:0 20px 20px;display:flex;justify-content:space-between}
    .btn-outline{background:#f0f2f5;border:none;border-radius:8px;padding:9px 18px;font-weight:600;cursor:pointer}
    .notice{background:#fdf2f8;border:1px solid #fbcfe8;border-radius:8px;padding:10px 14px;font-size:12px;color:#9d174d;margin-bottom:14px;line-height:1.5}
    table{width:100%;border-collapse:collapse}
    th{text-align:left;font-size:12px;color:#65676b;padding:8px;border-bottom:1px solid #e4e6eb}
    td{padding:10px;font-size:13px;border-bottom:1px solid #f0f2f5}
    .tag{background:#fdf2f8;color:#db2777;border-radius:6px;padding:2px 7px;font-size:11px;font-family:monospace}
    .section-label{font-size:11px;font-weight:700;color:#65676b;text-transform:uppercase;letter-spacing:.5px;margin:16px 0 10px}
  </style>
</head>
<body>
<header>
  <div class="logo">📷 Instagram AutoDM</div>
  <div style="display:flex;align-items:center;gap:16px">
    <!-- Removed platform tabs since Instagram is handled by SuperProfile -->
    <button class="btn btn-primary" onclick="openModal()">+ Create Automation</button>
  </div>
</header>

<div class="container">
  <div class="hero">
    <h1>Instagram Automation</h1>
    <p>AutoDM, comment replies, story & live automation — inspired by SuperProfile.bio. Uses your Meta Graph API token from .env.</p>
    <div class="feature-chips">
      <span class="chip">AutoDM</span><span class="chip">Comment Reply</span><span class="chip">Keyword Triggers</span>
      <span class="chip">Story Replies</span><span class="chip">Live Comments</span><span class="chip">DM Keywords</span>
      <span class="chip">@Mention</span><span class="chip">Link in DM</span><span class="chip">Email Capture</span>
      <span class="chip">Welcome DM</span><span class="chip">Daily Cap</span><span class="chip">Send Delay</span>
    </div>
  </div>

  <div class="stats">
    <div class="stat-box"><div class="stat-num">{{ stats.get('dms_sent',0) }}</div><div class="stat-label">AutoDMs Sent</div></div>
    <div class="stat-box"><div class="stat-num">{{ stats.get('comment_replies',0) }}</div><div class="stat-label">Comment Replies</div></div>
    <div class="stat-box"><div class="stat-num">{{ stats.get('story_replies',0) }}</div><div class="stat-label">Story Replies</div></div>
    <div class="stat-box"><div class="stat-num">{{ stats.get('live_replies',0) }}</div><div class="stat-label">Live Replies</div></div>
    <div class="stat-box"><div class="stat-num">{{ stats.get('mentions_handled',0) }}</div><div class="stat-label">Mentions Handled</div></div>
    <div class="stat-box"><div class="stat-num" id="dms-today">{{ stats.get('dms_today',0) }}</div><div class="stat-label">DMs Today</div></div>
    <div class="stat-box"><div class="stat-num">{{ automations|selectattr('active')|list|length }}</div><div class="stat-label">Active Rules</div></div>
  </div>

  <div class="card">
    <div class="card-header">
      <h2>Instagram Automations</h2>
      <button class="btn" onclick="resetStats()" style="background:#f0f2f5;color:#65676b;font-size:12px;padding:6px 12px">🔄 Reset Stats</button>
    </div>
    {% for auto in automations %}
    <div class="auto-item">
      {% if auto.get('thumbnail') %}<img src="{{ auto['thumbnail'] }}" class="auto-thumb">
      {% else %}<div class="auto-thumb-ph">📷</div>{% endif %}
      <div class="auto-info">
        <div class="auto-name">{{ auto['name'] }}</div>
        <div class="auto-meta">
          {% set tt = auto.get('trigger_type','comment') %}
          {% if tt == 'comment' %}<span class="pill pill-pink">💬 Post/Reel Comment</span>
          {% elif tt == 'story' %}<span class="pill pill-purple">📖 Story Reply</span>
          {% elif tt == 'live' %}<span class="pill pill-orange">🔴 Live Comment</span>
          {% elif tt == 'dm' %}<span class="pill pill-blue">✉️ DM Keyword</span>
          {% elif tt == 'mention' %}<span class="pill pill-orange">📣 @Mention</span>
          {% else %}<span class="pill pill-green">👋 Welcome DM</span>{% endif %}
          {% if auto.get('scope')=='all' %}<span class="pill pill-pink">All media</span>
          {% else %}<span class="pill pill-pink">{{ auto.get('post_ids',[])|length }} post(s)</span>{% endif %}
          {% if auto.get('keyword_type')=='any' %}<span class="pill pill-green">Any text</span>
          {% else %}<span class="pill pill-green">🔑 {{ auto.get('keywords',[])|join(', ') }}</span>{% endif %}
          {% if auto.get('ask_follow') %}<span class="pill pill-orange">Follow gate</span>{% endif %}
          {% if auto.get('email_capture') %}<span class="pill pill-blue">Email capture</span>{% endif %}
          {% if auto.get('link_url') %}<span class="pill pill-purple">Link</span>{% endif %}
        </div>
      </div>
      <div class="auto-actions">
        <label class="toggle"><input type="checkbox" {{ 'checked' if auto.get('active',True) else '' }} onchange="toggleAuto({{ loop.index0 }}, this.checked)"><span class="slider"></span></label>
        <button class="btn" onclick="testAuto({{ loop.index0 }})" style="background:#f0fdf4;color:#16a34a;font-size:12px">▶ Test</button>
        <button class="btn btn-edit" onclick='editAuto({{ loop.index0 }}, {{ auto|tojson }})'>Edit</button>
        <button class="btn btn-danger" onclick="deleteAuto({{ loop.index0 }})">Delete</button>
      </div>
    </div>
    {% else %}
    <div class="empty-state"><div style="font-size:40px;margin-bottom:10px">📷</div><p>No Instagram automations yet.<br>Create your first AutoDM rule.</p></div>
    {% endfor %}
  </div>

  <div class="card">
    <div class="card-header"><h2>Global DM Keywords</h2></div>
    <div class="notice">Fallback replies when no automation matches an incoming DM keyword.</div>
    <table><thead><tr><th>Keyword</th><th>Reply</th><th></th></tr></thead>
    <tbody>{% for kw, reply in keywords.items() %}
      <tr><td><span class="tag">{{ kw }}</span></td><td>{{ reply }}</td>
      <td><button class="btn btn-danger" onclick="deleteKeyword('{{ kw }}')">Delete</button></td></tr>
    {% else %}<tr><td colspan="3" style="text-align:center;color:#65676b;padding:20px">No keywords</td></tr>{% endfor %}
    </tbody></table>
    <div style="display:flex;gap:10px;margin-top:14px;flex-wrap:wrap">
      <input type="text" id="g-kw" placeholder="Keyword" style="flex:1;min-width:120px;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px">
      <input type="text" id="g-reply" placeholder="Auto-reply message" style="flex:2;min-width:160px;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px">
      <button class="btn btn-primary" onclick="addGlobalKeyword()">+ Add</button>
    </div>
  </div>

  <div class="card">
    <div class="card-header"><h2>⚙️ Settings</h2></div>
    <div style="max-width:420px">
      <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px">Daily DM Cap</label>
      <p style="font-size:12px;color:#65676b;margin-bottom:10px">Max DMs to send per day (auto-resets at midnight). Prevents Instagram spam flags.<br>Remaining today: <strong id="cap-remaining">loading...</strong></p>
      <div style="display:flex;gap:10px">
        <input type="number" id="daily-cap-input" min="1" max="1000" value="200" style="flex:1;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:14px;outline:none">
        <button class="btn btn-primary" onclick="saveDailyCap()">Save</button>
      </div>
    </div>
  </div>

</div>


<div class="overlay" id="modal-overlay">
  <div class="modal">
    <div class="modal-header"><h3 id="modal-title">Create Instagram Automation</h3><button class="modal-close" onclick="closeModal()">×</button></div>
    <div class="modal-body">
      <div class="step-dots"><div class="step-dot active" id="dot-1"></div><div class="step-dot" id="dot-2"></div><div class="step-dot" id="dot-3"></div><div class="step-dot" id="dot-4"></div><div class="step-dot" id="dot-5"></div><div class="step-dot" id="dot-6"></div></div>

      <div class="step active" id="step-1">
        <div class="step-title">What should trigger this?</div>
        <div class="step-sub">Choose the Instagram interaction type (SuperProfile-style triggers).</div>
        <div class="option-grid-3">
          <div class="option-card" id="trigger-comment" onclick="selectTrigger('comment')"><div class="oc-icon">💬</div><div class="oc-label">Post/Reel Comment</div><div class="oc-desc">AutoDM on comments</div></div>
          <div class="option-card" id="trigger-story" onclick="selectTrigger('story')"><div class="oc-icon">📖</div><div class="oc-label">Story Reply</div><div class="oc-desc">When someone replies to story</div></div>
          <div class="option-card" id="trigger-live" onclick="selectTrigger('live')"><div class="oc-icon">🔴</div><div class="oc-label">Live Comment</div><div class="oc-desc">During live streams</div></div>
          <div class="option-card" id="trigger-dm" onclick="selectTrigger('dm')"><div class="oc-icon">✉️</div><div class="oc-label">DM Keyword</div><div class="oc-desc">Incoming DM trigger</div></div>
          <div class="option-card" id="trigger-welcome" onclick="selectTrigger('welcome')"><div class="oc-icon">👋</div><div class="oc-label">Welcome DM</div><div class="oc-desc">First-time conversation only</div></div>
          <div class="option-card" id="trigger-mention" onclick="selectTrigger('mention')"><div class="oc-icon">📣</div><div class="oc-label">@Mention</div><div class="oc-desc">Tagged in post/story</div></div>
        </div>
      </div>

      <div class="step" id="step-2">
        <div class="step-title">Which posts/reels?</div>
        <div class="step-sub" id="scope-sub">Apply to all media or pick specific posts.</div>
        <div class="option-grid">
          <div class="option-card" id="scope-all" onclick="selectScope('all')"><div class="oc-icon">📢</div><div class="oc-label">All Posts & Reels</div></div>
          <div class="option-card" id="scope-specific" onclick="selectScope('specific')"><div class="oc-icon">📌</div><div class="oc-label">Specific Media</div></div>
        </div>
      </div>

      <div class="step" id="step-3">
        <div class="step-title">Select Instagram Media</div>
        <div id="posts-grid-modal" class="posts-grid"><div style="text-align:center;padding:20px;color:#65676b">Loading...</div></div>
        <div id="post-select-count" style="font-size:12px;color:#65676b"></div>
      </div>

      <div class="step" id="step-4">
        <div class="step-title">Keyword trigger</div>
        <div class="step-sub">Reply when comment/DM contains specific words, or any message.</div>
        <div class="option-grid">
          <div class="option-card" id="kw-any" onclick="selectKwType('any')"><div class="oc-icon">💬</div><div class="oc-label">Any Message</div></div>
          <div class="option-card" id="kw-specific" onclick="selectKwType('specific')"><div class="oc-icon">🔑</div><div class="oc-label">Specific Keywords</div></div>
        </div>
        <div id="kw-input-area" style="display:none;margin-top:14px">
          <div class="input-group"><label>Keywords (e.g. link, price, info)</label>
            <div style="display:flex;gap:8px"><input type="text" id="kw-input" placeholder="Add keyword" onkeydown="if(event.key==='Enter')addKwTag()"><button class="btn btn-primary" onclick="addKwTag()">Add</button></div>
          </div>
          <div class="kw-tags" id="kw-tags"></div>
        </div>
      </div>

      <div class="step" id="step-5">
        <div class="step-title">Action type</div>
        <div class="option-grid-3">
          <div class="option-card" id="action-comment" onclick="selectAction('comment')"><div class="oc-icon">💬</div><div class="oc-label">Comment Only</div></div>
          <div class="option-card" id="action-dm" onclick="selectAction('dm')"><div class="oc-icon">✉️</div><div class="oc-label">AutoDM Only</div></div>
          <div class="option-card" id="action-both" onclick="selectAction('both')"><div class="oc-icon">🔔</div><div class="oc-label">Both</div></div>
        </div>
      </div>

      <div class="step" id="step-6">
        <div class="step-title">Messages & advanced options</div>
        <div class="notice">Instagram allows one private reply per comment within 7 days. Use {username} for personalization.</div>
        <div class="input-group"><label>Automation Name</label><input type="text" id="auto-name" placeholder="e.g. Reel Link AutoDM"></div>

        <div id="comment-section">
          <div class="section-label">Public Comment Reply</div>
          <div class="input-group"><textarea id="auto-reply" rows="2" placeholder="Thanks @{username}! Check your DMs 📩"></textarea></div>
        </div>

        <div id="dm-section">
          <div class="section-label">AutoDM Message</div>
          <div class="input-group"><textarea id="auto-dm" rows="3" placeholder="Hi {username}! Here's the link you asked for..."></textarea></div>
          <div class="input-group"><label>Link URL (optional)</label><input type="url" id="auto-link" placeholder="https://superprofile.bio/yourlink"></div>
          <div class="input-group"><label>Follow-up DM (optional)</label><textarea id="auto-followup" rows="2" placeholder="Did you get a chance to check it out?"></textarea></div>
          <div class="input-group">
            <label>Send Delay <span style="color:#65676b;font-weight:400;font-size:11px">(seconds, 0 = instant — humanises the response timing)</span></label>
            <input type="number" id="auto-delay" min="0" max="30" value="0" style="max-width:110px;padding:9px 12px;border:1px solid #ccd0d5;border-radius:8px;font-size:14px;outline:none">
          </div>
        </div>

        <div class="section-label">SuperProfile-style extras</div>
        <div class="check-row"><input type="checkbox" id="ask-follow"><label for="ask-follow"><strong>Ask for Follow</strong><small>Send a follow prompt before the link/DM content</small></label></div>
        <div class="input-group" id="follow-prompt-wrap" style="display:none"><input type="text" id="follow-prompt" placeholder="Follow us to unlock the link!"></div>
        <div class="check-row"><input type="checkbox" id="email-capture"><label for="email-capture"><strong>Email Capture</strong><small>Ask for email in the DM flow</small></label></div>
        <div class="input-group" id="email-prompt-wrap" style="display:none"><input type="text" id="email-prompt" placeholder="What's your best email?"></div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn-outline" id="btn-back" onclick="prevStep()" style="display:none">← Back</button>
      <button class="btn btn-primary" id="btn-next" onclick="nextStep()">Next →</button>
    </div>
  </div>
</div>

<script>
let currentStep=1,totalSteps=6,selectedTrigger=null,selectedScope=null,selectedPostIds={},selectedKwType=null,selectedAction='both',keywords=[],postsLoaded=false,editingIdx=-1;

document.getElementById('ask-follow').onchange=e=>{document.getElementById('follow-prompt-wrap').style.display=e.target.checked?'block':'none'};
document.getElementById('email-capture').onchange=e=>{document.getElementById('email-prompt-wrap').style.display=e.target.checked?'block':'none'};

function openModal(d,idx){
  editingIdx=idx!==undefined?idx:-1; currentStep=1; selectedTrigger=null; selectedScope=null; selectedPostIds={}; selectedKwType=null; selectedAction='both'; keywords=[]; postsLoaded=false;
  document.querySelectorAll('.option-card').forEach(c=>c.classList.remove('selected'));
  ['auto-name','auto-reply','auto-dm','auto-link','auto-followup','follow-prompt','email-prompt'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('auto-delay').value='0';
  document.getElementById('ask-follow').checked=false; document.getElementById('email-capture').checked=false;
  document.getElementById('follow-prompt-wrap').style.display='none'; document.getElementById('email-prompt-wrap').style.display='none';
  document.getElementById('kw-tags').innerHTML=''; document.getElementById('kw-input-area').style.display='none';
  if(d){
    selectedTrigger=d.trigger_type||'comment'; selectedScope=d.scope||'all'; selectedKwType=d.keyword_type||'any'; selectedAction=d.action||'both'; keywords=d.keywords||[];
    document.getElementById('trigger-'+selectedTrigger).classList.add('selected');
    document.getElementById('scope-'+selectedScope).classList.add('selected');
    document.getElementById('kw-'+selectedKwType).classList.add('selected');
    document.getElementById('action-'+selectedAction).classList.add('selected');
    if(selectedKwType==='specific') document.getElementById('kw-input-area').style.display='block';
    renderTags();
    document.getElementById('auto-name').value=d.name||'';
    document.getElementById('auto-reply').value=d.reply||'';
    document.getElementById('auto-dm').value=d.dm_message||'';
    document.getElementById('auto-link').value=d.link_url||'';
    document.getElementById('auto-followup').value=d.follow_up_message||'';
    document.getElementById('auto-delay').value=d.delay_seconds||0;
    document.getElementById('ask-follow').checked=!!d.ask_follow;
    document.getElementById('follow-prompt').value=d.follow_prompt||'';
    document.getElementById('email-capture').checked=!!d.email_capture;
    document.getElementById('email-prompt').value=d.email_prompt||'';
    if(d.ask_follow) document.getElementById('follow-prompt-wrap').style.display='block';
    if(d.email_capture) document.getElementById('email-prompt-wrap').style.display='block';
    (d.post_ids||[]).forEach(pid=>{selectedPostIds[pid]={id:pid,thumbnail:d.thumbnail||''};});
  }
  updateSections(); showStep(1); document.getElementById('modal-overlay').classList.add('open');
}
function editAuto(i,d){postsLoaded=false;openModal(d,i);}
function closeModal(){document.getElementById('modal-overlay').classList.remove('open');}
function selectTrigger(t){selectedTrigger=t;document.querySelectorAll('[id^="trigger-"]').forEach(c=>c.classList.remove('selected'));document.getElementById('trigger-'+t).classList.add('selected');}
function selectScope(s){selectedScope=s;document.querySelectorAll('[id^="scope-"]').forEach(c=>c.classList.remove('selected'));document.getElementById('scope-'+s).classList.add('selected');}
function selectKwType(t){selectedKwType=t;document.querySelectorAll('[id^="kw-"]').forEach(c=>c.classList.remove('selected'));document.getElementById('kw-'+t).classList.add('selected');document.getElementById('kw-input-area').style.display=t==='specific'?'block':'none';}
function selectAction(a){selectedAction=a;document.querySelectorAll('[id^="action-"]').forEach(c=>c.classList.remove('selected'));document.getElementById('action-'+a).classList.add('selected');updateSections();}
function addKwTag(){const v=document.getElementById('kw-input').value.trim();if(v&&!keywords.includes(v)){keywords.push(v);document.getElementById('kw-input').value='';renderTags();}}
function removeTag(i){keywords.splice(i,1);renderTags();}
function renderTags(){document.getElementById('kw-tags').innerHTML=keywords.map((k,i)=>`<span class="kw-tag">${k}<button onclick="removeTag(${i})">×</button></span>`).join('');}
function updateSections(){
  const showComment=selectedAction==='comment'||selectedAction==='both';
  const showDm=selectedAction==='dm'||selectedAction==='both';
  const noCommentTrigger=['dm','welcome','mention'].includes(selectedTrigger);
  document.getElementById('comment-section').style.display=showComment&&!noCommentTrigger?'block':'none';
  document.getElementById('dm-section').style.display=showDm?'block':'none';
}
function showStep(n){
  document.querySelectorAll('.step').forEach(s=>s.classList.remove('active'));
  document.getElementById('step-'+n).classList.add('active');
  document.getElementById('btn-back').style.display=n>1?'block':'none';
  document.getElementById('btn-next').textContent=n===totalSteps?(editingIdx>=0?'✓ Save':'✓ Save'):'Next →';
  for(let i=1;i<=totalSteps;i++) document.getElementById('dot-'+i).classList.toggle('active',i===n);
  if(n===6) updateSections();
}
async function loadPostsGrid(){
  const grid=document.getElementById('posts-grid-modal'); grid.innerHTML='Loading...';
  try {
    const r=await fetch('/instagram/ui/fetch-media');
    const d=await r.json();
    postsLoaded=true;
    grid.innerHTML='';
    if (d.error) {
      grid.innerHTML = `<div style="color:#ef4444;padding:20px;text-align:center;font-weight:600;font-size:13px">Error: ${d.error}</div>`;
      return;
    }
    const posts = d.posts || [];
    if (posts.length === 0) {
      grid.innerHTML = '<div style="color:#65676b;padding:20px;text-align:center;font-size:13px">No Instagram media found.</div>';
      return;
    }
    posts.forEach(post=>{
      const div=document.createElement('div'); div.className='post-card'+(selectedPostIds[post.id]?' selected':'');
      div.innerHTML=`<div class="post-check">✓</div>${post.thumbnail?`<img src="${post.thumbnail}">`:`<div class="post-thumb-ph">${post.media_type==='video'?'🎬':'📷'}</div>`}<div class="post-caption">${post.message.substring(0,35)}</div>`;
      div.onclick=()=>{if(selectedPostIds[post.id]){delete selectedPostIds[post.id];div.classList.remove('selected');}else{selectedPostIds[post.id]=post;div.classList.add('selected');}document.getElementById('post-select-count').textContent=Object.keys(selectedPostIds).length+' selected';};
      grid.appendChild(div);
    });
  } catch (err) {
    grid.innerHTML = `<div style="color:#ef4444;padding:20px;text-align:center;font-weight:600;font-size:13px">Request failed: ${err.message}</div>`;
  }
}
async function nextStep(){
  const skipScope=['story','dm','welcome','mention'];
  if(currentStep===1){
    if(!selectedTrigger)return alert('Choose a trigger');
    if(skipScope.includes(selectedTrigger)){selectedScope='all';currentStep=4;}
    else currentStep=2;
  }
  else if(currentStep===2){
    if(!selectedScope)return alert('Choose scope');
    if(['comment','live'].includes(selectedTrigger)&&selectedScope==='specific'){if(!postsLoaded)await loadPostsGrid();currentStep=3;}
    else currentStep=4;
  } else if(currentStep===3){if(!Object.keys(selectedPostIds).length)return alert('Select at least one post');currentStep=4;}
  else if(currentStep===4){if(!selectedKwType)return alert('Choose keyword type');if(selectedKwType==='specific'&&!keywords.length)return alert('Add keywords');currentStep=5;}
  else if(currentStep===5){if(!selectedAction)return alert('Choose action');currentStep=6;}
  else{
    const name=document.getElementById('auto-name').value.trim();
    const reply=document.getElementById('auto-reply').value.trim();
    const dm=document.getElementById('auto-dm').value.trim();
    if(!name)return alert('Enter a name');
    const noComment=['dm','welcome','mention'].includes(selectedTrigger);
    if((selectedAction==='comment'||selectedAction==='both')&&!noComment&&!reply)return alert('Enter comment reply');
    if((selectedAction==='dm'||selectedAction==='both')&&!dm)return alert('Enter AutoDM message');
    const posts=Object.values(selectedPostIds);
    const payload={
      name,reply,action:selectedAction,dm_message:dm,trigger_type:selectedTrigger,
      scope:['comment','live'].includes(selectedTrigger)?selectedScope:'all',
      post_ids:Object.keys(selectedPostIds),thumbnail:posts.length?posts[0].thumbnail||'':'',
      keyword_type:selectedKwType,keywords,active:true,
      delay_seconds:parseInt(document.getElementById('auto-delay').value)||0,
      link_url:document.getElementById('auto-link').value.trim(),
      follow_up_message:document.getElementById('auto-followup').value.trim(),
      ask_follow:document.getElementById('ask-follow').checked,
      follow_prompt:document.getElementById('follow-prompt').value.trim(),
      email_capture:document.getElementById('email-capture').checked,
      email_prompt:document.getElementById('email-prompt').value.trim(),
    };
    const url=editingIdx>=0?'/instagram/ui/automations/'+editingIdx:'/instagram/ui/automations';
    await fetch(url,{method:editingIdx>=0?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    closeModal(); location.reload(); return;
  }
  showStep(currentStep);
}
function prevStep(){
  const skipScope=['story','dm','welcome','mention'];
  if(currentStep===4&&skipScope.includes(selectedTrigger))currentStep=1;
  else if(currentStep===4&&(!['comment','live'].includes(selectedTrigger)||selectedScope==='all'))currentStep=2;
  else if(currentStep===4&&selectedScope==='specific')currentStep=3;
  else currentStep--;
  showStep(currentStep);
}
async function toggleAuto(i,a){await fetch('/instagram/ui/automations/'+i+'/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:a})});}
async function deleteAuto(i){if(confirm('Delete?')){await fetch('/instagram/ui/automations/'+i,{method:'DELETE'});location.reload();}}
async function testAuto(idx){
  const r=await fetch('/instagram/ui/automations/'+idx+'/test',{method:'POST'});
  const d=await r.json();
  alert(d.ok?'\u2705 '+d.message:'\u274c '+(d.error||'Test failed'));
}
async function resetStats(){
  if(!confirm('Reset all stats to zero?'))return;
  await fetch('/instagram/ui/stats/reset',{method:'POST'});
  location.reload();
}
async function saveDailyCap(){
  const cap=parseInt(document.getElementById('daily-cap-input').value)||200;
  await fetch('/instagram/ui/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({daily_dm_cap:cap})});
  alert('\u2705 Daily cap saved to '+cap+' DMs/day');
}
async function addGlobalKeyword(){const kw=document.getElementById('g-kw').value.trim(),r=document.getElementById('g-reply').value.trim();if(!kw||!r)return alert('Enter keyword and reply');await fetch('/instagram/ui/keywords',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keyword:kw,reply:r})});location.reload();}
async function deleteKeyword(kw){if(confirm('Delete?')){await fetch('/instagram/ui/keywords/'+encodeURIComponent(kw),{method:'DELETE'});location.reload();}}
document.getElementById('modal-overlay').onclick=e=>{if(e.target===document.getElementById('modal-overlay'))closeModal();};
// Load settings on page load
(async()=>{
  try{
    const r=await fetch('/instagram/ui/settings');const d=await r.json();
    if(document.getElementById('daily-cap-input'))document.getElementById('daily-cap-input').value=d.daily_dm_cap||200;
    const remaining=(d.daily_dm_cap||200)-(d.dms_today||0);
    if(document.getElementById('cap-remaining'))document.getElementById('cap-remaining').textContent=remaining+' remaining today';
  }catch(e){}
})();
</script>
</body>
</html>
"""


@app.route("/debug-token")
def debug_token():
    return jsonify({
        "page_id": PAGE_ID,
        "token_prefix": PAGE_ACCESS_TOKEN[:15] if PAGE_ACCESS_TOKEN else "None",
        "fb_automations": load_automations(),
        "ig_automations": load_ig_automations(),
    })


@app.route("/")
def dashboard():
    # Prefix all API endpoints and local dashboard tab links for reverse proxy /fb support
    prefixed_html = (
        HTML
        .replace("'/ui/", "'/fb/ui/")
        .replace('"/ui/', '"/fb/ui/')
        .replace("'/instagram", "'/fb/instagram")
        .replace('"/instagram', '"/fb/instagram')
        .replace('href="/"', 'href="/fb/"')
        .replace('href="/instagram"', 'href="/fb/instagram"')
    )
    return render_template_string(prefixed_html, keywords=load_keywords(), automations=load_automations())

@app.route("/ui/fetch-posts")
def fetch_posts_api():
    force = request.args.get("refresh") == "1"
    try:
        posts = fetch_page_posts(force=force)
        try:
            subscribe_page()
            subscribe_instagram()
        except Exception as sub_err:
            print(f"[Webhook Auto-Subscribe] FAILED: {sub_err}")
        return jsonify({"posts": posts})
    except Exception as e:
        return jsonify({"error": str(e), "posts": []})

@app.route("/ui/automations", methods=["POST"])
def add_automation():
    data  = request.json
    autos = load_automations()
    autos.append(data)
    save_automations(autos)
    return jsonify({"ok": True})

@app.route("/ui/automations/<int:idx>", methods=["PUT"])
def edit_automation(idx):
    data  = request.json
    autos = load_automations()
    if 0 <= idx < len(autos):
        data["active"] = autos[idx].get("active", True)
        autos[idx] = data
        save_automations(autos)
    return jsonify({"ok": True})

@app.route("/ui/automations/<int:idx>", methods=["DELETE"])
def delete_automation(idx):
    autos = load_automations()
    if 0 <= idx < len(autos):
        autos.pop(idx)
        save_automations(autos)
    return jsonify({"ok": True})

@app.route("/ui/automations/<int:idx>/toggle", methods=["POST"])
def toggle_automation(idx):
    data  = request.json
    autos = load_automations()
    if 0 <= idx < len(autos):
        autos[idx]["active"] = data["active"]
        save_automations(autos)
    return jsonify({"ok": True})

@app.route("/ui/keywords", methods=["POST"])
def add_keyword():
    data = request.json
    kws  = load_keywords()
    kws[data["keyword"]] = data["reply"]
    save_keywords(kws)
    return jsonify({"ok": True})

@app.route("/ui/keywords/<keyword>", methods=["DELETE"])
def delete_keyword(keyword):
    kws = load_keywords()
    kws.pop(keyword, None)
    save_keywords(kws)
    return jsonify({"ok": True})


# ── Instagram dashboard (separate page & data) ───────────────────────────────

@app.route("/instagram")
def instagram_dashboard():
    # Prefix all API endpoints and local dashboard tab links for reverse proxy /fb support
    prefixed_html = (
        INSTAGRAM_HTML
        .replace("'/ui/", "'/fb/ui/")
        .replace('"/ui/', '"/fb/ui/')
        .replace("'/instagram", "'/fb/instagram")
        .replace('"/instagram', '"/fb/instagram')
        .replace('href="/"', 'href="/fb/"')
        .replace('href="/instagram"', 'href="/fb/instagram"')
    )
    return render_template_string(
        prefixed_html,
        keywords=load_ig_keywords(),
        automations=load_ig_automations(),
        stats=load_ig_stats(),
    )

@app.route("/instagram/ui/fetch-media")
def ig_fetch_media_api():
    force = request.args.get("refresh") == "1"
    try:
        media = fetch_ig_media(force=force)
        try:
            subscribe_page()
            subscribe_instagram()
        except Exception as sub_err:
            print(f"[Webhook Auto-Subscribe] FAILED: {sub_err}")
        return jsonify({"posts": media})
    except Exception as e:
        return jsonify({"error": str(e), "posts": []})

@app.route("/instagram/ui/automations", methods=["POST"])
def ig_add_automation():
    autos = load_ig_automations()
    autos.append(request.json)
    save_ig_automations(autos)
    return jsonify({"ok": True})

@app.route("/instagram/ui/automations/<int:idx>", methods=["PUT"])
def ig_edit_automation(idx):
    autos = load_ig_automations()
    if 0 <= idx < len(autos):
        data = request.json
        data["active"] = autos[idx].get("active", True)
        autos[idx] = data
        save_ig_automations(autos)
    return jsonify({"ok": True})

@app.route("/instagram/ui/automations/<int:idx>", methods=["DELETE"])
def ig_delete_automation(idx):
    autos = load_ig_automations()
    if 0 <= idx < len(autos):
        autos.pop(idx)
        save_ig_automations(autos)
    return jsonify({"ok": True})

@app.route("/instagram/ui/automations/<int:idx>/toggle", methods=["POST"])
def ig_toggle_automation(idx):
    autos = load_ig_automations()
    if 0 <= idx < len(autos):
        autos[idx]["active"] = request.json["active"]
        save_ig_automations(autos)
    return jsonify({"ok": True})

@app.route("/instagram/ui/keywords", methods=["POST"])
def ig_add_keyword():
    kws = load_ig_keywords()
    data = request.json
    kws[data["keyword"]] = data["reply"]
    save_ig_keywords(kws)
    return jsonify({"ok": True})

@app.route("/instagram/ui/keywords/<keyword>", methods=["DELETE"])
def ig_delete_keyword(keyword):
    kws = load_ig_keywords()
    kws.pop(keyword, None)
    save_ig_keywords(kws)
    return jsonify({"ok": True})


@app.route("/instagram/ui/automations/<int:idx>/test", methods=["POST"])
def ig_test_automation(idx):
    autos = load_ig_automations()
    if not (0 <= idx < len(autos)):
        return jsonify({"ok": False, "error": "Automation not found"})
    if not IG_USER_ID:
        return jsonify({"ok": False, "error": "IG_USER_ID not discovered yet"})
    
    tester = load_last_tester()
    if not tester or not tester.get("user_id"):
        return jsonify({
            "ok": False, 
            "error": "No test user found! Please send a comment, mention, or DM to your Instagram Business account from your personal Instagram account first to register your test ID, then click Test again."
        })
        
    target_id = tester["user_id"]
    target_name = tester.get("username") or "you"
    auto    = autos[idx]
    
    # Check if the action is a flow
    if auto.get("action") == "flow" and auto.get("link_url"):
        success = start_ig_flow(target_id, auto["link_url"], target_name)
        if success:
            return jsonify({"ok": True, "message": f"Test Flow '{auto['link_url']}' started for @{target_name}"})
        return jsonify({"ok": False, "error": f"Flow '{auto['link_url']}' could not be started."})
        
    dm_body = build_ig_dm_body(auto, target_name)
    if not dm_body:
        dm_body = auto.get("reply", "Test automation message")
        
    prepend_str = "[TEST]"
    if auto.get("trigger_type") == "story_mention":
        prepend_str = "[TEST STORY MENTION]"
        
    resp = requests.post(
        f"{GRAPH_URL}/{IG_USER_ID}/messages",
        params={"access_token": PAGE_ACCESS_TOKEN},
        json={"recipient": {"id": target_id}, "message": {"text": f"{prepend_str} — {auto['name']}\n\n{dm_body}"}},
        timeout=8,
    )
    result = resp.json()
    if "message_id" in result:
        return jsonify({"ok": True, "message": f"Test DM sent to @{target_name} for '{auto['name']}'"})
    return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})

@app.route("/instagram/ui/stats")
def ig_get_stats():
    local_stats = load_ig_stats()
    
    live_insights = {
        "reach_day": 0,
        "impressions_day": 0,
        "follower_count": 0,
        "reach_week": 0,
        "impressions_week": 0
    }
    
    if IG_USER_ID:
        try:
            resp = requests.get(
                f"{GRAPH_URL}/{IG_USER_ID}/insights",
                params={
                    "metric": "reach,impressions",
                    "period": "day",
                    "access_token": PAGE_ACCESS_TOKEN
                },
                timeout=8
            )
            insights_data = resp.json()
            if "data" in insights_data:
                for metric in insights_data["data"]:
                    name = metric.get("name")
                    values = metric.get("values", [])
                    if values:
                        val = values[-1].get("value", 0)
                        if name == "reach":
                            live_insights["reach_day"] = val
                        elif name == "impressions":
                            live_insights["impressions_day"] = val
                            
            profile_resp = requests.get(
                f"{GRAPH_URL}/{IG_USER_ID}",
                params={
                    "fields": "followers_count",
                    "access_token": PAGE_ACCESS_TOKEN
                },
                timeout=8
            )
            profile_data = profile_resp.json()
            if "followers_count" in profile_data:
                live_insights["follower_count"] = profile_data["followers_count"]
                
        except Exception as e:
            print(f"[Insights Fetch Error] Failed: {e}", flush=True)
            
    local_stats.update(live_insights)
    
    logs = load_ig_messages_log()
    local_stats["total_queued"] = len(load_ig_messages_queue())
    local_stats["total_logged"] = len(logs)
    local_stats["total_success_dms"] = sum(1 for log in logs if log.get("status") == "success")
    local_stats["total_failed_dms"] = sum(1 for log in logs if "failed" in str(log.get("status", "")))
    local_stats["total_skipped_dms"] = sum(1 for log in logs if log.get("status") in ("skipped_24h_limit", "outside_messaging_window"))
    
    # 3. Dynamic SQLite Analytics
    automation_stats = []
    try:
        rows = db_execute("""
            SELECT automation_name, 
                   COUNT(*) as total_triggered,
                   SUM(CASE WHEN status='success' THEN 1 ELSE 0 end) as sent_count,
                   SUM(CASE WHEN status='outside_messaging_window' OR status='outside_manual_7day_window' THEN 1 ELSE 0 end) as blocked_count
            FROM ig_messages_log 
            GROUP BY automation_name
        """)
        
        # Load leads count per automation
        lead_rows = db_execute("SELECT automation_name, COUNT(*) as lead_count FROM ig_leads GROUP BY automation_name")
        leads_map = {r["automation_name"]: r["lead_count"] for r in lead_rows}
        
        for r in rows:
            name = r["automation_name"] or "manual"
            leads = leads_map.get(name, 0)
            sent = r["sent_count"] or 0
            automation_stats.append({
                "automation_name": name,
                "triggered": r["total_triggered"] or 0,
                "sent": sent,
                "blocked": r["blocked_count"] or 0,
                "leads": leads,
                "conversion_rate": round((leads / sent * 100.0), 1) if sent > 0 else 0.0
            })
    except Exception as stat_err:
        print(f"[SQLite Stats Error] {stat_err}")

    local_stats["automation_stats"] = automation_stats

    # Lead conversion funnel summary
    try:
        total_leads = db_execute("SELECT COUNT(*) as cnt FROM ig_leads")[0]["cnt"]
        total_dms = db_execute("SELECT COUNT(*) as cnt FROM ig_messages_log WHERE status='success'")[0]["cnt"]
        local_stats["funnel"] = {
            "total_dms_sent": total_dms,
            "total_leads_captured": total_leads,
            "conversion_funnel_percentage": round((total_leads / total_dms * 100.0), 1) if total_dms > 0 else 0.0
        }
    except Exception as funnel_err:
        print(f"[Funnel Calculation Error] {funnel_err}")
        local_stats["funnel"] = {"total_dms_sent": 0, "total_leads_captured": 0, "conversion_funnel_percentage": 0.0}

    # Trend chart (last 7 days of successful sends)
    trend_data = []
    try:
        trend_rows = db_execute("""
            SELECT strftime('%Y-%m-%d', datetime(sent_at, 'unixepoch')) as day, COUNT(*) as count 
            FROM ig_messages_log 
            WHERE status='success' 
            GROUP BY day 
            ORDER BY day DESC 
            LIMIT 7
        """)
        for tr in trend_rows:
            trend_data.append({
                "date": tr["day"],
                "count": tr["count"]
            })
    except Exception as trend_err:
        print(f"[SQLite Trend Error] {trend_err}")
    local_stats["trend_last_7_days"] = trend_data

    # Calculate top posts by engagement
    top_posts = []
    try:
        media_list = fetch_ig_media()
        for media in media_list[:8]:
            try:
                m_resp = requests.get(
                    f"{GRAPH_URL}/{media['id']}",
                    params={
                        "fields": "comments_count,like_count",
                        "access_token": PAGE_ACCESS_TOKEN
                    },
                    timeout=8
                )
                m_data = m_resp.json()
                likes = m_data.get("like_count", 0)
                comments = m_data.get("comments_count", 0)
                engagement = likes + comments
                top_posts.append({
                    "id": media["id"],
                    "caption": media["message"],
                    "thumbnail": media["thumbnail"],
                    "likes": likes,
                    "comments": comments,
                    "engagement": engagement
                })
            except Exception:
                pass
        top_posts.sort(key=lambda x: x["engagement"], reverse=True)
    except Exception as e:
        print(f"[Top Posts Fetch Error] {e}", flush=True)
        
    local_stats["top_posts"] = top_posts[:5]
    
    return jsonify(local_stats)

@app.route("/instagram/ui/stats/reset", methods=["POST"])
def ig_reset_stats():
    save_ig_stats({"comment_replies": 0, "dms_sent": 0, "story_replies": 0,
                   "live_replies": 0, "dm_triggers": 0, "mentions_handled": 0,
                   "dms_today": 0, "dms_today_date": ""})
    return jsonify({"ok": True})

@app.route("/instagram/ui/settings", methods=["GET", "POST"])
def ig_settings_route():
    if request.method == "POST":
        data     = request.json
        settings = load_ig_settings()
        settings["daily_dm_cap"] = max(1, int(data.get("daily_dm_cap", 200)))
        settings["spam_keywords"] = data.get("spam_keywords", "")
        settings["crm_webhook_url"] = data.get("crm_webhook_url", "")
        save_ig_settings(settings)
        return jsonify({"ok": True})
    settings = load_ig_settings()
    stats    = load_ig_stats()
    settings["dms_today"]       = stats.get("dms_today", 0)
    settings["dms_today_date"]  = stats.get("dms_today_date", "")
    settings["spam_keywords"]   = settings.get("spam_keywords", "")
    settings["crm_webhook_url"] = settings.get("crm_webhook_url", "")
    return jsonify(settings)


# ── Flows endpoints ──
@app.route("/instagram/ui/flows", methods=["GET"])
def get_flows():
    return jsonify(load_ig_flows())

@app.route("/instagram/ui/flows", methods=["POST"])
def create_flow():
    data = request.json or {}
    flow_key = data.get("flow_key")
    steps = data.get("steps")
    if not flow_key or not steps:
        return jsonify({"success": False, "error": "flow_key and steps are required"}), 400
    
    flows = load_ig_flows()
    flows[flow_key] = steps
    save_ig_flows(flows)
    return jsonify({"success": True})

@app.route("/instagram/ui/flows/<flow_key>", methods=["DELETE"])
def delete_flow(flow_key):
    flows = load_ig_flows()
    if flow_key in flows:
        del flows[flow_key]
        save_ig_flows(flows)
        return jsonify({"success": True})
    return jsonify({"success": False, "error": "Flow not found"}), 404

# ── Broadcast route ──
@app.route("/instagram/ui/broadcast", methods=["POST"])
def instagram_broadcast():
    data = request.json or {}
    text = data.get("text")
    if not text:
        return jsonify({"success": False, "error": "Message text is required"}), 400
    
    twenty_four_hours_ago = time.time() - 86400
    rows = db_execute("SELECT user_id FROM ig_user_interactions WHERE last_interaction >= ?", (twenty_four_hours_ago,))
    
    count = 0
    for r in rows:
        uid = r["user_id"]
        queue_ig_message(recipient_id=uid, text=text, automation_name="broadcast")
        count += 1
        
    return jsonify({"success": True, "queued_count": count})


# ── Comment Moderation endpoints ──
@app.route("/instagram/ui/comments", methods=["GET"])
def ig_get_comments():
    try:
        media_list = fetch_ig_media()
        all_comments = []
        for media in media_list[:10]:
            media_id = media["id"]
            thumbnail = media["thumbnail"]
            try:
                resp = requests.get(
                    f"{GRAPH_URL}/{media_id}/comments",
                    params={
                        "fields": "id,text,timestamp,username,from,hidden",
                        "access_token": PAGE_ACCESS_TOKEN
                    },
                    timeout=8
                )
                data = resp.json()
                if "data" in data:
                    for comment in data["data"]:
                        all_comments.append({
                            "id": comment["id"],
                            "text": comment.get("text", ""),
                            "username": comment.get("username") or comment.get("from", {}).get("username", "anonymous"),
                            "timestamp": comment.get("timestamp", ""),
                            "hidden": comment.get("hidden", False),
                            "media_id": media_id,
                            "media_thumbnail": thumbnail
                        })
            except Exception as e:
                print(f"[Fetch Comments Error] Failed for media {media_id}: {e}", flush=True)
                
        all_comments.sort(key=lambda c: c.get("timestamp", ""), reverse=True)
        return jsonify({"comments": all_comments})
    except Exception as e:
        return jsonify({"error": str(e), "comments": []})

@app.route("/instagram/ui/comments/<comment_id>/reply", methods=["POST"])
def ig_reply_comment_manual(comment_id):
    try:
        message = request.json.get("message")
        if not message:
            return jsonify({"ok": False, "error": "Message is empty"})
        resp = requests.post(
            f"{GRAPH_URL}/{comment_id}/replies",
            data={"message": message, "access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        result = resp.json()
        if "id" in result:
            bump_ig_stat("comment_replies")
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/instagram/ui/comments/<comment_id>/hide", methods=["POST"])
def ig_hide_comment(comment_id):
    try:
        resp = requests.post(
            f"{GRAPH_URL}/{comment_id}",
            data={"hide": True, "access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        result = resp.json()
        if result.get("success"):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/instagram/ui/comments/<comment_id>/unhide", methods=["POST"])
def ig_unhide_comment(comment_id):
    try:
        resp = requests.post(
            f"{GRAPH_URL}/{comment_id}",
            data={"hide": False, "access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        result = resp.json()
        if result.get("success"):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/instagram/ui/comments/<comment_id>", methods=["DELETE"])
def ig_delete_comment(comment_id):
    try:
        resp = requests.delete(
            f"{GRAPH_URL}/{comment_id}",
            params={"access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        result = resp.json()
        if result.get("success"):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Link-in-Bio endpoints ──
@app.route("/instagram/ui/link-page", methods=["GET"])
def ig_get_link_page():
    pages = load_ig_link_pages()
    username = load_ig_settings().get("username") or "instagram"
    config = pages.get(username, {
        "title": f"@{username}",
        "bio": "Welcome to my profile!",
        "btn_color": "#db2777",
        "btn_text_color": "#ffffff",
        "links": [],
        "show_ig_feed": True
    })
    return jsonify(config)

@app.route("/instagram/ui/link-page", methods=["POST"])
def ig_save_link_page():
    try:
        data = request.json
        pages = load_ig_link_pages()
        username = load_ig_settings().get("username") or "instagram"
        pages[username] = {
            "title": data.get("title", f"@{username}"),
            "bio": data.get("bio", ""),
            "btn_color": data.get("btn_color", "#db2777"),
            "btn_text_color": data.get("btn_text_color", "#ffffff"),
            "links": data.get("links", []),
            "show_ig_feed": bool(data.get("show_ig_feed", True)),
            "avatar_url": data.get("avatar_url", "")
        }
        save_ig_link_pages(pages)
        
        # Save username to settings for quick lookup
        settings = load_ig_settings()
        settings["username"] = username
        save_ig_settings(settings)
        
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/ig/<username>")
def ig_public_bio_page(username):
    pages = load_ig_link_pages()
    config = pages.get(username)
    if not config:
        config = {
            "title": f"@{username}",
            "bio": "Check out my links below!",
            "btn_color": "#db2777",
            "btn_text_color": "#ffffff",
            "links": [],
            "show_ig_feed": True
        }
    
    media_items = []
    if config.get("show_ig_feed", True):
        try:
            # Reuses the Graph API media fetch
            media_items = fetch_ig_media()
        except Exception as e:
            print(f"[Bio Page Media Fetch Error] {e}", flush=True)
            
    bio_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ title }}</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-base: #060913;
                --bg-glass: rgba(15, 23, 42, 0.65);
                --border-glass: rgba(255, 255, 255, 0.08);
                --text-primary: #f3f4f6;
                --text-secondary: #9ca3af;
                --btn-bg: {{ btn_color }};
                --btn-color: {{ btn_text_color }};
            }
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Outfit', sans-serif; }
            body {
                background-color: var(--bg-base);
                background-image: radial-gradient(at 0% 0%, rgba(219,39,119,0.15) 0px, transparent 50%), radial-gradient(at 100% 100%, rgba(124,58,237,0.12) 0px, transparent 50%);
                color: var(--text-primary);
                min-height: 100vh;
                display: flex;
                flex-direction: column;
                align-items: center;
                padding: 40px 20px;
            }
            .bio-container {
                width: 100%;
                max-width: 580px;
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 24px;
            }
            .avatar {
                width: 96px;
                height: 96px;
                border-radius: 50%;
                background: linear-gradient(45deg, #f09433, #e6683c, #dc2743, #cc2366, #bc1888);
                padding: 4px;
                box-shadow: 0 8px 24px rgba(0,0,0,0.5);
            }
            .avatar-inner {
                width: 100%;
                height: 100%;
                border-radius: 50%;
                background: var(--bg-base);
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 32px;
                overflow: hidden;
            }
            .avatar-inner img {
                width: 100%;
                height: 100%;
                object-fit: cover;
            }
            .header-info {
                text-align: center;
            }
            .header-info h1 {
                font-size: 22px;
                font-weight: 700;
                margin-bottom: 6px;
                letter-spacing: -0.02em;
            }
            .header-info p {
                font-size: 14px;
                color: var(--text-secondary);
                max-width: 400px;
                line-height: 1.5;
            }
            .links-list {
                width: 100%;
                display: flex;
                flex-direction: column;
                gap: 14px;
            }
            .bio-link {
                display: block;
                width: 100%;
                background: var(--btn-bg);
                color: var(--btn-color);
                border: 1px solid var(--border-glass);
                padding: 16px 20px;
                border-radius: 12px;
                text-align: center;
                text-decoration: none;
                font-size: 15px;
                font-weight: 600;
                box-shadow: 0 4px 15px rgba(0,0,0,0.25);
                transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            }
            .bio-link:hover {
                transform: translateY(-2px);
                filter: brightness(1.15);
                box-shadow: 0 8px 25px rgba(219,39,119,0.3);
            }
            .media-section {
                width: 100%;
                margin-top: 12px;
            }
            .media-title {
                font-size: 13px;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                color: var(--text-secondary);
                margin-bottom: 16px;
                text-align: center;
            }
            .media-grid {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 8px;
            }
            .media-card {
                aspect-ratio: 1;
                border-radius: 8px;
                overflow: hidden;
                border: 1px solid var(--border-glass);
                position: relative;
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                transition: transform 0.2s ease;
            }
            .media-card:hover {
                transform: scale(1.03);
            }
            .media-card img {
                width: 100%;
                height: 100%;
                object-fit: cover;
            }
            .media-badge {
                position: absolute;
                top: 6px;
                right: 6px;
                background: rgba(0,0,0,0.6);
                padding: 2px 5px;
                border-radius: 4px;
                font-size: 10px;
            }
            footer {
                margin-top: 40px;
                font-size: 12px;
                color: var(--text-secondary);
                display: flex;
                align-items: center;
                gap: 6px;
            }
            footer a {
                color: var(--text-primary);
                text-decoration: none;
                font-weight: 600;
            }
        </style>
    </head>
    <body>
        <div class="bio-container">
            <div class="avatar">
                <div class="avatar-inner">
                    {% if avatar_url %}
                        <img src="{{ avatar_url }}" alt="avatar">
                    {% else %}
                        📷
                    {% endif %}
                </div>
            </div>
            
            <div class="header-info">
                <h1>{{ title }}</h1>
                <p>{{ bio }}</p>
            </div>
            
            <div class="links-list">
                {% for link in links %}
                    <a href="{{ link.url }}" target="_blank" class="bio-link">{{ link.label }}</a>
                {% endfor %}
            </div>
            
            {% if show_ig_feed and media_items %}
            <div class="media-section">
                <div class="media-title">Instagram Feed</div>
                <div class="media-grid">
                    {% for item in media_items %}
                        <a href="{{ item.permalink or '#' }}" target="_blank" class="media-card">
                            <img src="{{ item.thumbnail }}" alt="feed item">
                            {% if item.media_type == 'video' %}
                                <span class="media-badge">🎬</span>
                            {% endif %}
                        </a>
                    {% endfor %}
                </div>
            </div>
            {% endif %}
            
            <footer>
                Powered by <a href="https://superprofile.bio/" target="_blank">SuperProfile.bio</a>
            </footer>
        </div>
    </body>
    </html>
    """
    
    clean_media = []
    for item in media_items:
        clean_media.append({
            "thumbnail": item.get("thumbnail"),
            "permalink": item.get("permalink") or f"https://www.instagram.com/p/{item.get('id')}/",
            "media_type": item.get("media_type")
        })
        
    return render_template_string(
        bio_html,
        title=config.get("title", f"@{username}"),
        bio=config.get("bio", ""),
        btn_color=config.get("btn_color", "#db2777"),
        btn_text_color=config.get("btn_text_color", "#ffffff"),
        avatar_url=config.get("avatar_url", ""),
        links=config.get("links", []),
        show_ig_feed=config.get("show_ig_feed", True),
    )


# ── Leads Capture endpoints ──
@app.route("/instagram/ui/leads", methods=["GET"])
def ig_get_leads():
    return jsonify(load_ig_leads())

@app.route("/instagram/ui/leads/export", methods=["GET"])
def ig_export_leads_csv():
    try:
        leads = load_ig_leads()
        
        import io
        import csv
        from flask import Response
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(["Instagram ID", "Username", "Email", "Phone", "Automation Triggered", "Captured At"])
        
        for lead in leads:
            cap_time = datetime.datetime.fromtimestamp(lead.get("captured_at", 0)).strftime("%Y-%m-%d %H:%M:%S") if lead.get("captured_at") else ""
            writer.writerow([
                lead.get("user_id", ""),
                lead.get("username", ""),
                lead.get("email", ""),
                lead.get("phone", ""),
                lead.get("automation_name", ""),
                cap_time
            ])
            
        csv_data = output.getvalue()
        output.close()
        
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=ig_leads.csv"}
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Content Scheduler endpoints ──
@app.route("/instagram/ui/schedule", methods=["GET"])
def ig_get_scheduled_posts():
    return jsonify(load_ig_scheduled_posts())

@app.route("/instagram/ui/schedule", methods=["POST"])
def ig_schedule_post():
    try:
        data = request.json
        media_url = data.get("media_url")
        caption = data.get("caption", "")
        scheduled_time = float(data.get("scheduled_time", time.time()))
        
        if not media_url:
            return jsonify({"ok": False, "error": "media_url is required"})
            
        posts = load_ig_scheduled_posts()
        new_post = {
            "id": str(int(time.time() * 1000)),
            "media_url": media_url,
            "caption": caption,
            "scheduled_time": scheduled_time,
            "status": "scheduled",
            "error": ""
        }
        posts.append(new_post)
        save_ig_scheduled_posts(posts)
        return jsonify({"ok": True, "post": new_post})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/instagram/ui/schedule/<post_id>", methods=["DELETE"])
def ig_delete_scheduled_post(post_id):
    try:
        posts = load_ig_scheduled_posts()
        filtered = [p for p in posts if p.get("id") != post_id]
        if len(filtered) < len(posts):
            save_ig_scheduled_posts(filtered)
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Post not found"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Instagram Setup status check ──
@app.route("/instagram/ui/status-check", methods=["GET"])
def ig_status_check():
    if not PAGE_ID:
        return jsonify({"ok": False, "linked": False, "error": "PAGE_ID is not configured in .env"})
        
    try:
        resp = requests.get(
            f"{GRAPH_URL}/{PAGE_ID}",
            params={"fields": "instagram_business_account", "access_token": PAGE_ACCESS_TOKEN},
            timeout=8
        )
        data = resp.json()
        ig_id = data.get("instagram_business_account", {}).get("id")
        if ig_id:
            return jsonify({
                "ok": True,
                "linked": True,
                "business_account_id": ig_id,
                "message": "Account validation successful. Connected to Instagram Business/Creator account."
            })
        else:
            return jsonify({
                "ok": False,
                "linked": False,
                "error": "Connected Facebook Page is not linked to any Instagram Business/Creator account. Make sure your Instagram profile is converted to Creator or Business and linked to the Facebook Page."
            })
    except Exception as e:
        return jsonify({"ok": False, "linked": False, "error": f"API connection error: {str(e)}"})


@app.route("/webhook", methods=["GET"])

def verify():
    mode      = request.args.get("hub.mode")
    token     = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge
    return "Forbidden", 403

@app.route("/webhook", methods=["POST"])
def webhook():
    # Meta signature validation (X-Hub-Signature-256)
    signature_header = request.headers.get("X-Hub-Signature-256")
    if signature_header and IG_APP_SECRET:
        try:
            sha_name, signature = signature_header.split("=")
            if sha_name == "sha256":
                mac = hmac.new(IG_APP_SECRET.encode("utf-8"), request.get_data(), hashlib.sha256)
                if not hmac.compare_digest(mac.hexdigest(), signature):
                    print("[Webhook Validation] ❌ Signature verification failed!", flush=True)
                    return "Invalid signature", 403
                print("[Webhook Validation] ✅ Signature verification passed.", flush=True)
        except Exception as e:
            print(f"[Webhook Validation] ❌ Error verifying signature: {e}", flush=True)
            return "Signature verification error", 403

    data = request.json
    obj  = data.get("object")
    print(f"[Webhook Received] object={obj} payload={json.dumps(data)}")
    for entry in data.get("entry", []):
        # Instagram webhooks (separate automation engine)
        if obj == "instagram":
            for change in entry.get("changes", []):
                field = change.get("field")
                value = change.get("value", {})
                if field == "comments":
                    handle_ig_comment(value, "comment")
                elif field == "live_comments":
                    handle_ig_comment(value, "live")
                elif field in ("mentions", "mention_tag"):
                    handle_ig_mention(value)
            for event in entry.get("messaging", []):
                handle_ig_messaging(event)
            continue

        # WhatsApp webhooks (official Meta Cloud API)
        if obj == "whatsapp_business_account":
            for change in entry.get("changes", []):
                if change.get("field") == "messages":
                    val = change.get("value", {})
                    for msg in val.get("messages", []):
                        contacts = val.get("contacts", [])
                        contact = contacts[0] if contacts else {}
                        handle_official_wa_message(msg, contact)
            continue

        # Facebook webhooks (unchanged)
        for change in entry.get("changes", []):
            if change.get("field") == "feed":
                handle_comment(change.get("value", {}))
    return "EVENT_RECEIVED", 200

# Load replied IDs into memory on startup
_load_replied_from_file()
_load_ig_replied_from_file()
_load_ig_welcomed()

def initialize_token():
    global PAGE_ACCESS_TOKEN
    print("[Token Initialization] Validating token...")
    try:
        resp = requests.get(f"https://graph.facebook.com/v19.0/{PAGE_ID}", params={"access_token": PAGE_ACCESS_TOKEN}, timeout=8)
        if resp.status_code == 200 and "error" not in resp.json():
            print("[Token Initialization] Token is valid.")
            return
        else:
            err_msg = resp.json().get("error", {}).get("message", "Unknown error")
            raise RuntimeError(err_msg)
    except Exception as check_err:
        raise RuntimeError(f"Token validation failed: {check_err}")

try:
    init_sqlite_db()
    initialize_token()
    discover_ig_user_id()
    subscribe_page()
    subscribe_instagram()
except Exception as e:
    print(f"[Subscribe error] {e}")


def check_token_health():
    if not IG_APP_ID or not IG_APP_SECRET or not PAGE_ACCESS_TOKEN:
        print("[Token Health] Missing credentials to check token health.")
        return
    
    print("[Token Health] Checking token health...", flush=True)
    try:
        app_token = f"{IG_APP_ID}|{IG_APP_SECRET}"
        resp = requests.get(
            "https://graph.facebook.com/debug_token",
            params={
                "input_token": PAGE_ACCESS_TOKEN,
                "access_token": app_token
            },
            timeout=10
        )
        data = resp.json()
        if resp.status_code == 200 and "data" in data:
            token_info = data["data"]
            is_valid = token_info.get("is_valid", False)
            expires_at = token_info.get("expires_at", 0)
            scopes = token_info.get("scopes", [])
            
            status_str = "valid" if is_valid else "invalid"
            
            with db_lock:
                conn = get_db_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT OR REPLACE INTO ig_token_health (key, status, expires_at, last_check, scopes, error) VALUES ('main', ?, ?, ?, ?, NULL)",
                        (status_str, expires_at, time.time(), json.dumps(scopes))
                    )
                    conn.commit()
                finally:
                    conn.close()
            print(f"[Token Health] Token is {status_str}. Expires at: {expires_at}", flush=True)
        else:
            err = data.get("error", {}).get("message", str(data))
            _save_token_error(err)
    except Exception as e:
        _save_token_error(str(e))

def _save_token_error(err_str):
    print(f"[Token Health Error] {err_str}", flush=True)
    with db_lock:
        conn = get_db_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO ig_token_health (key, status, expires_at, last_check, scopes, error) VALUES ('main', 'error', 0, ?, '[]', ?)",
                (time.time(), err_str)
            )
            conn.commit()
        finally:
            conn.close()

def ig_token_health_worker():
    print("[Token Health Worker] Started background token health daemon.", flush=True)
    while True:
        try:
            check_token_health()
        except Exception as e:
            print(f"[Token Health Worker Error] {e}")
        time.sleep(21600)

# ── Unified Inbox endpoints ──
@app.route("/instagram/ui/inbox/threads", methods=["GET"])
def get_ig_threads():
    if not IG_USER_ID:
        return jsonify({"error": "IG_USER_ID is not configured"}), 400
    try:
        resp = requests.get(
            f"{GRAPH_URL}/{IG_USER_ID}/conversations",
            params={
                "fields": "id,participants,updated_time,unread_count,messages.limit(1){message,from,created_time}",
                "access_token": PAGE_ACCESS_TOKEN
            },
            timeout=12
        )
        if resp.status_code != 200:
            return jsonify({"error": resp.json().get("error", {}).get("message", "API Error")}), resp.status_code
        
        data = resp.json().get("data", [])
        threads = []
        for item in data:
            participants = item.get("participants", {}).get("data", [])
            user = None
            for p in participants:
                if str(p.get("id")) != str(IG_USER_ID):
                    user = p
                    break
            if not user:
                if participants:
                    user = participants[0]
                else:
                    user = {"id": "unknown", "username": "Instagram User"}
                    
            last_msg = ""
            msgs = item.get("messages", {}).get("data", [])
            if msgs:
                last_msg = msgs[0].get("message", "")
                
            threads.append({
                "thread_id": item.get("id"),
                "user_id": user.get("id"),
                "username": user.get("username", "User"),
                "updated_time": item.get("updated_time"),
                "unread_count": item.get("unread_count", 0),
                "last_message": last_msg
            })
        return jsonify(threads)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/instagram/ui/inbox/reply", methods=["POST"])
def send_manual_inbox_reply():
    data = request.json or {}
    user_id = data.get("user_id")
    text = data.get("text")
    if not user_id or not text:
        return jsonify({"error": "user_id and text are required"}), 400
    
    queue_ig_message(recipient_id=user_id, text=text, automation_name="manual")
    return jsonify({"success": True, "message": "Reply queued for sending."})


@app.route("/instagram/ui/token-health", methods=["GET"])
def get_token_health():
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status, expires_at, last_check, scopes, error FROM ig_token_health WHERE key = 'main'")
        row = cursor.fetchone()
        if row:
            return jsonify({
                "status": row["status"],
                "expires_at": row["expires_at"],
                "last_check": row["last_check"],
                "scopes": json.loads(row["scopes"] or "[]"),
                "error": row["error"]
            })
        return jsonify({"status": "unknown", "message": "No health check performed yet."})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        conn.close()


if __name__ == "__main__":
    import threading
    threading.Thread(target=ig_queue_worker, daemon=True).start()
    threading.Thread(target=ig_scheduler_worker, daemon=True).start()
    threading.Thread(target=ig_token_health_worker, daemon=True).start()
    
    port = int(os.environ.get("FLASK_PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)