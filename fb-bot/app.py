from flask import Flask, request, jsonify, render_template_string
import time
import requests
import os
import json
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


VERIFY_TOKEN      = os.getenv("VERIFY_TOKEN", "myverifytoken")
PAGE_ID           = os.getenv("PAGE_ID", "657207910809297")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN", "EAAOEye5xXB4BR6ch9TYwzTXjHzZBm0B2hEIEcOiaKKkwApIAxriXPcL6JWRZBZCY4btAOJfrlpFZCvsZBqyZBGZAAFZCohutvzKfK56zZAnQLguXHrUvCbMhZCRZA5j0ZCpu9WeNVP2ZABN3rW4bWYPbl8V6iTSvcxt5pV7pdc1ZBjZAiuquoLd2Wt2oZAeeKRx8tZAAVyWk51ZCkDwshH")
IG_USER_ID        = os.getenv("IG_USER_ID", "17841451641925459")
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v19.0")
GRAPH_URL         = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_FILE     = os.path.join(BASE_DIR, "keywords.json")
AUTOMATIONS_FILE  = os.path.join(BASE_DIR, "automations.json")
REPLIED_FILE      = os.path.join(BASE_DIR, "replied.json")
MAX_REPLIED_STORE = 5000   # cap stored IDs to avoid unbounded growth

# Instagram automation — separate storage (does not touch Facebook data)
IG_AUTOMATIONS_FILE = os.path.join(BASE_DIR, "ig_automations.json")
IG_KEYWORDS_FILE    = os.path.join(BASE_DIR, "ig_keywords.json")
IG_REPLIED_FILE     = os.path.join(BASE_DIR, "ig_replied.json")
IG_STATS_FILE       = os.path.join(BASE_DIR, "ig_stats.json")
IG_WELCOMED_FILE    = os.path.join(BASE_DIR, "ig_welcomed.json")
IG_SETTINGS_FILE    = os.path.join(BASE_DIR, "ig_settings.json")

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
    
    tokens_to_try = [PAGE_ACCESS_TOKEN, "EAAOEye5xXB4BR6ch9TYwzTXjHzZBm0B2hEIEcOiaKKkwApIAxriXPcL6JWRZBZCY4btAOJfrlpFZCvsZBqyZBGZAAFZCohutvzKfK56zZAnQLguXHrUvCbMhZCRZA5j0ZCpu9WeNVP2ZABN3rW4bWYPbl8V6iTSvcxt5pV7pdc1ZBjZAiuquoLd2Wt2oZAeeKRx8tZAAVyWk51ZCkDwshH", "EAAOEye5xXB4BRzz8MnN62XaqxROB40ES6qPY1PY0Vpf5jpZAjsCAu0ZCOs9cNQqRgZAp9NrKJp8bMtIOhe3bWPovQJFlwcYkDuLytihtDXKeqHQvoJQERMKQ5xPZCepNLve3G6jU1Dyb4rtZAPKv2MeqB2IqsEolCGe4tu9nYdC7ZB0nMLoOKZBvazjZCzDmS8ZBm6kIbE9ZBY"]
    tokens_to_try = list(dict.fromkeys(t for t in tokens_to_try if t))
    
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
    This works even if the user has never messaged the Page before.
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
    else:
        print(f"  [Private Reply failed ❌] {result.get('error', {}).get('message', result)}")

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


def load_ig_keywords():
    if os.path.exists(IG_KEYWORDS_FILE):
        with open(IG_KEYWORDS_FILE) as f:
            return json.load(f)
    return {}

def save_ig_keywords(data):
    with open(IG_KEYWORDS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_ig_automations():
    if os.path.exists(IG_AUTOMATIONS_FILE):
        with open(IG_AUTOMATIONS_FILE) as f:
            return json.load(f)
    return []

def save_ig_automations(data):
    with open(IG_AUTOMATIONS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_ig_stats():
    if os.path.exists(IG_STATS_FILE):
        with open(IG_STATS_FILE) as f:
            return json.load(f)
    return {"comment_replies": 0, "dms_sent": 0, "story_replies": 0, "live_replies": 0,
            "dm_triggers": 0, "mentions_handled": 0, "dms_today": 0, "dms_today_date": ""}

def save_ig_stats(stats):
    with open(IG_STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)

def bump_ig_stat(key):
    stats = load_ig_stats()
    stats[key] = stats.get(key, 0) + 1
    save_ig_stats(stats)

def _load_ig_replied_from_file():
    global _ig_replied_set
    try:
        if os.path.exists(IG_REPLIED_FILE):
            with open(IG_REPLIED_FILE) as f:
                _ig_replied_set = set(json.load(f))
            print(f"[IG Replied] Loaded {len(_ig_replied_set)} IDs")
    except Exception as e:
        print(f"[IG Replied] Load error: {e}")
        _ig_replied_set = set()

def _save_ig_replied_to_file():
    try:
        ids = list(_ig_replied_set)[-MAX_REPLIED_STORE:]
        with open(IG_REPLIED_FILE, "w") as f:
            json.dump(ids, f)
    except Exception as e:
        print(f"[IG Replied] Save error: {e}")

def ig_already_replied(key):
    return key in _ig_replied_set

def ig_mark_replied(key):
    _ig_replied_set.add(key)
    if len(_ig_replied_set) > MAX_REPLIED_STORE:
        trimmed = list(_ig_replied_set)[-MAX_REPLIED_STORE:]
        _ig_replied_set.clear()
        _ig_replied_set.update(trimmed)
    _save_ig_replied_to_file()


# ── Welcome DM tracking (first-time per user only) ──────────────────────────────────
_ig_welcomed_set: set = set()

def _load_ig_welcomed():
    global _ig_welcomed_set
    try:
        if os.path.exists(IG_WELCOMED_FILE):
            with open(IG_WELCOMED_FILE) as f:
                _ig_welcomed_set = set(json.load(f))
            print(f"[IG Welcomed] Loaded {len(_ig_welcomed_set)} user IDs")
    except Exception:
        _ig_welcomed_set = set()

def _save_ig_welcomed():
    try:
        with open(IG_WELCOMED_FILE, "w") as f:
            json.dump(list(_ig_welcomed_set)[-MAX_REPLIED_STORE:], f)
    except Exception as e:
        print(f"[IG Welcomed] Save error: {e}")

def ig_already_welcomed(user_id):
    return user_id in _ig_welcomed_set

def ig_mark_welcomed(user_id):
    _ig_welcomed_set.add(user_id)
    _save_ig_welcomed()


# ── Instagram settings (daily DM cap) ───────────────────────────────────────────
def load_ig_settings():
    if os.path.exists(IG_SETTINGS_FILE):
        with open(IG_SETTINGS_FILE) as f:
            return json.load(f)
    return {"daily_dm_cap": 200}

def save_ig_settings(data):
    with open(IG_SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2)


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
        
    tokens_to_try = [PAGE_ACCESS_TOKEN, "EAAOEye5xXB4BR6ch9TYwzTXjHzZBm0B2hEIEcOiaKKkwApIAxriXPcL6JWRZBZCY4btAOJfrlpFZCvsZBqyZBGZAAFZCohutvzKfK56zZAnQLguXHrUvCbMhZCRZA5j0ZCpu9WeNVP2ZABN3rW4bWYPbl8V6iTSvcxt5pV7pdc1ZBjZAiuquoLd2Wt2oZAeeKRx8tZAAVyWk51ZCkDwshH", "EAAOEye5xXB4BRzz8MnN62XaqxROB40ES6qPY1PY0Vpf5jpZAjsCAu0ZCOs9cNQqRgZAp9NrKJp8bMtIOhe3bWPovQJFlwcYkDuLytihtDXKeqHQvoJQERMKQ5xPZCepNLve3G6jU1Dyb4rtZAPKv2MeqB2IqsEolCGe4tu9nYdC7ZB0nMLoOKZBvazjZCzDmS8ZBm6kIbE9ZBY"]
    tokens_to_try = list(dict.fromkeys(t for t in tokens_to_try if t))
    
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
    if auto.get("email_capture") and auto.get("email_prompt"):
        parts.append(auto["email_prompt"])
    follow_up = personalize_ig_message(auto.get("follow_up_message", ""), username)
    if follow_up:
        parts.append(follow_up)
    return "\n\n".join(p for p in parts if p)


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


def send_ig_private_reply(comment_id, message):
    if not IG_USER_ID:
        print("  [IG AutoDM failed ❌] IG_USER_ID not set")
        return
    if not daily_cap_ok():
        print("  [IG AutoDM skipped ⚠️] Daily DM cap reached")
        return
    resp = requests.post(
        f"{GRAPH_URL}/{IG_USER_ID}/messages",
        params={"access_token": PAGE_ACCESS_TOKEN},
        json={"recipient": {"comment_id": comment_id}, "message": {"text": message}},
        timeout=8,
    )
    result = resp.json()
    if "message_id" in result:
        print(f"  [IG AutoDM sent] ✅")
        bump_ig_stat("dms_sent")
        bump_daily_dm()
    else:
        print(f"  [IG AutoDM failed] ❌ → {result}")


def send_ig_dm(user_id, message):
    if not IG_USER_ID:
        return
    if not daily_cap_ok():
        print("  [IG DM skipped ⚠️] Daily DM cap reached")
        return
    resp = requests.post(
        f"{GRAPH_URL}/{IG_USER_ID}/messages",
        params={"access_token": PAGE_ACCESS_TOKEN},
        json={"recipient": {"id": user_id}, "message": {"text": message}},
        timeout=8,
    )
    result = resp.json()
    if "message_id" in result:
        bump_ig_stat("dms_sent")
        bump_daily_dm()
    else:
        print(f"  [IG DM failed] ❌ → {result}")


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

        print(f"  IG Auto '{auto['name']}' matched ({trigger_type})")

        # Welcome DM: only fire once per user (first-time detection)
        if trigger_type == "welcome" and user_id:
            if ig_already_welcomed(user_id):
                print(f"  [SKIP] Welcome DM already sent to {user_id}")
                continue
            ig_mark_welcomed(user_id)

        # Optional send delay — makes bot appear more human
        delay = int(auto.get("delay_seconds", 0))
        if delay > 0:
            print(f"  [Delay] Waiting {delay}s before sending...")
            time.sleep(delay)

        action = auto.get("action", "both")

        if action in ("comment", "both") and auto.get("reply") and comment_id:
            reply_to_ig_comment(comment_id, personalize_ig_message(auto["reply"], username))

        if action in ("dm", "both") and auto.get("dm_message"):
            dm_body = build_ig_dm_body(auto, username)
            if comment_id:
                send_ig_private_reply(comment_id, dm_body)
            elif user_id:
                send_ig_dm(user_id, dm_body)

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


def handle_ig_comment(value, trigger_type="comment"):
    comment_id = value.get("comment_id") or value.get("id", "")
    text       = (value.get("text") or "").lower().strip()
    media_id   = value.get("media", {}).get("id", "")
    user_id    = value.get("from", {}).get("id", "")
    username   = value.get("from", {}).get("username", "")
    if not comment_id:
        return
    print(f"[IG {trigger_type.upper()}] id={comment_id} from=@{username} text={text}")

    if IG_USER_ID and user_id == IG_USER_ID:
        print("[SKIP] Own account comment")
        return

    save_last_tester(user_id, username)

    dedup_key = f"ig:{trigger_type}:{user_id}:{media_id}:{text}"
    if ig_already_replied(dedup_key):
        print(f"[SKIP] Already handled (dedup_key={dedup_key})")
        return

    if run_ig_automations(trigger_type, text, media_id, comment_id, user_id, username):
        if trigger_type == "live":
            bump_ig_stat("live_replies")
        ig_mark_replied(dedup_key)
    print("  ---")


def handle_ig_messaging(event):
    sender_id = event.get("sender", {}).get("id", "")
    recipient_id = event.get("recipient", {}).get("id", "")
    message   = event.get("message", {}) or {}
    is_echo   = message.get("is_echo", False)
    
    # Identify actual user (follower) ID to avoid registering the bot itself
    actual_user_id = recipient_id if is_echo else sender_id
    if actual_user_id and actual_user_id != IG_USER_ID:
        save_last_tester(actual_user_id, "")
        
    if not sender_id or not message:
        return

    text = (message.get("text") or "").lower().strip()
    reply_to = message.get("reply_to") or {}

    if reply_to.get("story"):
        print(f"[IG STORY REPLY] from={sender_id} text={text}")
        dedup_key = f"ig:story:{sender_id}:{text}"
        if ig_already_replied(dedup_key):
            return
        if run_ig_automations("story", text, user_id=sender_id):
            bump_ig_stat("story_replies")
            ig_mark_replied(dedup_key)
        return

    if text:
        print(f"[IG DM] from={sender_id} text={text}")
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
    print(f"[IG MENTION] from=@{username} media={media_id} text={text}")

    if IG_USER_ID and user_id == IG_USER_ID:
        print("[SKIP] Own account mention")
        return

    save_last_tester(user_id, username)

    dedup_key = f"ig:mention:{user_id}:{media_id}"
    if ig_already_replied(dedup_key):
        print(f"[SKIP] Already handled mention (dedup_key={dedup_key})")
        return

    if run_ig_automations("mention", text, media_id=media_id, user_id=user_id, username=username):
        bump_ig_stat("mentions_handled")
        ig_mark_replied(dedup_key)
    print("  ---")


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
    """Send a test DM to the last active tester's IG account to preview the automation message."""
    autos = load_ig_automations()
    if not (0 <= idx < len(autos)):
        return jsonify({"ok": False, "error": "Automation not found"})
    if not IG_USER_ID:
        return jsonify({"ok": False, "error": "IG_USER_ID not discovered yet — make sure your IG is linked to your Facebook Page"})
    
    tester = load_last_tester()
    print("[DEBUG TESTER] Loaded tester:", tester)
    if not tester or not tester.get("user_id"):
        print("[DEBUG TESTER] No test user found, returning error JSON")
        return jsonify({
            "ok": False, 
            "error": "No test user found! Please send a comment, mention, or DM to your Instagram Business account from your personal Instagram account first to register your test ID, then click Test again."
        })
        
    target_id = tester["user_id"]
    target_name = tester.get("username") or "you"
    
    auto    = autos[idx]
    dm_body = build_ig_dm_body(auto, target_name)
    if not dm_body:
        dm_body = auto.get("reply", "Test automation message")
        
    # Bypass daily cap for test — send directly
    resp = requests.post(
        f"{GRAPH_URL}/{IG_USER_ID}/messages",
        params={"access_token": PAGE_ACCESS_TOKEN},
        json={"recipient": {"id": target_id}, "message": {"text": f"[TEST — {auto['name']}]\n\n{dm_body}"}},
        timeout=8,
    )
    result = resp.json()
    if "message_id" in result:
        return jsonify({"ok": True, "message": f"Test DM sent to @{target_name} for '{auto['name']}'"})
    return jsonify({"ok": False, "error": result.get("error", {}).get("message", str(result))})

@app.route("/instagram/ui/stats")
def ig_get_stats():
    return jsonify(load_ig_stats())

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
        save_ig_settings(settings)
        return jsonify({"ok": True})
    settings = load_ig_settings()
    stats    = load_ig_stats()
    settings["dms_today"]       = stats.get("dms_today", 0)
    settings["dms_today_date"]  = stats.get("dms_today_date", "")
    return jsonify(settings)


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
    data = request.json
    obj  = data.get("object")
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
    except Exception as check_err:
        print(f"[Token Check Error] {check_err}")

    print("[Token Initialization] Token is invalid. Finding working fallback token...")
    fallbacks = [
        "EAAOEye5xXB4BR6ch9TYwzTXjHzZBm0B2hEIEcOiaKKkwApIAxriXPcL6JWRZBZCY4btAOJfrlpFZCvsZBqyZBGZAAFZCohutvzKfK56zZAnQLguXHrUvCbMhZCRZA5j0ZCpu9WeNVP2ZABN3rW4bWYPbl8V6iTSvcxt5pV7pdc1ZBjZAiuquoLd2Wt2oZAeeKRx8tZAAVyWk51ZCkDwshH",
        "EAAOEye5xXB4BRzz8MnN62XaqxROB40ES6qPY1PY0Vpf5jpZAjsCAu0ZCOs9cNQqRgZAp9NrKJp8bMtIOhe3bWPovQJFlwcYkDuLytihtDXKeqHQvoJQERMKQ5xPZCepNLve3G6jU1Dyb4rtZAPKv2MeqB2IqsEolCGe4tu9nYdC7ZB0nMLoOKZBvazjZCzDmS8ZBm6kIbE9ZBY"
    ]
    for fb in fallbacks:
        try:
            r = requests.get(f"https://graph.facebook.com/v19.0/{PAGE_ID}", params={"access_token": fb}, timeout=8)
            if r.status_code == 200 and "error" not in r.json():
                PAGE_ACCESS_TOKEN = fb
                print(f"[Token Initialization] Recovered using fallback token: {fb[:15]}...")
                return
        except Exception:
            continue
    print("[Token Initialization] WARNING: No working fallback tokens found!")

try:
    initialize_token()
    discover_ig_user_id()
    subscribe_page()
    subscribe_instagram()
except Exception as e:
    print(f"[Subscribe error] {e}")

if __name__ == "__main__":
    # Dynamically bind port to the environment variable FLASK_PORT
    port = int(os.environ.get("FLASK_PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)