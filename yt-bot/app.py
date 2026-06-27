import os
import json
import time
import random
import threading
import secrets
from flask import Flask, request, jsonify, render_template, redirect, session

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("pip install google-api-python-client google-auth-oauthlib")
    raise

# ── File Paths ────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE    = os.path.join(SCRIPT_DIR, "token.json")
SETTINGS_FILE = os.path.join(SCRIPT_DIR, "settings.json")
QUOTA_FILE    = os.path.join(SCRIPT_DIR, "quota_log.json")
SEEN_FILE     = os.path.join(SCRIPT_DIR, "seen_authors.json")   # video_id::author_id
SECRET_FILE   = os.path.join(SCRIPT_DIR, "session.key")
LOG_FILE      = os.path.join(SCRIPT_DIR, "bot.log")

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# ── Quota limit: hard-stop at 9599 used (401 buffer) ─────────────────────────
QUOTA_HARD_STOP = 9599

# ── Persistent secret key (survives restarts = sessions always saved) ─────────
def _get_secret_key() -> bytes:
    if os.path.exists(SECRET_FILE):
        try:
            return open(SECRET_FILE, "rb").read()
        except Exception:
            pass
    key = secrets.token_bytes(32)
    try:
        with open(SECRET_FILE, "wb") as f:
            f.write(key)
    except Exception:
        pass
    return key

app = Flask(__name__)
app.secret_key = _get_secret_key()          # fixed → Flask sessions persist across restarts

# ── In-Memory triple-guard: never reply to same author twice ──────────────────
# Layer 1: in-memory set (fastest, lost on restart → backed by layer 2)
_replied_memory: set = set()
_memory_lock = threading.Lock()

# ── Logs ──────────────────────────────────────────────────────────────────────
logs_history: list = []
MAX_LOG = 200
_log_lock = threading.Lock()

def log(level: str, msg: str):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{level}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    with _log_lock:
        logs_history.append(line)
        if len(logs_history) > MAX_LOG:
            logs_history.pop(0)

# ── Settings ──────────────────────────────────────────────────────────────────
def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log("WARNING", f"settings.json parse error: {e}")
    return {
        "bot_running":         False,
        "poll_interval":       120,
        "max_results":         20,
        "reply_delay_min":     8,
        "reply_delay_max":     20,
        "automated_video_ids": [],
        "video_configs":       {}
    }

def save_settings(data: dict) -> bool:
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        log("ERROR", f"save settings failed: {e}")
        return False

# ── Quota Tracker ─────────────────────────────────────────────────────────────
class QuotaTracker:
    def __init__(self):
        self.filename = QUOTA_FILE
        self.data = {"date": time.strftime("%Y-%m-%d"), "used": 0}
        self._load()

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                pass
        self._reset_if_needed()

    def _save(self):
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2)
        except Exception:
            pass

    def _reset_if_needed(self):
        today = time.strftime("%Y-%m-%d")
        if self.data.get("date") != today:
            self.data = {"date": today, "used": 0}
            self._save()

    def consume(self, units: int):
        self._reset_if_needed()
        self.data["used"] = self.data.get("used", 0) + units
        self._save()

    @property
    def used(self) -> int:
        self._reset_if_needed()
        return self.data.get("used", 0)

    @property
    def remaining(self) -> int:
        return max(0, 10000 - self.used)

    @property
    def hard_stop_reached(self) -> bool:
        return self.used >= QUOTA_HARD_STOP

# ── Seen-Authors Tracker (Layer 2: persistent disk) ───────────────────────────
class SeenAuthors:
    def __init__(self):
        self.filename = SEEN_FILE
        self.seen: set = set()
        self._load()
        # Sync to in-memory layer too
        with _memory_lock:
            _replied_memory.update(self.seen)

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    self.seen = set(json.load(f))
            except Exception:
                pass

    def _save(self):
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(sorted(list(self.seen)), f, indent=2)
        except Exception:
            pass

    @staticmethod
    def _key(video_id: str, author_id: str) -> str:
        return f"{video_id}::{author_id}"

    def is_seen(self, video_id: str, author_id: str) -> bool:
        k = self._key(video_id, author_id)
        # Layer 1 check (fastest)
        with _memory_lock:
            if k in _replied_memory:
                return True
        # Layer 2 check (disk)
        return k in self.seen

    def mark(self, video_id: str, author_id: str):
        k = self._key(video_id, author_id)
        self.seen.add(k)
        with _memory_lock:
            _replied_memory.add(k)
        self._save()

# ── Super trick: Layer 3 — Live API verification ──────────────────────────────
# Before posting ANY reply, actually fetch the thread from YouTube API and
# confirm the channel owner has NOT already replied. This is bulletproof.
def owner_already_replied(youtube, thread_id: str, channel_id: str) -> bool:
    """Returns True if the channel owner already has a reply in this thread."""
    try:
        resp = youtube.commentThreads().list(
            part="replies",
            id=thread_id,
            maxResults=1
        ).execute()
        items = resp.get("items", [])
        if not items:
            return False
        replies = items[0].get("replies", {}).get("comments", [])
        for r in replies:
            r_author = r.get("snippet", {}).get("authorChannelId", {}).get("value", "")
            if r_author == channel_id:
                return True
        # If totalReplyCount > len(replies), fetch full list
        total = items[0].get("snippet", {}).get("totalReplyCount", 0)
        if total > len(replies):
            full = youtube.comments().list(
                part="snippet",
                parentId=thread_id,
                maxResults=100
            ).execute()
            for r in full.get("items", []):
                r_author = r.get("snippet", {}).get("authorChannelId", {}).get("value", "")
                if r_author == channel_id:
                    return True
    except Exception as ex:
        log("WARNING", f"Live reply check failed for thread {thread_id}: {ex}")
        # Fail safe: assume already replied to avoid duplication
        return True
    return False

# ── OAuth helpers ─────────────────────────────────────────────────────────────
def find_client_secrets_file() -> str:
    """Locate client_secret*.json, falling back to env var or built-in credentials."""
    import base64
    # 1. Look for an existing file in the script directory
    for name in os.listdir(SCRIPT_DIR):
        if name.lower().endswith('.json') and 'client_secret' in name.lower():
            return os.path.join(SCRIPT_DIR, name)

    # 2. Try to reconstruct from environment variable (Railway deployment)
    env_json = os.environ.get("YOUTUBE_CLIENT_SECRETS_JSON", "").strip()
    if env_json:
        target = os.path.join(SCRIPT_DIR, "client_secrets.json")
        try:
            with open(target, "w", encoding="utf-8") as f:
                f.write(env_json)
            log("INFO", "Wrote client_secrets.json from YOUTUBE_CLIENT_SECRETS_JSON env var.")
            return target
        except Exception as exc:
            log("ERROR", f"Failed to write client_secrets.json from env var: {exc}")

    # 3. Built-in fallback (base64-encoded to avoid accidental exposure in logs)
    _B64 = (
        "eyJ3ZWIiOiB7ImNsaWVudF9pZCI6ICI5ODE2MjA3ODQzODEtZ2x0NDh1bjJrdGoxMnQxMzM3"
        "cXF0ODNpbzFiYXJ0ODQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCAicHJvamVjdF9p"
        "ZCI6ICJkZXYtc2V0dGluZy00NTk1MTAtZDUiLCAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2Nv"
        "dW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLCAidG9rZW5fdXJpIjogImh0dHBzOi8v"
        "b2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0"
        "X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLCAi"
        "Y2xpZW50X3NlY3JldCI6ICJHT0NTUFgtS29pRUhLTk9OSWxrUjA4RWRvNEpvUWoxaWNjUSIs"
        "ICJyZWRpcmVjdF91cmlzIjogWyJodHRwczovL3doYXRzYXBwYm90LXByb2R1Y3Rpb24tZDgx"
        "Yy51cC5yYWlsd2F5LmFwcC95dC8iXX19"
    )
    target = os.path.join(SCRIPT_DIR, "client_secrets.json")
    try:
        decoded = base64.b64decode(_B64).decode("utf-8")
        with open(target, "w", encoding="utf-8") as f:
            f.write(decoded)
        log("INFO", "Wrote client_secrets.json from built-in fallback credentials.")
        return target
    except Exception as exc:
        log("ERROR", f"Failed to write built-in client_secrets.json: {exc}")

    raise FileNotFoundError("client_secret*.json not found and all fallback methods failed.")



def get_youtube_client():
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(TOKEN_FILE, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
                log("INFO", "OAuth token refreshed silently.")
            except Exception as ex:
                log("WARNING", f"Token refresh failed: {ex}")
                return None
        if creds and creds.valid:
            missing = [s for s in SCOPES if s not in (creds.scopes or [])]
            if missing:
                log("WARNING", f"Scope mismatch — need {SCOPES}")
                return None
            return build("youtube", "v3", credentials=creds)
    except Exception as e:
        log("ERROR", f"YouTube client init error: {e}")
    return None

# ── Reply-text picker ─────────────────────────────────────────────────────────
def pick_reply(comment_text: str, video_cfg: dict) -> str:
    # ── Any Comment wildcard: fires for EVERY comment if enabled ──
    if video_cfg.get("any_comment_enabled") and video_cfg.get("any_comment_reply", "").strip():
        return video_cfg["any_comment_reply"].strip()

    cleaned = comment_text.lower()
    for rule in video_cfg.get("rules", []):
        for kw in rule.get("keywords", []):
            kw = kw.strip().lower()
            if kw and kw in cleaned:
                return rule.get("reply", "").strip()
    return video_cfg.get("default_reply", "").strip()

# ── Get channel ID (cached in memory to save quota) ───────────────────────────
_channel_id_cache: str = ""

def get_channel_id(youtube) -> str:
    global _channel_id_cache
    if _channel_id_cache:
        return _channel_id_cache
    try:
        ch = youtube.channels().list(part="id", mine=True).execute()
        if ch.get("items"):
            _channel_id_cache = ch["items"][0]["id"]
            log("INFO", f"Channel ID cached: {_channel_id_cache}")
    except Exception as e:
        log("ERROR", f"Could not fetch channel ID: {e}")
    return _channel_id_cache

# ── 24/7 Background Polling Daemon ───────────────────────────────────────────
def polling_daemon():
    log("INFO", "24/7 polling daemon started.")
    tracker = QuotaTracker()
    seen    = SeenAuthors()

    while True:
        try:
            settings = load_settings()

            if not settings.get("bot_running", False):
                time.sleep(5)
                continue

            # ── Hard quota stop ──────────────────────────────────────────────
            if tracker.hard_stop_reached:
                log("WARNING", f"Quota used {tracker.used} >= {QUOTA_HARD_STOP}. Bot paused until tomorrow.")
                time.sleep(300)
                continue

            youtube = get_youtube_client()
            if not youtube:
                log("WARNING", "Credentials missing or invalid — waiting 30s…")
                time.sleep(30)
                continue

            channel_id = get_channel_id(youtube)
            if not channel_id:
                log("ERROR", "Cannot determine channel ID — skipping cycle.")
                time.sleep(60)
                continue

            # Only process videos that are:
            #   a) in automated_video_ids   (video 24/7 toggle ON)
            #   b) have at least one rule in video_configs
            video_ids   = settings.get("automated_video_ids", [])
            video_cfgs  = settings.get("video_configs", {})
            active_vids = [
                v for v in video_ids
                if v in video_cfgs and (
                    video_cfgs[v].get("rules")
                    or video_cfgs[v].get("default_reply")
                    or (video_cfgs[v].get("any_comment_enabled") and video_cfgs[v].get("any_comment_reply", "").strip())
                )
            ]

            if not active_vids:
                log("INFO", "No enabled videos with saved rules. Configure rules and toggle videos ON.")
                time.sleep(30)
                continue

            for video_id in active_vids:
                # Reload to catch live changes mid-cycle
                settings   = load_settings()
                video_cfgs = settings.get("video_configs", {})

                if not settings.get("bot_running", False):
                    break
                if video_id not in settings.get("automated_video_ids", []):
                    continue
                if tracker.hard_stop_reached:
                    log("WARNING", "Quota limit reached mid-cycle. Stopping.")
                    break

                video_cfg = video_cfgs.get(video_id, {})
                has_any = (
                    video_cfg.get("any_comment_enabled")
                    and video_cfg.get("any_comment_reply", "").strip()
                )
                if not (video_cfg.get("rules") or video_cfg.get("default_reply") or has_any):
                    continue

                log("INFO", f"Polling video {video_id}  (used: {tracker.used}/{QUOTA_HARD_STOP})")
                try:
                    resp = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        order="time",
                        maxResults=settings.get("max_results", 20),
                        textFormat="plainText"
                    ).execute()
                    tracker.consume(1)

                    for item in resp.get("items", []):
                        if tracker.hard_stop_reached:
                            log("WARNING", "Quota limit hit — aborting comment loop.")
                            break

                        top       = item.get("snippet", {}).get("topLevelComment", {})
                        thread_id = item.get("id", "")
                        comment_id= top.get("id", "")
                        snippet   = top.get("snippet", {})
                        text      = snippet.get("textOriginal", "")
                        author_id = snippet.get("authorChannelId", {}).get("value", "")

                        if not comment_id or not author_id or not thread_id:
                            continue

                        # Skip own comments
                        if author_id == channel_id:
                            continue

                        # ── LAYER 1 + 2: Memory + disk guard ────────────────
                        if seen.is_seen(video_id, author_id):
                            continue

                        # ── LAYER 3: Live API check — the super trick ────────
                        # Costs 1 quota unit but guarantees no double-reply ever
                        if owner_already_replied(youtube, thread_id, channel_id):
                            log("INFO", f"  Already replied in thread {thread_id} — marking + skipping.")
                            seen.mark(video_id, author_id)  # backfill the guard
                            tracker.consume(1)
                            continue

                        reply_text = pick_reply(text, video_cfg)
                        if not reply_text:
                            continue

                        delay = random.randint(
                            settings.get("reply_delay_min", 8),
                            settings.get("reply_delay_max", 20)
                        )
                        log("INFO", f"  Replying to {author_id[:14]}… in {delay}s")
                        time.sleep(delay)

                        # Final in-flight guard — mark BEFORE posting so concurrent
                        # threads (if any) can't also post
                        if seen.is_seen(video_id, author_id):
                            log("INFO", f"  Raced — skipping {author_id[:14]}.")
                            continue
                        seen.mark(video_id, author_id)

                        youtube.comments().insert(
                            part="snippet",
                            body={"snippet": {"parentId": comment_id, "textOriginal": reply_text}}
                        ).execute()
                        tracker.consume(50)
                        log("SUCCESS", f"  Replied '{reply_text[:60]}' to {author_id[:14]}…")

                except HttpError as ex:
                    status = getattr(ex.resp, "status", None)
                    log("ERROR", f"YouTube API error on {video_id}: {ex}")
                    if status == 403:
                        log("WARNING", "403 Forbidden — sleeping 1 hour.")
                        time.sleep(3600)
                        break
                except Exception as ex:
                    log("ERROR", f"Unexpected error on {video_id}: {ex}")

                time.sleep(random.randint(3, 6))

            poll_interval = settings.get("poll_interval", 120)
            log("INFO", f"Cycle done. Next in {poll_interval}s. Quota used: {tracker.used}/{QUOTA_HARD_STOP}")
            time.sleep(poll_interval)

        except Exception as outer:
            log("ERROR", f"Daemon crash: {outer}")
            time.sleep(60)

# Start daemon immediately (daemon=True so it stops when main process exits)
threading.Thread(target=polling_daemon, daemon=True, name="YTPoller").start()

# ── Flask Routes ──────────────────────────────────────────────────────────────
# ── OAuth redirect URI helper ──────────────────────────────────────────
def get_oauth_redirect_uri() -> str:
    """Build the OAuth redirect URI.
    Hardcoded for Railway deployment, localhost for local dev.
    """
    # Detect Railway environment by checking Railway-specific env vars
    is_railway = bool(
        os.environ.get("RAILWAY_ENVIRONMENT") or
        os.environ.get("RAILWAY_SERVICE_ID") or
        os.environ.get("RAILWAY_PROJECT_ID")
    )
    if is_railway:
        # Fixed Railway deployment URL — update this if your Railway URL changes
        return "https://whatsappbot-production-d81c.up.railway.app/yt/"
    # Also support dynamic detection for flexibility
    public_domain = (
        os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip() or
        os.environ.get("RAILWAY_STATIC_URL", "").strip()
    )
    if public_domain:
        if "://" in public_domain:
            public_domain = public_domain.split("://", 1)[1].rstrip("/")
        return f"https://{public_domain}/yt/"
    # Local development fallback
    return "http://localhost:8080/"


@app.route("/")
def index():
    if "code" in request.args:
        try:
            cf = find_client_secrets_file()
            redirect_uri = get_oauth_redirect_uri()
            flow = Flow.from_client_secrets_file(cf, scopes=SCOPES, redirect_uri=redirect_uri)
            flow.fetch_token(authorization_response=request.url)
            with open(TOKEN_FILE, "w", encoding="utf-8") as f:
                f.write(flow.credentials.to_json())
            log("SUCCESS", "Google account linked successfully.")
            return redirect("/yt/" if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_STATIC_URL") or os.environ.get("RAILWAY_PUBLIC_DOMAIN") else "/")
        except Exception as e:
            log("ERROR", f"OAuth callback error: {e}")
            return f"Authorization failed: {e}", 400

    youtube = get_youtube_client()
    return render_template("index.html", authenticated=(youtube is not None))

@app.route("/auth/start")
def auth_start():
    try:
        cf = find_client_secrets_file()
        redirect_uri = get_oauth_redirect_uri()
        flow = Flow.from_client_secrets_file(cf, scopes=SCOPES, redirect_uri=redirect_uri)
        auth_url, state = flow.authorization_url(access_type="offline", prompt="consent")
        session["state"] = state
        return redirect(auth_url)
    except Exception as e:
        log("ERROR", f"OAuth init failed: {e}")
        return f"Cannot start authentication: {e}", 500

@app.route("/api/status")
def api_status():
    tracker  = QuotaTracker()
    settings = load_settings()
    youtube  = get_youtube_client()
    with _log_lock:
        logs = list(logs_history)
    return jsonify({
        "authenticated":       youtube is not None,
        "bot_running":         settings.get("bot_running", False),
        "poll_interval":       settings.get("poll_interval", 120),
        "reply_delay_min":     settings.get("reply_delay_min", 8),
        "reply_delay_max":     settings.get("reply_delay_max", 20),
        "max_results":         settings.get("max_results", 20),
        "quota_used":          tracker.used,
        "quota_remaining":     tracker.remaining,
        "quota_hard_stop":     QUOTA_HARD_STOP,
        "quota_stopped":       tracker.hard_stop_reached,
        "automated_video_ids": settings.get("automated_video_ids", []),
        "video_configs":       settings.get("video_configs", {}),
        "logs":                logs
    })

@app.route("/api/bot/control", methods=["POST"])
def api_bot_control():
    data = request.json or {}
    settings = load_settings()
    settings["bot_running"] = bool(data.get("bot_running", False))
    save_settings(settings)
    state = "STARTED" if settings["bot_running"] else "STOPPED"
    log("INFO", f"Bot {state} by user.")
    return jsonify({"success": True, "bot_running": settings["bot_running"]})

@app.route("/api/settings", methods=["POST"])
def api_settings():
    data = request.json or {}
    settings = load_settings()
    if "poll_interval"        in data: settings["poll_interval"]       = max(10, int(data["poll_interval"]))
    if "max_results"          in data: settings["max_results"]         = max(1, min(50, int(data["max_results"])))
    if "reply_delay_min"      in data: settings["reply_delay_min"]     = max(1, int(data["reply_delay_min"]))
    if "reply_delay_max"      in data: settings["reply_delay_max"]     = max(settings["reply_delay_min"], int(data["reply_delay_max"]))
    if "automated_video_ids"  in data: settings["automated_video_ids"] = data["automated_video_ids"]
    if "video_configs"        in data: settings["video_configs"]        = data["video_configs"]
    save_settings(settings)
    log("INFO", "Settings saved.")
    return jsonify({"success": True, "settings": settings})

@app.route("/api/videos")
def api_videos():
    youtube = get_youtube_client()
    if not youtube:
        return jsonify({"error": "Unauthorized"}), 401
    tracker = QuotaTracker()
    try:
        ch = youtube.channels().list(part="contentDetails", mine=True).execute()
        tracker.consume(1)
        if not ch.get("items"):
            return jsonify([])
        uploads_pl = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        pl = youtube.playlistItems().list(part="snippet", playlistId=uploads_pl, maxResults=50).execute()
        tracker.consume(1)
        settings  = load_settings()
        automated = settings.get("automated_video_ids", [])
        configs   = settings.get("video_configs", {})
        videos = []
        for item in pl.get("items", []):
            sn     = item.get("snippet", {})
            vid    = sn.get("resourceId", {}).get("videoId")
            thumbs = sn.get("thumbnails", {})
            thumb  = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
            cfg    = configs.get(vid, {"rules": [], "default_reply": ""})
            has_rules = bool(
                cfg.get("rules")
                or cfg.get("default_reply")
                or (cfg.get("any_comment_enabled") and cfg.get("any_comment_reply", "").strip())
            )
            videos.append({
                "id":           vid,
                "title":        sn.get("title", ""),
                "thumbnail":    thumb,
                "published_at": sn.get("publishedAt", ""),
                "automated":    vid in automated,
                "has_rules":    has_rules,
                "config":       cfg
            })
        return jsonify(videos)
    except Exception as e:
        log("ERROR", f"Video list error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/seen/clear/<video_id>", methods=["POST"])
def api_seen_clear(video_id):
    """Remove all seen-author entries for a specific video so they can receive replies again."""
    seen = SeenAuthors()
    prefix = f"{video_id}::"
    before = len(seen.seen)
    seen.seen = {k for k in seen.seen if not k.startswith(prefix)}
    # Also clear from in-memory layer
    with _memory_lock:
        _replied_memory.difference_update({k for k in list(_replied_memory) if k.startswith(prefix)})
    seen._save()
    removed = before - len(seen.seen)
    log("INFO", f"Cleared {removed} seen-author entries for video {video_id}.")
    return jsonify({"success": True, "removed": removed, "remaining": len(seen.seen)})

@app.route("/api/seen/count/<video_id>")
def api_seen_count(video_id):
    """Return how many unique authors have already been replied to for a video."""
    seen = SeenAuthors()
    prefix = f"{video_id}::"
    count = sum(1 for k in seen.seen if k.startswith(prefix))
    return jsonify({"video_id": video_id, "seen_count": count})

if __name__ == "__main__":
    port = int(os.environ.get("FLASK_PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
