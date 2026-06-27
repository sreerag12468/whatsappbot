import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except (ModuleNotFoundError, ImportError) as exc:
    missing_message = str(exc)
    print("\nMissing Google API dependencies. Install them with:")
    print("  pip install google-api-python-client google-auth-oauthlib\n")
    print("Error details:", missing_message)
    sys.exit(1)

from config import (
    MAX_RESULTS,
    POLL_INTERVAL,
    REPLY_DELAY_MAX,
    REPLY_DELAY_MIN,
    REPLY_TEMPLATES,
    VIDEO_IDS,
)

TOKEN_FILE = "token.json"
SEEN_FILE = "seen_comments.json"
QUOTA_FILE = "quota_log.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
DAILY_QUOTA = 10000

def find_client_secrets_file() -> str:
    # 1. Check current working directory
    candidate = "client_secrets.json"
    if os.path.exists(candidate):
        return candidate

    for name in os.listdir('.'):
        if name.lower().endswith('.json') and 'client_secret' in name.lower():
            return name

    # 2. Check main.py's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidate_in_script_dir = os.path.join(script_dir, "client_secrets.json")
    if os.path.exists(candidate_in_script_dir):
        return candidate_in_script_dir

    for name in os.listdir(script_dir):
        if name.lower().endswith('.json') and 'client_secret' in name.lower():
            return os.path.join(script_dir, name)

    raise FileNotFoundError(
        "No OAuth client secrets JSON file found in the current directory or script directory. "
        "Place it next to main.py as client_secrets.json or with a name starting with client_secret_."
    )

logger = logging.getLogger("yt_bot")
logger.setLevel(logging.INFO)


def setup_logger() -> None:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("bot.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


class QuotaTracker:
    def __init__(self, filename: str = QUOTA_FILE) -> None:
        self.filename = filename
        self.data = {
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "used": 0,
        }
        self._load()
        self._reset_if_needed()

    def _load(self) -> None:
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as fh:
                    self.data = json.load(fh)
            except (ValueError, OSError) as exc:
                logger.warning("Could not load quota log: %s", exc)
                self.data = {"date": datetime.utcnow().strftime("%Y-%m-%d"), "used": 0}

    def _save(self) -> None:
        try:
            with open(self.filename, "w", encoding="utf-8") as fh:
                json.dump(self.data, fh, indent=2)
        except OSError as exc:
            logger.error("Failed to save quota log: %s", exc)

    def _reset_if_needed(self) -> None:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if self.data.get("date") != today:
            logger.info("Resetting quota tracker for new day.")
            self.data = {"date": today, "used": 0}
            self._save()

    def consume(self, units: int) -> None:
        self._reset_if_needed()
        self.data["used"] = self.data.get("used", 0) + units
        self._save()
        logger.debug("Consumed %s quota units. Used today: %s", units, self.data["used"])

    def remaining(self) -> int:
        self._reset_if_needed()
        return max(0, DAILY_QUOTA - self.data.get("used", 0))

    def can_post(self) -> bool:
        return self.remaining() >= 51


class SeenComments:
    def __init__(self, filename: str = SEEN_FILE) -> None:
        self.filename = filename
        self.seen = set()
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as fh:
                    items = json.load(fh)
                    self.seen = set(items)
            except (ValueError, OSError) as exc:
                logger.warning("Could not load seen comments: %s", exc)
                self.seen = set()

    def _save(self) -> None:
        try:
            with open(self.filename, "w", encoding="utf-8") as fh:
                json.dump(sorted(self.seen), fh, indent=2)
        except OSError as exc:
            logger.error("Failed to save seen comments: %s", exc)

    def is_seen(self, comment_id: str) -> bool:
        return comment_id in self.seen

    def mark(self, comment_id: str) -> None:
        self.seen.add(comment_id)
        self._save()
        logger.debug("Marked comment as seen: %s", comment_id)


def authenticate() -> object:
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as exc:
            logger.warning("Could not read token file: %s", exc)
            creds = None

    # Try to load from youtube_token.pickle if it exists in the directory or parent directory
    pickle_file = "youtube_token.pickle"
    parent_pickle = os.path.join("..", pickle_file)
    target_pickle = None
    if os.path.exists(pickle_file):
        target_pickle = pickle_file
    elif os.path.exists(parent_pickle):
        target_pickle = parent_pickle

    if not creds and target_pickle:
        import pickle
        try:
            with open(target_pickle, "rb") as pf:
                creds = pickle.load(pf)
            logger.info("Loaded credentials from %s", target_pickle)
            
            # Check if scopes are sufficient
            missing_scopes = [s for s in SCOPES if s not in (creds.scopes or [])]
            if missing_scopes:
                logger.warning(
                    "Token in %s has insufficient scopes. Required: %s, Found: %s. "
                    "This token cannot be used for comment replies. Falling back to new authentication.",
                    target_pickle, SCOPES, creds.scopes
                )
                creds = None
        except Exception as exc:
            logger.warning("Could not read pickle file %s: %s", target_pickle, exc)
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(TOKEN_FILE, "w", encoding="utf-8") as token:
                token.write(creds.to_json())
            logger.info("Refreshed OAuth token silently.")
        except Exception as exc:
            logger.warning("Failed to refresh token: %s", exc)
            creds = None

    if not creds or not creds.valid:
        client_secrets_file = find_client_secrets_file()

        if not os.path.exists(client_secrets_file):
            logger.error(
                "Missing %s. Create OAuth credentials at console.developers.google.com and save the file next to main.py.",
                client_secrets_file,
            )
            raise FileNotFoundError(f"Missing {client_secrets_file}")

        # Detect port from redirect URIs if it is a 'web' client application
        import re
        local_port = 0
        try:
            with open(client_secrets_file, "r", encoding="utf-8") as sf:
                secrets_data = json.load(sf)
            # Check for "web" configuration
            web_config = secrets_data.get("web", {})
            if web_config:
                redirect_uris = web_config.get("redirect_uris", [])
                for uri in redirect_uris:
                    # Match localhost with port, e.g. http://localhost:8080/
                    port_match = re.search(r"localhost:(\d+)", uri)
                    if port_match:
                        local_port = int(port_match.group(1))
                        logger.info("Detected local port %s from web client redirect URIs.", local_port)
                        break
        except Exception as ex:
            logger.warning("Could not parse client secrets to extract port: %s", ex)

        logger.info("Requesting new OAuth credentials. Opening browser window for verification...")
        flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
        
        if local_port > 0:
            logger.info("Running OAuth flow local server on port %s to match redirect URI...", local_port)
            creds = flow.run_local_server(port=local_port)
        else:
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
        logger.info("Saved new OAuth token to %s.", TOKEN_FILE)

    youtube = build("youtube", "v3", credentials=creds)
    return youtube


def get_reply(text: str) -> str:
    cleaned = text.lower()
    for keyword, templates in REPLY_TEMPLATES.items():
        if keyword == "default":
            continue
        if keyword in cleaned:
            return random.choice(templates)
    return random.choice(REPLY_TEMPLATES.get("default", ["Thanks for watching! 🙏"]))


def fetch_comments(youtube, video_id: str, tracker: QuotaTracker) -> list:
    try:
        response = (
            youtube.commentThreads()
            .list(
                part="snippet",
                videoId=video_id,
                order="time",
                maxResults=MAX_RESULTS,
                textFormat="plainText",
            )
            .execute()
        )
        tracker.consume(1)
        items = response.get("items", [])
        logger.info("Fetched %s comments for video %s.", len(items), video_id)
        return items
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        logger.error("Failed to fetch comments for %s: %s", video_id, exc)
        if status == 403:
            logger.warning("Quota or permission problem detected. Sleeping for 1 hour.")
            time.sleep(3600)
        return []


def post_reply(youtube, comment_id: str, reply_text: str, tracker: QuotaTracker) -> bool:
    try:
        youtube.comments().insert(
            part="snippet",
            body={
                "snippet": {
                    "parentId": comment_id,
                    "textOriginal": reply_text,
                }
            },
        ).execute()
        tracker.consume(50)
        logger.info("Posted reply to comment %s.", comment_id)
        return True
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        logger.error("Failed to post reply to %s: %s", comment_id, exc)
        if status == 403:
            logger.warning("Quota exceeded or forbidden. Sleeping for 1 hour.")
            time.sleep(3600)
        elif status in {429, 500, 503}:
            delay = random.randint(30, 60)
            logger.warning("Rate limit or server error. Sleeping for %s seconds.", delay)
            time.sleep(delay)
        return False


def monitor(youtube, tracker: QuotaTracker, seen: SeenComments) -> None:
    for video_id in VIDEO_IDS:
        comments = fetch_comments(youtube, video_id, tracker)

        for item in comments:
            top_comment = item.get("snippet", {}).get("topLevelComment", {})
            snippet = top_comment.get("snippet", {})
            comment_id = top_comment.get("id")
            text = snippet.get("textOriginal", "")

            if not comment_id:
                continue

            if seen.is_seen(comment_id):
                continue

            if not tracker.can_post():
                logger.warning(
                    "Quota too low to reply. Marking comment %s as seen to avoid retry today.", comment_id
                )
                seen.mark(comment_id)
                continue

            delay = random.randint(REPLY_DELAY_MIN, REPLY_DELAY_MAX)
            logger.info("Waiting %s seconds before replying to %s.", delay, comment_id)
            time.sleep(delay)

            reply_text = get_reply(text)
            logger.info("Replying to %s with: %s", comment_id, reply_text)
            success = post_reply(youtube, comment_id, reply_text, tracker)

            if success:
                seen.mark(comment_id)
            else:
                logger.warning("Reply failed for %s. Will retry later if quota allows.", comment_id)

        between_delay = random.randint(2, 4)
        logger.info("Waiting %s seconds before checking next video.", between_delay)
        time.sleep(between_delay)


def run() -> None:
    from app import app
    print("*" * 60)
    print(" [LAUNCH] YOUTUBE COMMENTATOR BOT DASHBOARD")
    print("   Running locally on: http://localhost:8080/")
    print("*" * 60)
    app.run(host="localhost", port=8080, debug=False)


if __name__ == "__main__":
    run()


# DEPLOYMENT STEPS:
# 1. Go to pythonanywhere.com and create free account
# 2. Open Bash console
# 3. Run: pip install --user google-api-python-client google-auth-oauthlib
# 4. Upload all files via Files tab
# 5. Run locally first to complete OAuth login: python main.py
# 6. Upload the generated token.json to PythonAnywhere
# 7. Go to Tasks tab → Always-on task
# 8. Command: python /home/yourusername/yt-bot/main.py
# 9. Bot runs 24/7 automatically
