import os
import requests
from dotenv import load_dotenv

load_dotenv(r'c:\Users\sreer\OneDrive\Desktop\ttsaccessing\.env')

page_token = os.getenv("PAGE_ACCESS_TOKEN")
page_id = os.getenv("PAGE_ID")
ig_user_id = os.getenv("IG_USER_ID")

recipient_id = "691553176799246"  # winikek's ID from logs

print("Page ID:", page_id)
print("IG User ID:", ig_user_id)
print("Page Token starts with:", page_token[:15] if page_token else "None")

# Test 1: Using IG_USER_ID
url_ig = f"https://graph.facebook.com/v19.0/{ig_user_id}/messages"
payload = {"recipient": {"id": recipient_id}, "message": {"text": "Test from IG_USER_ID"}}
try:
    resp = requests.post(url_ig, params={"access_token": page_token}, json=payload)
    print("Test 1 (IG_USER_ID) Response:", resp.status_code, resp.json())
except Exception as e:
    print("Test 1 Exception:", e)

# Test 2: Using PAGE_ID
url_page = f"https://graph.facebook.com/v19.0/{page_id}/messages"
payload = {"recipient": {"id": recipient_id}, "message": {"text": "Test from PAGE_ID"}}
try:
    resp = requests.post(url_page, params={"access_token": page_token}, json=payload)
    print("Test 2 (PAGE_ID) Response:", resp.status_code, resp.json())
except Exception as e:
    print("Test 2 Exception:", e)

# Test 3: Using me
url_me = f"https://graph.facebook.com/v19.0/me/messages"
payload = {"recipient": {"id": recipient_id}, "message": {"text": "Test from /me"}}
try:
    resp = requests.post(url_me, params={"access_token": page_token}, json=payload)
    print("Test 3 (/me) Response:", resp.status_code, resp.json())
except Exception as e:
    print("Test 3 Exception:", e)
