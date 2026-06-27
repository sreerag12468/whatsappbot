VIDEO_IDS = ["VIDEO_ID_1", "VIDEO_ID_2"]  # list of your video IDs

POLL_INTERVAL = 120        # poll every 2 minutes
MAX_RESULTS = 10            # max comments to fetch per poll
REPLY_DELAY_MIN = 8         # min seconds before replying (looks natural)
REPLY_DELAY_MAX = 20        # max seconds before replying

REPLY_TEMPLATES = {
    "thank": ["You're welcome!", "Happy to help!", "Glad it helped! 😊"],
    "great": ["Thank you so much!", "Appreciate it! 🙏"],
    "good": ["Thank you! Glad you liked it!"],
    "love": ["That means a lot, thank you! ❤️"],
    "how": ["Check the description for more details!", "Covering that in the next video!"],
    "help": ["Happy to help! Let me know if you have more questions."],
    "nice": ["Thank you! 😊", "Appreciate it!"],
    "best": ["Thank you so much! 🙏"],
    "when": ["Stay tuned, coming soon!", "Subscribe to get notified!"],
    "default": [
        "Thanks for watching! 🙏",
        "Appreciate the comment! 😊",
        "Thank you! ❤️",
        "Thanks for the support!",
    ],
}
