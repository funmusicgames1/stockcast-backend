"""
notify.py — Sends push notifications to all subscribed devices
after the daily prediction pipeline completes.

Uses Firebase Cloud Messaging V1 API with service account authentication.

Environment variables needed in .env and Railway:
  FIREBASE_PROJECT_ID=stockcast-959fc
  FIREBASE_CLIENT_EMAIL=firebase-adminsdk-fbsvc@stockcast-959fc.iam.gserviceaccount.com
  FIREBASE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n
"""

import os
import json
import time
import logging
import urllib.request
import urllib.error
import urllib.parse
import base64

logger = logging.getLogger(__name__)

FIREBASE_PROJECT_ID   = os.getenv("FIREBASE_PROJECT_ID", "stockcast-959fc")
FIREBASE_CLIENT_EMAIL = os.getenv("FIREBASE_CLIENT_EMAIL", "")
FIREBASE_PRIVATE_KEY  = os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n")
FIREBASE_TOPIC = "daily_predictions"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _make_jwt() -> str:
    now = int(time.time())
    header  = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({
        "iss": FIREBASE_CLIENT_EMAIL,
        "sub": FIREBASE_CLIENT_EMAIL,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
        "scope": "https://www.googleapis.com/auth/firebase.messaging",
    }).encode())

    signing_input = f"{header}.{payload}".encode()

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    private_key = serialization.load_pem_private_key(
        FIREBASE_PRIVATE_KEY.encode(), password=None
    )
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{payload}.{_b64url(signature)}"


def _get_access_token() -> str:
    jwt = _make_jwt()
    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt,
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read().decode())
        return result["access_token"]


def send_prediction_notification(winners: list, losers: list) -> bool:
    if not FIREBASE_CLIENT_EMAIL or not FIREBASE_PRIVATE_KEY:
        logger.warning("Firebase credentials not set — skipping push notification.")
        return False

    top_winner = winners[0] if winners else None
    top_loser  = losers[0]  if losers  else None

    if top_winner and top_loser:
        body = (
            f"\U0001f4c8 {top_winner['ticker']} +{top_winner['predicted_change_pct']}%  "
            f"\U0001f4c9 {top_loser['ticker']} {top_loser['predicted_change_pct']}%"
        )
    elif top_winner:
        body = f"\U0001f4c8 Top pick: {top_winner['ticker']} +{top_winner['predicted_change_pct']}%"
    else:
        body = "Today's stock predictions are ready."

    try:
        access_token = _get_access_token()
    except Exception as e:
        logger.error(f"Failed to get Firebase access token: {e}")
        return False

    fcm_url = f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send"

    payload = json.dumps({
        "message": {
            "topic": FIREBASE_TOPIC,
            "notification": {
                "title": "StockCast \u2014 Today\u2019s Predictions Are Ready",
                "body": body,
            },
            "apns": {
                "payload": {
                    "aps": {
                        "alert": {
                            "title": "StockCast \u2014 Today\u2019s Predictions Are Ready",
                            "body": body,
                        },
                        "sound": "default",
                        "badge": 1,
                    }
                }
            },
            "data": {"type": "daily_predictions"}
        }
    }).encode()

    try:
        req = urllib.request.Request(
            fcm_url,
            data=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            logger.info(f"Push notification sent: {result.get('name', 'ok')}")
            return True

    except urllib.error.HTTPError as e:
        logger.error(f"FCM error {e.code}: {e.read().decode()}")
        return False
    except Exception as e:
        logger.error(f"Failed to send push notification: {e}")
        return False
