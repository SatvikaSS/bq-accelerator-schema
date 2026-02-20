from typing import Dict
from fastapi import Request
from jose import jwt


def extract_user_identity(request: Request, payload: Dict) -> str:
    """
    Extract user identity from:
    1. Cloud Run IAM header
    2. JWT token
    3. Payload
    4. Fallback to anonymous
    """

    # Cloud Run IAM header
    user_email = request.headers.get("X-Goog-Authenticated-User-Email")
    if user_email:
        return user_email.split(":")[-1]

    # JWT fallback
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            return decoded.get("email") or decoded.get("sub") or "unknown_user"
        except Exception:
            pass

    # Payload fallback
    if payload.get("user_id"):
        return payload["user_id"]

    return "anonymous"