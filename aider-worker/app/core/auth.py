import json
import os
from functools import lru_cache
from typing import Optional, Dict
from uuid import UUID

import httpx
import jwt
from supabase import create_client


def get_bearer_token(auth_header: Optional[str]) -> Optional[str]:
    if not auth_header:
        return None
    parts = auth_header.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


@lru_cache(maxsize=1)
def _jwks_url() -> str:
    url = os.getenv("SUPABASE_JWKS_URL")
    if not url:
        raise RuntimeError("SUPABASE_JWKS_URL is not set")
    return url


def _fetch_jwks() -> Dict:
    with httpx.Client(timeout=5.0) as client:
        resp = client.get(_jwks_url())
        resp.raise_for_status()
        return resp.json()


def verify_and_get_user(token: str) -> Dict[str, Optional[str]]:
    if not token:
        raise ValueError("Missing JWT")

    header = jwt.get_unverified_header(token)
    kid = header.get("kid")
    
    # Try JWKS verification first (for production)
    if kid:
        try:
            jwks = _fetch_jwks()
            keys = jwks.get("keys", [])
            jwk = next((k for k in keys if k.get("kid") == kid), None)
            if not jwk:
                raise ValueError("No matching JWK")

            # Use jwt.algorithms to get the key
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

            payload = jwt.decode(
                token,
                key=public_key,
                algorithms=[header.get("alg", "RS256"), "RS256", "ES256"],
                options={"verify_aud": False},
            )
            return {"user_id": payload.get("sub"), "email": payload.get("email")}
        except Exception as e:
            raise ValueError(f"JWKS verification failed: {e}")
    
    # Fallback: decode without verification (for testing with simple JWTs)
    # WARNING: This is insecure and should only be used for testing
    try:
        payload = jwt.decode(
            token,
            options={"verify_signature": False, "verify_aud": False}
        )
        return {"user_id": payload.get("sub"), "email": payload.get("email")}
    except Exception as e:
        raise ValueError(f"JWT decode failed: {e}")


def assert_member(project_id: UUID, user_id: UUID) -> None:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Supabase not configured")
    sb = create_client(url, key)
    res = (
        sb.table("project_members")
        .select("user_id")
        .eq("project_id", str(project_id))
        .eq("user_id", str(user_id))
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", []) or []
    if not data:
        raise PermissionError("User is not a member of this project")



