from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..database import execute_query
import hashlib

router = APIRouter()

class LoginRequest(BaseModel):
    wallet_address: str
    password: str

class LoginResponse(BaseModel):
    user_id: int
    wallet_address: str
    role: str


def _verify_password(password: str, stored: str) -> bool:
    """Verify a password against stored format pbkdf2_sha256$iterations$salt_hex$hash_hex"""
    try:
        algo, iter_str, salt_hex, hash_hex = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iter_str)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return dk == expected
    except Exception:
        return False


@router.post("/login", response_model=LoginResponse)
async def login(credentials: LoginRequest):
    """Authenticate a user by wallet address and password and return role."""
    try:
        select_query = (
            "SELECT user_id, wallet_address, role, password_hash FROM users "
            "WHERE LOWER(wallet_address) = LOWER(?)"
        )
        row = execute_query(select_query, (credentials.wallet_address,), fetch='one')

        if not row:
            raise HTTPException(status_code=401, detail="Invalid wallet or password")

        user_id, wallet_address, role, password_hash = row

        if not password_hash or not _verify_password(credentials.password, password_hash):
            raise HTTPException(status_code=401, detail="Invalid wallet or password")

        return LoginResponse(user_id=user_id, wallet_address=wallet_address, role=role)

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Login error: {e}")
        raise HTTPException(status_code=500, detail="Server error during login")
