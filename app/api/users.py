from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import uuid
from ..database import execute_query
import hashlib
import secrets

router = APIRouter()

class CreateUserRequest(BaseModel):
    role: str
    wallet_address: str
    password: str

class UserResponse(BaseModel):
    user_id: int
    wallet_address: str
    role: str
    created_at: datetime
    updated_at: datetime

def _hash_password(password: str) -> str:
    """Derive a secure hash using PBKDF2-HMAC with a random salt.
    Format: pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>
    """
    iterations = 200_000
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


@router.post("/users", response_model=UserResponse)
async def create_user(user_data: CreateUserRequest):
    """Create a new user with role and wallet address"""
    print(f"🔍 Creating user with role: {user_data.role} and wallet: {user_data.wallet_address}")
    
    try:
        password_hash = _hash_password(user_data.password)
        # Insert user into database with provided wallet address and role
        insert_query = """
        INSERT INTO users (wallet_address, role, password_hash)
        VALUES (%s, %s, %s)
        RETURNING user_id, wallet_address, role, created_at, updated_at
        """
        
        print(f"🔍 Executing query: {insert_query}")
        print(f"🔍 With parameters: {(user_data.wallet_address, user_data.role, '***hash***')}")
        
        result = execute_query(
            insert_query, 
            (user_data.wallet_address, user_data.role, password_hash),
            fetch='one'
        )
        
        print(f"🔍 Query result: {result}")
        print(f"🔍 Result type: {type(result)}")
        # Result is a dict due to RealDictCursor
        
        if result and all(k in result for k in ("user_id", "wallet_address", "role", "created_at", "updated_at")):
            # Successfully inserted into database
            user_response = UserResponse(
                user_id=result["user_id"],
                wallet_address=result["wallet_address"],
                role=result["role"],
                created_at=result["created_at"],
                updated_at=result["updated_at"]
            )
            print(f"✅ User created successfully: {user_response}")
            return user_response
        else:
            # Database insertion failed
            print(f"❌ Database insertion failed - invalid result: {result}")
            raise HTTPException(
                status_code=500, 
                detail="Failed to insert user into database"
            )
            
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Handle any other database or connection errors
        print(f"❌ Database error creating user: {e}")
        print(f"❌ Error type: {type(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Database connection or query error: {str(e)}"
        )

@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int):
    """Get user by ID"""
    try:
        select_query = """
        SELECT user_id, wallet_address, role, created_at, updated_at
        FROM users 
        WHERE user_id = %s
        """
        
        result = execute_query(select_query, (user_id,), fetch='one')
        
        if result:
            return UserResponse(
                user_id=result["user_id"],
                wallet_address=result["wallet_address"],
                role=result["role"],
                created_at=result["created_at"],
                updated_at=result["updated_at"]
            )
        else:
            raise HTTPException(status_code=404, detail="User not found")
            
    except Exception as e:
        print(f"Error fetching user: {e}")
        return {"status": "error", "message": f"Database error: {str(e)}"}

@router.delete("/users/{user_id}")
async def delete_user(user_id: int):
    """Delete a user (for cleanup of incomplete registrations)"""
    try:
        delete_query = "DELETE FROM users WHERE user_id = %s"
        
        result = execute_query(delete_query, (user_id,), fetch=False)
        
        if result > 0:
            return {"message": f"User {user_id} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
            
    except Exception as e:
        print(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

