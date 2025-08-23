from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import uuid
from ..database import execute_query

router = APIRouter()

class CreateUserRequest(BaseModel):
    role: str
    wallet_address: str

class UserResponse(BaseModel):
    user_id: int
    wallet_address: str
    role: str
    created_at: datetime
    updated_at: datetime

@router.post("/users", response_model=UserResponse)
async def create_user(user_data: CreateUserRequest):
    """Create a new user with role and wallet address"""
    print(f"🔍 Creating user with role: {user_data.role} and wallet: {user_data.wallet_address}")
    
    try:
        # Insert user into database with provided wallet address and role
        insert_query = """
        INSERT INTO users (wallet_address, role)
        OUTPUT INSERTED.user_id, INSERTED.wallet_address, INSERTED.role, 
               INSERTED.created_at, INSERTED.updated_at
        VALUES (?, ?)
        """
        
        print(f"🔍 Executing query: {insert_query}")
        print(f"🔍 With parameters: {(user_data.wallet_address, user_data.role)}")
        
        result = execute_query(
            insert_query, 
            (user_data.wallet_address, user_data.role),
            fetch='one'
        )
        
        print(f"🔍 Query result: {result}")
        print(f"🔍 Result type: {type(result)}")
        print(f"🔍 Result length: {len(result) if result else 'None'}")
        
        if result and len(result) >= 5:
            # Successfully inserted into database
            user_response = UserResponse(
                user_id=result[0],
                wallet_address=result[1],
                role=result[2],
                created_at=result[3],
                updated_at=result[4]
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
        WHERE user_id = ?
        """
        
        result = execute_query(select_query, (user_id,), fetch='one')
        
        if result:
            return UserResponse(
                user_id=result[0],
                wallet_address=result[1],
                role=result[2],
                created_at=result[3],
                updated_at=result[4]
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
        delete_query = "DELETE FROM users WHERE user_id = ?"
        
        result = execute_query(delete_query, (user_id,), fetch=False)
        
        if result > 0:
            return {"message": f"User {user_id} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
            
    except Exception as e:
        print(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/users/cleanup/incomplete")
async def cleanup_incomplete_users():
    """Clean up users with temporary wallet addresses (incomplete registrations)"""
    try:
        # Delete users with temp wallet addresses older than 24 hours
        cleanup_query = """
        DELETE FROM users 
        WHERE wallet_address LIKE 'temp_%' 
        AND created_at < DATEADD(hour, -24, GETDATE())
        """
        
        result = execute_query(cleanup_query, fetch=False)
        
        return {"message": f"Cleaned up {result} incomplete user registrations"}
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
