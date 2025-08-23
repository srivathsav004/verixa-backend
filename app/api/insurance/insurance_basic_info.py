from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class InsuranceBasicInfoRequest(BaseModel):
    user_id: int
    company_name: str
    company_type: str
    insurance_license_number: str
    registration_number: str
    established_year: Optional[int] = None
    website_url: Optional[str] = None

class InsuranceBasicInfoResponse(BaseModel):
    insurance_id: int
    user_id: int
    message: str

@router.post("/insurance/basic-info", response_model=InsuranceBasicInfoResponse)
async def create_insurance_basic_info(data: InsuranceBasicInfoRequest):
    """Create insurance basic info and return insurance_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
            INSERT INTO insurance_basic_info (
                user_id, company_name, company_type, insurance_license_number, registration_number,
                established_year, website_url
            )
            OUTPUT INSERTED.insurance_id, INSERTED.user_id
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(
                insert_query,
                (
                    data.user_id,
                    data.company_name,
                    data.company_type,
                    data.insurance_license_number,
                    data.registration_number,
                    data.established_year,
                    data.website_url,
                ),
            )
            row = cursor.fetchone()
            conn.commit()
            if not row:
                raise HTTPException(status_code=500, detail="Failed to create insurance basic info")
            insurance_id, user_id = row
            return InsuranceBasicInfoResponse(
                insurance_id=insurance_id,
                user_id=user_id,
                message="Insurance basic info created successfully",
            )
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
