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
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING insurance_id, user_id
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
            insurance_id, user_id = row["insurance_id"], row["user_id"]
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

class InsuranceListItem(BaseModel):
    insurance_id: int
    company_name: str
    company_type: str
    website_url: Optional[str] = None
    claim_settlement_ratio: Optional[float] = None
    claims_email: Optional[str] = None
    claims_phone: Optional[str] = None
    logo_url: Optional[str] = None

class InsuranceListResponse(BaseModel):
    items: list[InsuranceListItem]
    total: int

@router.get("/insurance/list", response_model=InsuranceListResponse)
async def list_insurances():
    """Return a concise list of insurances for patient selection while applying claims."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT i.insurance_id,
                       i.company_name,
                       i.company_type,
                       i.website_url,
                       i.logo_url,
                       b.claim_settlement_ratio,
                       c.claims_email,
                       c.claims_phone
                FROM insurance_basic_info i
                LEFT JOIN insurance_business_info b ON b.insurance_id = i.insurance_id
                LEFT JOIN insurance_contact_tech c ON c.insurance_id = i.insurance_id
                ORDER BY i.company_name
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            items = [
                InsuranceListItem(
                    insurance_id=r["insurance_id"],
                    company_name=r["company_name"],
                    company_type=r["company_type"],
                    website_url=r.get("website_url"),
                    logo_url=r.get("logo_url"),
                    claim_settlement_ratio=r.get("claim_settlement_ratio"),
                    claims_email=r.get("claims_email"),
                    claims_phone=r.get("claims_phone"),
                ) for r in rows
            ]
            return InsuranceListResponse(items=items, total=len(items))
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list insurances: {str(e)}")

class InsuranceByUserResponse(BaseModel):
    insurance_id: int
    user_id: int
    company_name: str

@router.get("/insurance/by-user/{user_id}", response_model=InsuranceByUserResponse)
async def get_insurance_by_user(user_id: int):
    """Resolve insurance_id and basic info by user_id (used by insurance dashboard bootstrap)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT insurance_id, user_id, company_name
                FROM insurance_basic_info
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Insurance not found for user")
            return InsuranceByUserResponse(
                insurance_id=row["insurance_id"],
                user_id=row["user_id"],
                company_name=row["company_name"],
            )
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resolve insurance by user: {str(e)}")
