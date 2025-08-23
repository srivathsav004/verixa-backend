from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class InsuranceBusinessInfoRequest(BaseModel):
    insurance_id: int
    annual_premium_collection: Optional[float] = None
    active_policies: Optional[int] = None
    coverage_areas: Optional[str] = None
    specialization: Optional[str] = None
    claim_settlement_ratio: Optional[float] = None

class InsuranceBusinessInfoResponse(BaseModel):
    business_id: int
    insurance_id: int
    message: str

@router.post("/insurance/business-info", response_model=InsuranceBusinessInfoResponse)
async def create_insurance_business_info(data: InsuranceBusinessInfoRequest):
    """Create insurance business info and return business_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
            INSERT INTO insurance_business_info (
                insurance_id, annual_premium_collection, active_policies,
                coverage_areas, specialization, claim_settlement_ratio
            )
            OUTPUT INSERTED.business_id, INSERTED.insurance_id
            VALUES (?, ?, ?, ?, ?, ?)
            """
            cursor.execute(
                insert_query,
                (
                    data.insurance_id,
                    data.annual_premium_collection,
                    data.active_policies,
                    data.coverage_areas,
                    data.specialization,
                    data.claim_settlement_ratio,
                ),
            )
            row = cursor.fetchone()
            conn.commit()
            if not row:
                raise HTTPException(status_code=500, detail="Failed to create insurance business info")
            business_id, insurance_id = row
            return InsuranceBusinessInfoResponse(
                business_id=business_id,
                insurance_id=insurance_id,
                message="Insurance business info created successfully",
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
