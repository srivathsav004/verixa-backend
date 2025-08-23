from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class InsuranceContactTechRequest(BaseModel):
    insurance_id: int
    primary_contact_name: str
    designation: Optional[str] = None
    department: Optional[str] = None
    official_email: Optional[str] = None
    phone_number: Optional[str] = None
    claims_email: Optional[str] = None
    claims_phone: Optional[str] = None
    head_office_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    regional_offices: Optional[str] = None
    technical_contact_name: Optional[str] = None
    technical_email: Optional[str] = None
    integration_method: Optional[str] = None
    claims_system: Optional[str] = None
    monthly_verification_volume: Optional[int] = None
    auto_approval_threshold: Optional[float] = None
    manual_review_threshold: Optional[float] = None
    rejection_threshold: Optional[float] = None
    notification_preferences: Optional[str] = None
    payment_method: Optional[str] = None
    monthly_budget: Optional[float] = None

class InsuranceContactTechResponse(BaseModel):
    contact_id: int
    insurance_id: int
    message: str

@router.post("/insurance/contact-tech", response_model=InsuranceContactTechResponse)
async def create_insurance_contact_tech(data: InsuranceContactTechRequest):
    """Create insurance contact & technical info and return contact_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
            INSERT INTO insurance_contact_tech (
                insurance_id, primary_contact_name, designation, department,
                official_email, phone_number, claims_email, claims_phone,
                head_office_address, city, state, country, regional_offices,
                technical_contact_name, technical_email, integration_method, claims_system,
                monthly_verification_volume, auto_approval_threshold, manual_review_threshold,
                rejection_threshold, notification_preferences, payment_method, monthly_budget
            )
            OUTPUT INSERTED.contact_id, INSERTED.insurance_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(
                insert_query,
                (
                    data.insurance_id,
                    data.primary_contact_name,
                    data.designation,
                    data.department,
                    data.official_email,
                    data.phone_number,
                    data.claims_email,
                    data.claims_phone,
                    data.head_office_address,
                    data.city,
                    data.state,
                    data.country,
                    data.regional_offices,
                    data.technical_contact_name,
                    data.technical_email,
                    data.integration_method,
                    data.claims_system,
                    data.monthly_verification_volume,
                    data.auto_approval_threshold,
                    data.manual_review_threshold,
                    data.rejection_threshold,
                    data.notification_preferences,
                    data.payment_method,
                    data.monthly_budget,
                ),
            )
            row = cursor.fetchone()
            conn.commit()
            if not row:
                raise HTTPException(status_code=500, detail="Failed to create insurance contact/tech info")
            contact_id, insurance_id = row
            return InsuranceContactTechResponse(
                contact_id=contact_id,
                insurance_id=insurance_id,
                message="Insurance contact & technical info created successfully",
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
