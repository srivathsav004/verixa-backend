from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class ValidatorBasicInfoRequest(BaseModel):
    user_id: int
    full_name: str
    professional_title: str
    license_number: str
    years_of_experience: int
    specialization: str
    current_institution: str
    professional_email: str
    preferred_validation_types: Optional[str] = None
    expected_validations_per_day: Optional[int] = None
    availability_hours: Optional[str] = None

class ValidatorBasicInfoResponse(BaseModel):
    validator_id: int
    user_id: int
    message: str

@router.post("/validator/basic-info", response_model=ValidatorBasicInfoResponse)
async def create_validator_basic_info(data: ValidatorBasicInfoRequest):
    """Create validator basic info using user_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
            INSERT INTO validator_basic_info (
                user_id,
                full_name,
                professional_title,
                license_number,
                years_of_experience,
                specialization,
                current_institution,
                professional_email,
                preferred_validation_types,
                expected_validations_per_day,
                availability_hours
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING validator_id
            """
            cursor.execute(
                insert_query,
                (
                    data.user_id,
                    data.full_name,
                    data.professional_title,
                    data.license_number,
                    data.years_of_experience,
                    data.specialization,
                    data.current_institution,
                    data.professional_email,
                    data.preferred_validation_types,
                    data.expected_validations_per_day,
                    data.availability_hours,
                ),
            )
            result = cursor.fetchone()
            validator_id = result["validator_id"]
            conn.commit()
            return ValidatorBasicInfoResponse(
                validator_id=validator_id,
                user_id=data.user_id,
                message="Validator basic info created successfully",
            )
        except Exception as e:
            conn.rollback()
            print(f"❌ Validator basic info transaction rolled back: {e}")
            raise e
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ Error creating validator basic info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create validator basic info: {str(e)}")
