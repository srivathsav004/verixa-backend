from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class PatientBasicInfoRequest(BaseModel):
    user_id: int
    first_name: str
    last_name: str
    dob: str  # Expecting YYYY-MM-DD string; DB will handle date type conversion if needed
    gender: str
    blood_group: Optional[str] = None
    marital_status: Optional[str] = None
    email: str
    phone_number: str
    alt_phone_number: Optional[str] = None

class PatientBasicInfoResponse(BaseModel):
    patient_id: int
    user_id: int
    message: str

@router.post("/patient/basic-info", response_model=PatientBasicInfoResponse)
async def create_patient_basic_info(data: PatientBasicInfoRequest):
    """Create patient basic info using user_id"""
    try:
        print(f"📝 Creating patient basic info for user_id: {data.user_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            insert_query = """
            INSERT INTO patient_basic_info (
                user_id, first_name, last_name, dob, gender,
                blood_group, marital_status, email, phone_number, alt_phone_number
            )
            OUTPUT INSERTED.patient_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor.execute(
                insert_query,
                (
                    data.user_id,
                    data.first_name,
                    data.last_name,
                    data.dob,
                    data.gender,
                    data.blood_group,
                    data.marital_status,
                    data.email,
                    data.phone_number,
                    data.alt_phone_number,
                ),
            )

            result = cursor.fetchone()
            patient_id = result[0]
            conn.commit()
            print(f"✅ Patient basic info created with ID: {patient_id}")

            return PatientBasicInfoResponse(
                patient_id=patient_id,
                user_id=data.user_id,
                message="Patient basic info created successfully",
            )

        except Exception as e:
            conn.rollback()
            print(f"❌ Basic info transaction rolled back: {e}")
            raise e
        finally:
            conn.close()

    except Exception as e:
        print(f"❌ Error creating patient basic info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create patient basic info: {str(e)}")
