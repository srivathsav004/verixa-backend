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

class PatientListItem(BaseModel):
    user_id: int
    patient_id: int
    first_name: str
    last_name: str
    email: str
    phone_number: str
    gender: Optional[str]

class PatientListResponse(BaseModel):
    items: list[PatientListItem]
    total: int

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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING patient_id
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
            patient_id = result["patient_id"]
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

@router.get("/patients/fetch", response_model=PatientListResponse)
async def fetch_patients():
    """Return all patients in a single response without pagination/search."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT u.user_id, p.patient_id, p.first_name, p.last_name, p.email, p.phone_number, p.gender
                FROM users u
                JOIN patient_basic_info p ON p.user_id = u.user_id
                WHERE u.role = 'patient'
                ORDER BY p.first_name, p.last_name
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            items = [
                PatientListItem(
                    user_id=row["user_id"],
                    patient_id=row["patient_id"],
                    first_name=row["first_name"],
                    last_name=row["last_name"],
                    email=row["email"],
                    phone_number=row["phone_number"],
                    gender=row.get("gender"),
                ) for row in rows
            ]
            return PatientListResponse(items=items, total=len(items))
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ Error listing patients: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch patients: {str(e)}")

class PatientNameResponse(BaseModel):
    patient_id: int
    user_id: int
    first_name: str
    last_name: str

@router.get("/patient/{patient_id}/basic-info", response_model=PatientNameResponse)
async def get_patient_basic_info(patient_id: int):
    """Fetch a single patient's basic info (first_name, last_name) by patient_id."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT p.patient_id, p.user_id, p.first_name, p.last_name
                FROM patient_basic_info p
                WHERE p.patient_id = %s
            """
            cursor.execute(sql, (patient_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Patient not found")
            return PatientNameResponse(
                patient_id=row["patient_id"],
                user_id=row["user_id"],
                first_name=row["first_name"],
                last_name=row["last_name"],
            )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch patient info: {str(e)}")
