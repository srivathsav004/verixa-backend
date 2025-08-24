from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection, upload_file_to_supabase
import uuid

router = APIRouter()

class PatientIdentityInsuranceResponse(BaseModel):
    pii_id: int
    patient_id: int
    message: str

async def upload_patient_document(file: UploadFile, folder: str) -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        file_content = await file.read()
        file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
        unique_filename = f"{uuid.uuid4().hex}.{file_extension}"
        file_path = f"{folder}/{unique_filename}"
        public_url = upload_file_to_supabase(file_content, file_path)
        return public_url
    except Exception as e:
        print(f"File upload error: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@router.post("/patient/identity-insurance", response_model=PatientIdentityInsuranceResponse)
async def create_patient_identity_insurance(
    patient_id: int = Form(...),
    gov_id_type: str = Form(...),
    gov_id_number: str = Form(...),
    gov_id_document: UploadFile = File(...),
    insurance_provider: Optional[str] = Form(None),
    policy_number: Optional[str] = Form(None),
    coverage_type: Optional[str] = Form(None),
    privacy_preferences: Optional[str] = Form(None),
):
    """Upload ID document and create patient identity & insurance record"""
    try:
        print(f"📁 Uploading ID document for patient_id: {patient_id}")

        # Upload ID document
        id_document_url = await upload_patient_document(gov_id_document, "patient_ids")
        print("✅ ID document uploaded")

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            insert_query = """
            INSERT INTO patient_identity_insurance (
                patient_id, gov_id_type, gov_id_number, gov_id_document,
                insurance_provider, policy_number, coverage_type, privacy_preferences
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING pii_id
            """

            cursor.execute(
                insert_query,
                (
                    patient_id,
                    gov_id_type,
                    gov_id_number,
                    id_document_url,
                    insurance_provider,
                    policy_number,
                    coverage_type,
                    privacy_preferences,
                ),
            )

            result = cursor.fetchone()
            pii_id = result["pii_id"]
            conn.commit()
            print("✅ Patient identity & insurance created successfully")

            return PatientIdentityInsuranceResponse(
                pii_id=pii_id,
                patient_id=patient_id,
                message="Patient identity and insurance stored successfully",
            )

        except Exception as e:
            conn.rollback()
            print(f"❌ Identity/Insurance transaction rolled back: {e}")
            raise e
        finally:
            conn.close()

    except Exception as e:
        print(f"❌ Error creating patient identity/insurance: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create patient identity/insurance: {str(e)}")
