from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection, get_supabase_client, upload_file_to_supabase
import uuid

router = APIRouter()

class ValidatorDocumentsResponse(BaseModel):
    doc_id: int
    validator_id: int
    professional_license_certificate: str
    institution_id_letter: str
    educational_qualification_certificate: str
    message: str

@router.post("/validator/documents", response_model=ValidatorDocumentsResponse)
async def upload_validator_documents(
    validator_id: int = Form(...),
    professional_license_certificate: UploadFile = File(...),
    institution_id_letter: UploadFile = File(...),
    educational_qualification_certificate: UploadFile = File(...),
):
    try:
        # Upload to Supabase similar to issuer flow
        bucket = "verixa-documents"
        plc_name = f"validators/{validator_id}/professional_license_{uuid.uuid4().hex}_{professional_license_certificate.filename}"
        iid_name = f"validators/{validator_id}/institution_id_letter_{uuid.uuid4().hex}_{institution_id_letter.filename}"
        eqc_name = f"validators/{validator_id}/education_qualification_{uuid.uuid4().hex}_{educational_qualification_certificate.filename}"

        plc_bytes = await professional_license_certificate.read()
        iid_bytes = await institution_id_letter.read()
        eqc_bytes = await educational_qualification_certificate.read()

        plc_url = upload_file_to_supabase(plc_bytes, plc_name, bucket)
        iid_url = upload_file_to_supabase(iid_bytes, iid_name, bucket)
        eqc_url = upload_file_to_supabase(eqc_bytes, eqc_name, bucket)

        # Insert into DB
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
            INSERT INTO validator_documents (
                validator_id,
                professional_license_certificate,
                institution_id_letter,
                educational_qualification_certificate
            )
            OUTPUT INSERTED.doc_id
            VALUES (?, ?, ?, ?)
            """
            cursor.execute(
                insert_query,
                (
                    validator_id,
                    plc_url,
                    iid_url,
                    eqc_url,
                ),
            )
            row = cursor.fetchone()
            conn.commit()
            return ValidatorDocumentsResponse(
                doc_id=row[0],
                validator_id=validator_id,
                professional_license_certificate=plc_url,
                institution_id_letter=iid_url,
                educational_qualification_certificate=eqc_url,
                message="Validator documents uploaded successfully",
            )
        except Exception as e:
            conn.rollback()
            print(f"❌ Validator documents insert error: {e}")
            raise e
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error uploading validator documents: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload documents")
