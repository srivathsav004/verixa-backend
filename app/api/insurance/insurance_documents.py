from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection, upload_file_to_supabase
import uuid

router = APIRouter()

class InsuranceDocumentsResponse(BaseModel):
    insurance_id: int
    message: str

async def upload_insurance_document(file: UploadFile, folder: str) -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        content = await file.read()
        ext = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
        unique_name = f"{uuid.uuid4().hex}.{ext}"
        path = f"{folder}/{unique_name}"
        public_url = upload_file_to_supabase(content, path)
        return public_url
    except Exception as e:
        print(f"File upload error: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@router.post("/insurance/documents", response_model=InsuranceDocumentsResponse)
async def create_insurance_documents(
    insurance_id: int = Form(...),
    company_logo: Optional[UploadFile] = File(None),
    insurance_license_certificate: UploadFile = File(...),
    registration_certificate: UploadFile = File(...),
    business_registration_doc: UploadFile = File(...),
    tax_registration_doc: UploadFile = File(...),
    audited_financials: Optional[UploadFile] = File(None),
):
    """Upload documents and create insurance documents record"""
    try:
        print(f"📁 Uploading insurance documents for insurance_id: {insurance_id}")

        logo_url = None
        if company_logo:
            logo_url = await upload_insurance_document(company_logo, "logos")

        license_url = await upload_insurance_document(insurance_license_certificate, "licenses")
        reg_cert_url = await upload_insurance_document(registration_certificate, "registrations")
        business_reg_url = await upload_insurance_document(business_registration_doc, "business_regs")
        tax_doc_url = await upload_insurance_document(tax_registration_doc, "tax_docs")
        audited_financials_url = None
        if audited_financials:
            audited_financials_url = await upload_insurance_document(audited_financials, "financials")

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # If logo exists, update in basic info
            if logo_url:
                cursor.execute(
                    """
                    UPDATE insurance_basic_info
                    SET logo_url = %s
                    WHERE insurance_id = %s
                    """,
                    (logo_url, insurance_id),
                )

            insert_query = (
                """
                INSERT INTO insurance_documents (
                    insurance_id, insurance_license_certificate, registration_certificate,
                    business_registration_doc, tax_registration_doc, audited_financials
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            )
            cursor.execute(
                insert_query,
                (
                    insurance_id,
                    license_url,
                    reg_cert_url,
                    business_reg_url,
                    tax_doc_url,
                    audited_financials_url,
                ),
            )
            conn.commit()
            print("✅ Insurance documents created successfully")
            return InsuranceDocumentsResponse(
                insurance_id=insurance_id,
                message="Insurance documents uploaded and stored successfully",
            )
        except Exception as e:
            conn.rollback()
            print(f"❌ Documents transaction rolled back: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creating insurance documents: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
