from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
from ..database import get_db_connection, upload_file_to_supabase
import uuid

router = APIRouter()

class IssuerDocumentsResponse(BaseModel):
    issuer_id: int
    message: str

async def upload_issuer_document(file: UploadFile, folder: str) -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        # Read file content
        file_content = await file.read()
        
        # Generate unique filename
        file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
        unique_filename = f"{uuid.uuid4().hex}.{file_extension}"
        file_path = f"{folder}/{unique_filename}"
        
        # Upload using centralized database function
        public_url = upload_file_to_supabase(file_content, file_path)
        return public_url
            
    except Exception as e:
        print(f"File upload error: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@router.post("/issuer/documents", response_model=IssuerDocumentsResponse)
async def create_issuer_documents(
    issuer_id: int = Form(...),
    logo_file: Optional[UploadFile] = File(None),
    medical_license_certificate: UploadFile = File(...),
    business_registration_certificate: UploadFile = File(...),
    tax_registration_document: UploadFile = File(...),
    accreditation_certificates: Optional[UploadFile] = File(None)
):
    """Upload documents and create issuer documents record"""
    
    try:
        print(f"📁 Uploading documents for issuer_id: {issuer_id}")
        
        # Upload files to Supabase
        logo_url = None
        if logo_file:
            logo_url = await upload_issuer_document(logo_file, "logos")
        
        medical_license_url = await upload_issuer_document(medical_license_certificate, "licenses")
        business_reg_url = await upload_issuer_document(business_registration_certificate, "registrations")
        tax_reg_url = await upload_issuer_document(tax_registration_document, "tax_docs")
        
        accreditation_url = None
        if accreditation_certificates:
            accreditation_url = await upload_issuer_document(accreditation_certificates, "accreditations")
        
        print(f"✅ Files uploaded successfully")
        
        # Update logo_url in issuer_basic_info if logo was uploaded
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            if logo_url:
                update_logo_query = """
                UPDATE issuer_basic_info 
                SET logo_url = ? 
                WHERE issuer_id = ?
                """
                cursor.execute(update_logo_query, (logo_url, issuer_id))
            
            # Create issuer documents record
            insert_query = """
            INSERT INTO issuer_documents (
                issuer_id, medical_license_certificate, business_registration_certificate,
                tax_registration_document, accreditation_certificates
            )
            VALUES (?, ?, ?, ?, ?)
            """
            
            cursor.execute(insert_query, (
                issuer_id, medical_license_url, business_reg_url, 
                tax_reg_url, accreditation_url
            ))
            
            conn.commit()
            print(f"✅ Issuer documents created successfully")
            
            return IssuerDocumentsResponse(
                issuer_id=issuer_id,
                message="Issuer documents uploaded and stored successfully"
            )
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Documents transaction rolled back: {e}")
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Error creating issuer documents: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create issuer documents: {str(e)}"
        )
