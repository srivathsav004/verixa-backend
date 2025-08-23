from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ..database import get_db_connection

router = APIRouter()

class IssuerBasicInfoRequest(BaseModel):
    user_id: int
    organization_name: str
    organization_type: str
    license_number: str
    registration_number: str
    established_year: Optional[int] = None
    website_url: Optional[str] = None
    logo_url: Optional[str] = None
    contact_person_name: str
    designation: str
    phone_number: str
    alt_phone_number: Optional[str] = None
    street_address: str
    city: str
    state: str
    postal_code: str
    country: str
    landmark: Optional[str] = None

class IssuerBasicInfoResponse(BaseModel):
    issuer_id: int
    user_id: int
    message: str

@router.post("/issuer/basic-info", response_model=IssuerBasicInfoResponse)
async def create_issuer_basic_info(data: IssuerBasicInfoRequest):
    """Create issuer basic info using user_id"""
    
    try:
        print(f"📝 Creating issuer basic info for user_id: {data.user_id}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Insert issuer basic info
            insert_query = """
            INSERT INTO issuer_basic_info (
                user_id, organization_name, organization_type, license_number, 
                registration_number, established_year, website_url, logo_url,
                contact_person_name, designation, phone_number, alt_phone_number,
                street_address, city, state, postal_code, country, landmark
            )
            OUTPUT INSERTED.issuer_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(insert_query, (
                data.user_id, data.organization_name, data.organization_type, 
                data.license_number, data.registration_number, data.established_year, 
                data.website_url, data.logo_url, data.contact_person_name, 
                data.designation, data.phone_number, data.alt_phone_number,
                data.street_address, data.city, data.state, data.postal_code, 
                data.country, data.landmark
            ))
            
            result = cursor.fetchone()
            issuer_id = result[0]
            
            conn.commit()
            print(f"✅ Issuer basic info created with ID: {issuer_id}")
            
            return IssuerBasicInfoResponse(
                issuer_id=issuer_id,
                user_id=data.user_id,
                message="Issuer basic info created successfully"
            )
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Basic info transaction rolled back: {e}")
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Error creating issuer basic info: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create issuer basic info: {str(e)}"
        )
