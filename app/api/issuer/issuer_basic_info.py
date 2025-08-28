from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

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
        print(f" Creating issuer basic info for user_id: {data.user_id}")
        
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING issuer_id
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
            issuer_id = result["issuer_id"]
            
            conn.commit()
            print(f" Issuer basic info created with ID: {issuer_id}")
            
            return IssuerBasicInfoResponse(
                issuer_id=issuer_id,
                user_id=data.user_id,
                message="Issuer basic info created successfully"
            )
            
        except Exception as e:
            conn.rollback()
            print(f" Basic info transaction rolled back: {e}")
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        print(f" Error creating issuer basic info: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create issuer basic info: {str(e)}"
        )

class IssuerNameResponse(BaseModel):
    issuer_id: int
    organization_name: str

@router.get("/issuer/{issuer_id}/basic-info", response_model=IssuerNameResponse)
async def get_issuer_basic_info(issuer_id: int):
    """Fetch issuer organization name by issuer_id."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT issuer_id, organization_name
                FROM issuer_basic_info
                WHERE issuer_id = %s
            """
            cursor.execute(sql, (issuer_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Issuer not found")
            return IssuerNameResponse(issuer_id=row["issuer_id"], organization_name=row["organization_name"])
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch issuer info: {str(e)}")

class IssuerWalletResponse(BaseModel):
    issuer_id: int
    user_id: int
    wallet_address: str

@router.get("/issuer/{issuer_id}/wallet", response_model=IssuerWalletResponse)
async def get_issuer_wallet(issuer_id: int):
    """Fetch issuer's registered wallet by issuer_id (joins users)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = (
                """
                SELECT ibi.issuer_id, ibi.user_id, u.wallet_address
                FROM issuer_basic_info ibi
                JOIN users u ON u.user_id = ibi.user_id
                WHERE ibi.issuer_id = %s
                """
            )
            cursor.execute(sql, (issuer_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Issuer not found")
            return IssuerWalletResponse(
                issuer_id=row["issuer_id"],
                user_id=row["user_id"],
                wallet_address=row["wallet_address"] or "",
            )
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch issuer wallet: {str(e)}")

class IssuerListItem(BaseModel):
    issuer_id: int
    organization_name: str

class IssuerListResponse(BaseModel):
    items: list[IssuerListItem]
    total: int

@router.get("/issuer/list", response_model=IssuerListResponse)
async def list_issuers():
    """Return a concise list of issuers (hospitals/labs) for selection in claims."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                SELECT issuer_id, organization_name
                FROM issuer_basic_info
                ORDER BY organization_name
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            items = [IssuerListItem(issuer_id=r["issuer_id"], organization_name=r["organization_name"]) for r in rows]
            return IssuerListResponse(items=items, total=len(items))
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list issuers: {str(e)}")
