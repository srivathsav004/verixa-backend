from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ...database import get_db_connection

router = APIRouter()

class IssuerReportFormatsRequest(BaseModel):
    issuer_id: int
    report_templates: str
    report_types: Optional[str] = None
    standard_font: Optional[str] = None
    standard_font_size: Optional[int] = None
    logo_position: Optional[str] = None
    header_format: Optional[str] = None
    footer_format: Optional[str] = None
    normal_ranges: Optional[str] = None
    units_used: Optional[str] = None
    reference_standards: Optional[str] = None

class IssuerReportFormatsResponse(BaseModel):
    issuer_id: int
    message: str

@router.post("/issuer/report-formats", response_model=IssuerReportFormatsResponse)
async def create_issuer_report_formats(data: IssuerReportFormatsRequest):
    """Create issuer report formats using issuer_id"""
    
    try:
        print(f"📋 Creating report formats for issuer_id: {data.issuer_id}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Insert issuer report formats
            insert_query = """
            INSERT INTO issuer_report_formats (
                issuer_id, report_templates, report_types, standard_font,
                standard_font_size, logo_position, header_format, footer_format,
                normal_ranges, units_used, reference_standards
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                data.issuer_id, data.report_templates, data.report_types, 
                data.standard_font, data.standard_font_size, data.logo_position,
                data.header_format, data.footer_format, data.normal_ranges,
                data.units_used, data.reference_standards
            ))
            
            conn.commit()
            print(f"✅ Issuer report formats created successfully")
            
            return IssuerReportFormatsResponse(
                issuer_id=data.issuer_id,
                message="Issuer report formats created successfully"
            )
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Report formats transaction rolled back: {e}")
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Error creating issuer report formats: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create issuer report formats: {str(e)}"
        )
