from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from ...database import get_db_connection, upload_file_to_supabase

router = APIRouter()

class IssueReportResponse(BaseModel):
    id: int
    patient_id: int
    report_type: str
    document_url: str
    created_at: datetime

class IssuedDoc(BaseModel):
    id: int
    patient_id: int
    report_type: str
    document_url: str
    issuer_id: Optional[int] = None
    created_at: datetime

class IssuedDocListResponse(BaseModel):
    items: list[IssuedDoc]
    total: int
    page: int
    page_size: int

@router.post("/issuer/issued-docs", response_model=IssueReportResponse)
async def issue_report(
    patient_id: int = Form(...),
    report_type: str = Form(...),
    issuer_id: int = Form(...),
    file: UploadFile = File(...),
):
    """Issue a new medical document for a patient.
    - Uploads the file to Supabase storage
    - Inserts a row into issuer_issued_medical_docs (assumed existing table)
      Columns expected: id (PK, IDENTITY), patient_id, report_type, document_url,
      issuer_user_id (nullable), created_at (default getdate())
    """
    try:
        # Read file and upload to Supabase
        data = await file.read()
        # Construct a storage path: patients/{patient_id}/{timestamp}_{filename}
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_name = file.filename.replace(" ", "_") if file.filename else f"report_{timestamp}.pdf"
        storage_path = f"patients/{patient_id}/{timestamp}_{safe_name}"

        try:
            document_url = upload_file_to_supabase(data, storage_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

        # Insert DB row
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_sql = (
                """
                INSERT INTO issuer_issued_medical_docs (patient_id, report_type, document_url, issuer_id, created_at)
                OUTPUT INSERTED.id, INSERTED.patient_id, INSERTED.report_type, INSERTED.document_url, INSERTED.created_at
                VALUES (?, ?, ?, ?, GETDATE())
                """
            )
            cursor.execute(
                insert_sql,
                (
                    patient_id,
                    report_type,
                    document_url,
                    issuer_id,
                ),
            )
            row = cursor.fetchone()
            conn.commit()

            return IssueReportResponse(
                id=row[0],
                patient_id=row[1],
                report_type=row[2],
                document_url=row[3],
                created_at=row[4],
            )
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"DB insert failed: {str(e)}")
        finally:
            conn.close()

    except HTTPException:
        # Propagate known HTTP errors
        raise
    except Exception as e:
        # Catch-all for unexpected failures in issuing report
        raise HTTPException(status_code=500, detail=f"Issue report failed: {str(e)}")

@router.get("/issuer/issued-docs/fetch", response_model=IssuedDocListResponse)
async def fetch_issued_docs():
    """Return all issued documents in a single response (no pagination)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = (
            """
            SELECT d.id, d.patient_id, d.report_type, d.document_url, d.issuer_id, d.created_at
            FROM issuer_issued_medical_docs d
            ORDER BY d.created_at DESC
            """
        )
        cursor.execute(sql)
        rows = cursor.fetchall()
        items = [
            IssuedDoc(
                id=r[0],
                patient_id=r[1],
                report_type=r[2],
                document_url=r[3],
                issuer_id=r[4],
                created_at=r[5],
            )
            for r in rows
        ]
        return IssuedDocListResponse(items=items, total=len(items), page=1, page_size=len(items))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list issued docs: {str(e)}")
    finally:
        conn.close()
