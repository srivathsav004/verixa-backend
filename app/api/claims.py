from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from ..database import get_db_connection, upload_file_to_supabase

router = APIRouter()

class ClaimCreateResponse(BaseModel):
    claim_id: int
    patient_id: int
    insurance_id: int
    report_url: str
    is_verified: bool
    issued_by: Optional[int] = None
    status: str

class ClaimItem(BaseModel):
    claim_id: int
    patient_id: int
    insurance_id: int
    report_url: str
    is_verified: bool
    issued_by: Optional[int] = None
    status: str
    created_at: datetime

class ClaimListResponse(BaseModel):
    items: List[ClaimItem]
    total: int

@router.post("/claims", response_model=ClaimCreateResponse)
async def create_claim(
    patient_id: int = Form(...),
    insurance_id: int = Form(...),
    is_verified: bool = Form(...),
    issued_by: Optional[int] = Form(None),
    # Optional: reference an issued document on platform
    issued_doc_id: Optional[int] = Form(None),
    # Optional direct URL if already issued on platform
    report_url: Optional[str] = Form(None),
    # Optional file if not issued on platform
    file: Optional[UploadFile] = File(None),
):
    """Create a claim. If `is_verified` is True, `report_url` must be provided and `issued_by` should be the issuer_id.
    If `is_verified` is False and a file is provided, it will be uploaded and stored as `report_url`.
    """
    try:
        final_url = report_url
        derived_issuer: Optional[int] = None
        used_issued_doc_id: Optional[int] = None
        if not is_verified:
            # If not verified (not issued on platform), allow file upload
            if file is None and not final_url:
                raise HTTPException(status_code=400, detail="Either a file or report_url must be provided when is_verified is False")
            if file is not None:
                data = await file.read()
                safe_name = (file.filename or "report.pdf").replace(" ", "_")
                path = f"claims/{patient_id}/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{safe_name}"
                try:
                    final_url = upload_file_to_supabase(data, path)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")
        else:
            # Verified: Prefer issued_doc_id path. If provided, validate and derive URL/issuer.
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                if issued_doc_id is not None:
                    # Validate the issued doc belongs to patient and is active
                    cursor.execute(
                        """
                        SELECT id, patient_id, document_url, issuer_id, is_active
                        FROM issuer_issued_medical_docs
                        WHERE id = %s
                        """,
                        (issued_doc_id,),
                    )
                    doc = cursor.fetchone()
                    if not doc:
                        raise HTTPException(status_code=400, detail="issued_doc_id not found")
                    if int(doc["patient_id"]) != int(patient_id):
                        raise HTTPException(status_code=400, detail="issued_doc_id does not belong to patient")
                    if not doc.get("is_active", False):
                        raise HTTPException(status_code=400, detail="issued document already used or inactive")
                    final_url = doc["document_url"]
                    derived_issuer = doc.get("issuer_id")
                    used_issued_doc_id = doc["id"]
                # If no issued_doc_id, fall back to requiring a report_url
                if not final_url:
                    raise HTTPException(status_code=400, detail="report_url or issued_doc_id is required when is_verified is True")
            finally:
                cursor.close()
                conn.close()

        # Enforce issued_by rules to align with DB CHECK constraint
        if is_verified:
            # For verified claims, issuer must be provided (use derived issuer if available)
            if derived_issuer is not None:
                issued_by = derived_issuer
            if issued_by is None:
                raise HTTPException(status_code=400, detail="issued_by is required when is_verified is True")
        else:
            # For unverified claims, issuer must be null
            if issued_by is not None:
                raise HTTPException(status_code=400, detail="issued_by must be null when is_verified is False")

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_sql = (
                """
                INSERT INTO claims (patient_id, report_url, is_verified, issued_by, insurance_id, status, created_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', NOW())
                RETURNING claim_id, patient_id, insurance_id, report_url, is_verified, issued_by, status
                """
            )
            cursor.execute(insert_sql, (patient_id, final_url, is_verified, issued_by, insurance_id))
            row = cursor.fetchone()
            # If we used an issued_doc, lock it (set is_active=false)
            if used_issued_doc_id is not None:
                try:
                    cursor.execute(
                        """
                        UPDATE issuer_issued_medical_docs
                        SET is_active = FALSE
                        WHERE id = %s AND is_active = TRUE
                        """,
                        (used_issued_doc_id,),
                    )
                except Exception as e:
                    conn.rollback()
                    raise HTTPException(status_code=500, detail=f"Failed to lock issued document: {str(e)}")
            conn.commit()
            return ClaimCreateResponse(
                claim_id=row["claim_id"],
                patient_id=row["patient_id"],
                insurance_id=row["insurance_id"],
                report_url=row["report_url"],
                is_verified=row["is_verified"],
                issued_by=row.get("issued_by"),
                status=row["status"],
            )
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"DB insert failed: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Create claim failed: {str(e)}")

@router.get("/claims/by-patient/{patient_id}", response_model=ClaimListResponse)
async def list_claims_by_patient(patient_id: int):
    """List all claims for a patient."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql = (
                """
                SELECT claim_id, patient_id, insurance_id, report_url, is_verified, issued_by, status, created_at
                FROM claims
                WHERE patient_id = %s
                ORDER BY created_at DESC
                """
            )
            cursor.execute(sql, (patient_id,))
            rows = cursor.fetchall()
            items = [
                ClaimItem(
                    claim_id=r["claim_id"],
                    patient_id=r["patient_id"],
                    insurance_id=r["insurance_id"],
                    report_url=r["report_url"],
                    is_verified=r["is_verified"],
                    issued_by=r.get("issued_by"),
                    status=r["status"],
                    created_at=r["created_at"],
                ) for r in rows
            ]
            return ClaimListResponse(items=items, total=len(items))
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch claims: {str(e)}")

@router.get("/claims/by-insurance/{insurance_id}", response_model=ClaimListResponse)
async def list_claims_by_insurance(insurance_id: int, status: Optional[str] = None):
    """List claims for an insurance. Optionally filter by status (e.g., pending/approved/rejected)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            if status:
                sql = (
                    """
                    SELECT claim_id, patient_id, insurance_id, report_url, is_verified, issued_by, status, created_at
                    FROM claims
                    WHERE insurance_id = %s AND LOWER(status) = LOWER(%s)
                    ORDER BY created_at DESC
                    """
                )
                cursor.execute(sql, (insurance_id, status))
            else:
                sql = (
                    """
                    SELECT claim_id, patient_id, insurance_id, report_url, is_verified, issued_by, status, created_at
                    FROM claims
                    WHERE insurance_id = %s
                    ORDER BY created_at DESC
                    """
                )
                cursor.execute(sql, (insurance_id,))
            rows = cursor.fetchall()
            items = [
                ClaimItem(
                    claim_id=r["claim_id"],
                    patient_id=r["patient_id"],
                    insurance_id=r["insurance_id"],
                    report_url=r["report_url"],
                    is_verified=r["is_verified"],
                    issued_by=r.get("issued_by"),
                    status=r["status"],
                    created_at=r["created_at"],
                ) for r in rows
            ]
            return ClaimListResponse(items=items, total=len(items))
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch insurance claims: {str(e)}")

# --- Status update endpoints for insurance actions ---

class ClaimStatusUpdateRequest(BaseModel):
    status: str  # expected: approved | rejected

class BulkClaimStatusUpdateRequest(BaseModel):
    claim_ids: List[int]
    status: str  # expected: approved | rejected


@router.patch("/claims/{claim_id}/status")
async def update_claim_status(claim_id: int, body: ClaimStatusUpdateRequest):
    """Update a single claim's status to approved/rejected."""
    status = (body.status or "").lower()
    if status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail="status must be 'approved' or 'rejected'")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE claims
                SET status = %s
                WHERE claim_id = %s
                RETURNING claim_id
                """,
                (status, claim_id),
            )
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                raise HTTPException(status_code=404, detail="claim not found")
            conn.commit()
            return {"ok": True, "claim_id": row["claim_id"], "status": status}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update claim: {str(e)}")


@router.post("/claims/bulk-status")
async def bulk_update_claim_status(body: BulkClaimStatusUpdateRequest):
    """Bulk update claim statuses (approve/reject)."""
    status = (body.status or "").lower()
    ids = body.claim_ids or []
    if status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail="status must be 'approved' or 'rejected'")
    if not ids:
        raise HTTPException(status_code=400, detail="claim_ids cannot be empty")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Use ANY(%s) requires list to be adapted, use IN with tuple formatting
            sql = f"UPDATE claims SET status = %s WHERE claim_id = ANY(%s) RETURNING claim_id"
            cursor.execute(sql, (status, ids))
            rows = cursor.fetchall()
            if not rows:
                conn.rollback()
                raise HTTPException(status_code=404, detail="no claims updated")
            conn.commit()
            updated_ids = [r["claim_id"] for r in rows]
            return {"ok": True, "updated": updated_ids, "status": status}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk update failed: {str(e)}")
