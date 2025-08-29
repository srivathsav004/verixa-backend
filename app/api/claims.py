from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
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

class PaginatedClaimsResponse(BaseModel):
    items: List[ClaimItem]
    total: int
    page: int
    page_size: int

class AIEvaluationItem(BaseModel):
    claim_id: int
    report_type: Optional[str] = None
    document_url: Optional[str] = None
    ai_score: int
    bucket: Optional[str] = None  # expected: auto | manual | reject

class AIEvaluationBulkRequest(BaseModel):
    evaluations: List[AIEvaluationItem]

class AIEvalFetchRequest(BaseModel):
    claim_ids: List[int]

class AIEvalRecord(BaseModel):
    claim_id: int
    ai_score: int
    bucket: Optional[str] = None
    evaluated_at: datetime

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
    """Create a claim.
    - If `is_verified` is True, prefer `issued_doc_id` to derive `report_url` and `issued_by` (issuer_id); otherwise require `report_url` and `issued_by`.
    - If `is_verified` is False and a file is provided, it will be uploaded and stored as `report_url`.
    - For unverified uploads, the client may provide `issued_by` to indicate the hospital/issuer where the report was obtained.
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

        # Enforce issued_by rules: verified must have issuer; unverified may specify issuer (required for our flow)
        if is_verified:
            # For verified claims, issuer must be provided (use derived issuer if available)
            if derived_issuer is not None:
                issued_by = derived_issuer
            if issued_by is None:
                raise HTTPException(status_code=400, detail="issued_by is required when is_verified is True")
        else:
            # For unverified claims, require issuer selection from client (hospital where report was obtained)
            if issued_by is None:
                raise HTTPException(status_code=400, detail="issued_by is required when is_verified is False")

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


# ---- New: Bulk set is_verified to TRUE (status unchanged) ----
class BulkSetVerifiedRequest(BaseModel):
    claim_ids: List[int]


@router.post("/claims/bulk-set-verified")
async def bulk_set_verified(body: BulkSetVerifiedRequest):
    """
    Bulk mark claims as verified (is_verified=TRUE) without changing status.
    """
    ids = body.claim_ids or []
    if not ids:
        raise HTTPException(status_code=400, detail="claim_ids cannot be empty")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            sql_update = (
                "UPDATE claims SET is_verified = TRUE "
                "WHERE claim_id = ANY(%s) "
                "RETURNING claim_id"
            )
            cursor.execute(sql_update, (ids,))
            rows = cursor.fetchall()
            if not rows:
                conn.rollback()
                raise HTTPException(status_code=404, detail="no claims updated")
            conn.commit()
            return {"ok": True, "updated": [r["claim_id"] for r in rows]}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk set verified failed: {str(e)}")

# ---- New: Unverified external claims listing (not issued on platform) ----
@router.get("/claims/unverified-external/by-insurance/{insurance_id}", response_model=PaginatedClaimsResponse)
async def list_unverified_external_claims(
    insurance_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
):
    """List unverified, pending claims for an insurance that DO NOT yet have a task AND have NO AI score yet.
    Conditions:
      - claims.is_verified = FALSE
      - claims.status = 'pending'
      - no row exists in tasks for this claim_id (LEFT JOIN tasks t ... t.claim_id IS NULL)
      - no row exists in ai_claim_evaluations for this claim_id (no AI score yet)
    Notes:
      - issued_by may be present or null (do not filter by it)
      - include regardless of latest AI bucket
    Supports simple search on report_url.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Build common filter and exclude claims that already have a task
            params: List[object] = [insurance_id]
            search_clause = ""
            if search:
                search_clause = " AND c.report_url ILIKE %s"
                params.append(f"%{search}%")

            count_sql = (
                """
                SELECT COUNT(*) AS c
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                LEFT JOIN (
                  SELECT DISTINCT claim_id FROM ai_claim_evaluations
                ) ae ON ae.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND t.claim_id IS NULL
                  AND ae.claim_id IS NULL
                """
                + search_clause
            )
            cursor.execute(count_sql, params)
            total = int(cursor.fetchone()["c"])

            # Page items
            params = [insurance_id]
            if search:
                params.append(f"%{search}%")
            params.extend([page_size, (page - 1) * page_size])
            list_sql = (
                """
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified, c.issued_by, c.status, c.created_at
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                LEFT JOIN (
                  SELECT DISTINCT claim_id FROM ai_claim_evaluations
                ) ae ON ae.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND t.claim_id IS NULL
                  AND ae.claim_id IS NULL
                """
                + search_clause +
                " ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
            )
            cursor.execute(list_sql, params)
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
            return PaginatedClaimsResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch unverified external claims: {str(e)}")


# ---- New: Validate-documents list — pending, unverified, HAVE AI score, and NO task ----
@router.get("/claims/validate-documents/by-insurance/{insurance_id}", response_model=PaginatedClaimsResponse)
async def list_validate_documents_claims(
    insurance_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
):
    """List claims for validation: pending, unverified, latest AI bucket='manual', and still no task.
    Conditions:
      - claims.is_verified = FALSE
      - claims.status = 'pending'
      - latest AI bucket is 'manual' (must have an AI evaluation)
      - no row exists in tasks for this claim_id
    Supports simple search on report_url.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            params: List[object] = [insurance_id]
            search_clause = ""
            if search:
                search_clause = " AND c.report_url ILIKE %s"
                params.append(f"%{search}%")

            count_sql = (
                """
                WITH latest_eval AS (
                  SELECT DISTINCT ON (claim_id) claim_id, bucket
                  FROM ai_claim_evaluations
                  ORDER BY claim_id, evaluated_at DESC
                )
                SELECT COUNT(*) AS c
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND t.claim_id IS NULL
                  AND LOWER(le.bucket) = 'manual'
                """
                + search_clause
            )
            cursor.execute(count_sql, params)
            total = int(cursor.fetchone()["c"])

            params = [insurance_id]
            if search:
                params.append(f"%{search}%")
            params.extend([page_size, (page - 1) * page_size])
            list_sql = (
                """
                WITH latest_eval AS (
                  SELECT DISTINCT ON (claim_id) claim_id, bucket
                  FROM ai_claim_evaluations
                  ORDER BY claim_id, evaluated_at DESC
                )
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified, c.issued_by, c.status, c.created_at
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND t.claim_id IS NULL
                  AND LOWER(le.bucket) = 'manual'
                """
                + search_clause +
                " ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
            )
            cursor.execute(list_sql, params)
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
            return PaginatedClaimsResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch validate-documents claims: {str(e)}")


# ---- New: Manual-review claims (pending, unverified, latest AI bucket='manual') ----
@router.get("/claims/manual-review/by-insurance/{insurance_id}", response_model=PaginatedClaimsResponse)
async def list_manual_review_claims(
    insurance_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
):
    """List claims requiring manual review for an insurance.
    Conditions:
      - claims.is_verified = FALSE
      - claims.issued_by IS NULL
      - claims.status = 'pending'
      - latest AI bucket is 'manual' (must have an AI evaluation)
    Supports simple search on report_url.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            params: List[object] = [insurance_id]
            search_clause = ""
            if search:
                search_clause = " AND c.report_url ILIKE %s"
                params.append(f"%{search}%")

            count_sql = (
                """
                WITH latest_eval AS (
                    SELECT DISTINCT ON (claim_id) claim_id, bucket
                    FROM ai_claim_evaluations
                    ORDER BY claim_id, evaluated_at DESC
                )
                SELECT COUNT(*) AS c
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.issued_by IS NULL
                  AND c.status = 'pending'
                  AND LOWER(le.bucket) = 'manual'
                """
                + search_clause
            )
            cursor.execute(count_sql, params)
            total = int(cursor.fetchone()["c"])

            params = [insurance_id]
            if search:
                params.append(f"%{search}%")
            params.extend([page_size, (page - 1) * page_size])
            list_sql = (
                """
                WITH latest_eval AS (
                    SELECT DISTINCT ON (claim_id) claim_id, bucket
                    FROM ai_claim_evaluations
                    ORDER BY claim_id, evaluated_at DESC
                )
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified, c.issued_by, c.status, c.created_at
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.issued_by IS NULL
                  AND c.status = 'pending'
                  AND LOWER(le.bucket) = 'manual'
                """
                + search_clause +
                " ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
            )
            cursor.execute(list_sql, params)
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
            return PaginatedClaimsResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch manual-review claims: {str(e)}")


# ---- New: Manual-review claims WITHOUT an associated task ----
@router.get("/claims/manual-review-without-task/by-insurance/{insurance_id}", response_model=PaginatedClaimsResponse)
async def list_manual_review_without_task_claims(
    insurance_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
):
    """List manual-bucket claims that are pending, unverified, external AND have no task yet.
    Conditions:
      - claims.is_verified = FALSE
      - claims.status = 'pending'
      - latest AI bucket is 'manual'
      - no row exists in tasks for this claim_id
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            params: List[object] = [insurance_id]
            search_clause = ""
            if search:
                search_clause = " AND c.report_url ILIKE %s"
                params.append(f"%{search}%")

            count_sql = (
                """
                WITH latest_eval AS (
                    SELECT DISTINCT ON (claim_id) claim_id, bucket
                    FROM ai_claim_evaluations
                    ORDER BY claim_id, evaluated_at DESC
                )
                SELECT COUNT(*) AS c
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND LOWER(le.bucket) = 'manual'
                  AND t.claim_id IS NULL
                """
                + search_clause
            )
            cursor.execute(count_sql, params)
            total = int(cursor.fetchone()["c"])

            params = [insurance_id]
            if search:
                params.append(f"%{search}%")
            params.extend([page_size, (page - 1) * page_size])
            list_sql = (
                """
                WITH latest_eval AS (
                    SELECT DISTINCT ON (claim_id) claim_id, bucket
                    FROM ai_claim_evaluations
                    ORDER BY claim_id, evaluated_at DESC
                )
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified, c.issued_by, c.status, c.created_at
                FROM claims c
                JOIN latest_eval le ON le.claim_id = c.claim_id
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.status = 'pending'
                  AND LOWER(le.bucket) = 'manual'
                  AND t.claim_id IS NULL
                """
                + search_clause +
                " ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
            )
            cursor.execute(list_sql, params)
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
            return PaginatedClaimsResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch manual-review without task: {str(e)}")


# ---- New: Verification queue — unverified pending claims that HAVE tasks ----
class VerificationQueueItem(BaseModel):
    claim_id: int
    patient_id: int
    insurance_id: int
    report_url: str
    is_verified: bool
    task_row_id: int
    task_id: Optional[int] = None
    contract_address: Optional[str] = None
    required_validators: Optional[int] = None
    tx_hash: Optional[str] = None
    reward_pol: Optional[str] = None
    status: Optional[str] = None
    created_at: datetime


class VerificationQueueResponse(BaseModel):
    items: List[VerificationQueueItem]
    total: int
    page: int
    page_size: int


@router.get("/verification-queue", response_model=VerificationQueueResponse)
async def get_verification_queue(
    request: Request,
    insurance_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    wallet_address: Optional[str] = None,
    validator_user_id: Optional[int] = None,
    include_completed: bool = False,
):
    """Return claims (pending, unverified) that already have a task, joined with task info.
    If wallet_address or validator_user_id is provided, exclude tasks already submitted by that validator.
    Also hide tasks whose status is already 'completed'.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Determine validator user id: prefer explicit param, else cookie, else wallet lookup
            uid: Optional[int] = validator_user_id
            if not uid:
                cookie_uid = request.cookies.get("user_id")
                try:
                    uid = int(cookie_uid) if cookie_uid is not None else None
                except Exception:
                    uid = None
            if not uid and wallet_address:
                cur.execute(
                    "SELECT user_id FROM users WHERE LOWER(wallet_address) = LOWER(%s) LIMIT 1",
                    (wallet_address.strip(),),
                )
                ur = cur.fetchone()
                uid = ur["user_id"] if ur else None
            where = [
                "c.insurance_id = %s",
                "c.is_verified = FALSE",
                "c.status = 'pending'",
                "t.claim_id IS NOT NULL",
                "LOWER(le.bucket) = 'manual'",
            ]
            params: list = [insurance_id]
            if search:
                where.append("(CAST(c.claim_id AS TEXT) ILIKE %s OR c.report_url ILIKE %s)")
                like = f"%{search}%"
                params.extend([like, like])
            # Task status constraint: default to pending only unless include_completed
            if not include_completed:
                where.append("t.status = 'pending'")

            # Exclude tasks this validator has already submitted
            exclude_sql = ""
            if uid:
                exclude_sql = " AND NOT EXISTS (SELECT 1 FROM validator_submissions vs WHERE vs.task_id = t.task_id AND vs.validator_user_id = %s)"
                params.append(uid)
            where_sql = " AND ".join(where)

            # count
            cur.execute(
                f"""
                WITH latest_eval AS (
                  SELECT DISTINCT ON (claim_id) claim_id, bucket
                  FROM ai_claim_evaluations
                  ORDER BY claim_id, evaluated_at DESC
                )
                SELECT COUNT(*) AS cnt
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE {where_sql}{exclude_sql}
                """,
                tuple(params),
            )

            total = int(cur.fetchone()["cnt"])

            # data
            offset = max(0, (page - 1) * page_size)
            cur.execute(
                f"""
                WITH latest_eval AS (
                  SELECT DISTINCT ON (claim_id) claim_id, bucket
                  FROM ai_claim_evaluations
                  ORDER BY claim_id, evaluated_at DESC
                )
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified,
                       t.id AS task_row_id, t.task_id,
                       t.contract_address,
                       t.required_validators,
                       t.tx_hash,
                       t.reward_pol::text AS reward_pol,
                       t.status,
                       t.created_at
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE {where_sql}{exclude_sql}
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [page_size, offset]),
            )

            rows = cur.fetchall()
            items = [
                VerificationQueueItem(
                    claim_id=r["claim_id"],
                    patient_id=r["patient_id"],
                    insurance_id=r["insurance_id"],
                    report_url=r["report_url"],
                    is_verified=r["is_verified"],
                    task_row_id=r["task_row_id"],
                    task_id=r["task_id"],
                    contract_address=r.get("contract_address"),
                    required_validators=r.get("required_validators"),
                    tx_hash=r.get("tx_hash"),
                    reward_pol=(str(r.get("reward_pol")) if r.get("reward_pol") is not None else None),
                    status=r.get("status"),
                    created_at=r["created_at"],
                ) for r in rows
            ]
            return VerificationQueueResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_verification_queue failed: {str(e)}")


# ---- New: Record AI evaluations for claims ----
@router.post("/claims/ai-evaluations")
async def record_ai_evaluations(body: AIEvaluationBulkRequest):
    """Insert AI evaluation rows for given claims into ai_claim_evaluations table."""
    evals = body.evaluations or []
    if not evals:
        raise HTTPException(status_code=400, detail="evaluations cannot be empty")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            insert_sql = (
                """
                INSERT INTO ai_claim_evaluations (claim_id, report_type, document_url, ai_score, bucket, evaluated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING id
                """
            )
            for e in evals:
                cursor.execute(insert_sql, (e.claim_id, e.report_type, e.document_url, int(e.ai_score), (e.bucket or None)))
                # If bucket is 'auto', immediately mark claim as verified and approved
                if e.bucket and str(e.bucket).lower() == 'auto':
                    cursor.execute(
                        """
                        UPDATE claims
                        SET is_verified = TRUE, status = 'approved'
                        WHERE claim_id = %s
                        """,
                        (e.claim_id,)
                    )
            conn.commit()
            return {"ok": True, "count": len(evals)}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to record AI evaluations: {str(e)}")


# ---- New: Fetch latest AI evaluation per claim ----
@router.post("/claims/ai-evaluations/query")
async def fetch_ai_evaluations(body: AIEvalFetchRequest) -> List[AIEvalRecord]:
    ids = body.claim_ids or []
    if not ids:
        return []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Use DISTINCT ON to get latest row per claim_id by evaluated_at
            sql = (
                """
                SELECT DISTINCT ON (claim_id)
                    claim_id, ai_score, bucket, evaluated_at
                FROM ai_claim_evaluations
                WHERE claim_id = ANY(%s)
                ORDER BY claim_id, evaluated_at DESC
                """
            )
            cursor.execute(sql, (ids,))
            rows = cursor.fetchall()
            out: List[AIEvalRecord] = []
            for r in rows:
                out.append(
                    AIEvalRecord(
                        claim_id=r["claim_id"],
                        ai_score=int(r["ai_score"]),
                        bucket=r.get("bucket"),
                        evaluated_at=r["evaluated_at"],
                    )
                )
            return out
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch AI evaluations: {str(e)}")


# ---- New: Bulk approve external claims (status only) ----
class BulkVerifyApproveRequest(BaseModel):
    claim_ids: List[int]

@router.post("/claims/bulk-verify-approve")
async def bulk_verify_approve(body: BulkVerifyApproveRequest):
    ids = body.claim_ids or []
    if not ids:
        raise HTTPException(status_code=400, detail="claim_ids cannot be empty")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Do not set is_verified here to avoid CHECK constraint with issued_by being NULL for external claims.
            sql = "UPDATE claims SET status = 'approved' WHERE claim_id = ANY(%s) RETURNING claim_id"
            cursor.execute(sql, (ids,))
            rows = cursor.fetchall()
            if not rows:
                conn.rollback()
                raise HTTPException(status_code=404, detail="no claims updated")
            conn.commit()
            return {"ok": True, "updated": [r["claim_id"] for r in rows]}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk approve failed: {str(e)}")


# ---------------------------- Web3 Integration ----------------------------
# NOTE: Web3 contracts endpoints are defined in `app/api/web3/contracts.py` using the unified
# `contracts` table (validate_contract, ai_contract). Duplicate endpoints previously defined here
# have been removed to avoid conflicts.


class SaveTaskRequest(BaseModel):
    user_id: int
    contract_address: str
    task_id: int
    doc_cid: str
    required_validators: int
    # Accept POL as string to preserve precision for NUMERIC(36,18)
    reward_pol: str
    tx_hash: Optional[str] = None
    claim_id: int
    status: Optional[str] = None  # e.g., pending | completed | cancelled


@router.post("/web3/tasks")
async def save_task(body: SaveTaskRequest):
    """Persist an on-chain task metadata."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Normalize reward to 3 decimals at persistence time (store trimmed value)
            norm_reward: Optional[str] = None
            if body.reward_pol is not None and str(body.reward_pol).strip() != "":
                try:
                    norm_reward = str(Decimal(str(body.reward_pol)).quantize(Decimal("0.001"), rounding=ROUND_DOWN))
                except Exception:
                    # Fallback: attempt float then Decimal; if still fails, set None
                    try:
                        norm_reward = str(Decimal(str(float(body.reward_pol))).quantize(Decimal("0.001"), rounding=ROUND_DOWN))
                    except Exception:
                        norm_reward = None
            cur.execute(
                """
                INSERT INTO tasks (user_id, contract_address, task_id, doc_cid, required_validators, reward_pol, claim_id, status, tx_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'pending'), %s)
                RETURNING id
                """,
                (
                    body.user_id,
                    body.contract_address,
                    body.task_id,
                    body.doc_cid,
                    body.required_validators,
                    norm_reward,
                    body.claim_id,
                    body.status,
                    body.tx_hash,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return {"ok": True, "id": row["id"]}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"save_task failed: {str(e)}")


class TaskStatusUpdateRequest(BaseModel):
    status: str  # expected values: pending | completed | cancelled
    tx_hash: Optional[str] = None


@router.patch("/tasks/{task_id}/status")
async def update_task_status(task_id: int, body: TaskStatusUpdateRequest):
    """Update a task row by on-chain task_id, set status and optionally store the latest tx_hash."""
    status = (body.status or "").lower()
    if status not in ("pending", "completed", "cancelled"):
        raise HTTPException(status_code=400, detail="invalid status")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE tasks
                SET status = %s,
                    tx_hash = COALESCE(%s, tx_hash)
                WHERE task_id = %s
                RETURNING id, task_id, status, tx_hash
                """,
                (status, (body.tx_hash or None), task_id),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                raise HTTPException(status_code=404, detail="task not found")
            conn.commit()
            return {"ok": True, "task_id": row["task_id"], "status": row["status"], "tx_hash": row.get("tx_hash")}
        finally:
            cur.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"update_task_status failed: {str(e)}")


class CompletedTaskItem(BaseModel):
    task_id: int
    claim_id: int
    insurance_id: Optional[int] = None
    company_name: Optional[str] = None
    contract_address: Optional[str] = None
    reward_pol: Optional[str] = None
    tx_hash: Optional[str] = None
    status: Optional[str] = None
    created_at: datetime
    report_url: Optional[str] = None
    required_validators: Optional[int] = None
    last_submission_created_at: Optional[datetime] = None
    last_submission_result_cid: Optional[str] = None
    last_submission_tx_hash: Optional[str] = None


class CompletedTasksResponse(BaseModel):
    items: List[CompletedTaskItem]
    total: int
    page: int
    page_size: int


@router.get("/tasks/completed", response_model=CompletedTasksResponse)
async def list_completed_tasks(
    request: Request,
    insurance_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    only_mine: bool = False,
    validator_user_id: Optional[int] = None,
    wallet_address: Optional[str] = None,
):
    """List completed tasks with optional insurance filter and simple search by claim_id or report_url.
    Includes insurance company_name and latest submission details.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            where = ["t.status = 'completed'"]
            params: List[object] = []

            # Optional insurance filter
            if insurance_id is not None:
                where.append("c.insurance_id = %s")
                params.append(insurance_id)

            # Optional search filter
            if search:
                where.append("(CAST(c.claim_id AS TEXT) ILIKE %s OR c.report_url ILIKE %s)")
                like = f"%{search}%"
                params.extend([like, like])

            # Optional: restrict to tasks where current validator submitted
            uid: Optional[int] = None
            if only_mine:
                cookie_uid = request.cookies.get("user_id")
                if cookie_uid:
                    try:
                        uid = int(cookie_uid)
                    except Exception:
                        uid = None
                if not uid and validator_user_id is not None:
                    try:
                        uid = int(validator_user_id)
                    except Exception:
                        uid = None
                if not uid and wallet_address:
                    cur.execute(
                        "SELECT user_id FROM users WHERE LOWER(wallet_address) = LOWER(%s) LIMIT 1",
                        (wallet_address.strip(),),
                    )
                    u = cur.fetchone()
                    if u:
                        uid = u["user_id"]
                # If only_mine is requested but we cannot resolve uid, return empty page
                if not uid:
                    return CompletedTasksResponse(items=[], total=0, page=page, page_size=page_size)
                where.append("EXISTS (SELECT 1 FROM validator_submissions vs WHERE vs.task_id = t.task_id AND vs.validator_user_id = %s)")
                params.append(uid)

            where_sql = " AND ".join(where)

            # Count
            cur.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM tasks t
                LEFT JOIN claims c ON c.claim_id = t.claim_id
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total = int(cur.fetchone()["cnt"])

            # Data
            offset = max(0, (page - 1) * page_size)
            cur.execute(
                f"""
                WITH last_sub AS (
                  SELECT DISTINCT ON (task_id)
                         task_id, created_at, result_cid, tx_hash
                  FROM validator_submissions
                  ORDER BY task_id, created_at DESC
                )
                SELECT t.task_id, t.claim_id, t.contract_address, t.reward_pol::text AS reward_pol,
                       t.tx_hash, t.status, t.created_at,
                       t.required_validators,
                       c.insurance_id, c.report_url,
                       i.company_name,
                       ls.created_at AS last_submission_created_at,
                       ls.result_cid   AS last_submission_result_cid,
                       ls.tx_hash      AS last_submission_tx_hash
                FROM tasks t
                LEFT JOIN claims c ON c.claim_id = t.claim_id
                LEFT JOIN insurance_basic_info i ON i.insurance_id = c.insurance_id
                LEFT JOIN last_sub ls ON ls.task_id = t.task_id
                WHERE {where_sql}
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [page_size, offset]),
            )
            rows = cur.fetchall()
            items = [
                CompletedTaskItem(
                    task_id=r["task_id"],
                    claim_id=r["claim_id"],
                    insurance_id=r.get("insurance_id"),
                    company_name=r.get("company_name"),
                    contract_address=r.get("contract_address"),
                    reward_pol=(str(r.get("reward_pol")) if r.get("reward_pol") is not None else None),
                    tx_hash=r.get("tx_hash"),
                    status=r.get("status"),
                    created_at=r["created_at"],
                    report_url=r.get("report_url"),
                    required_validators=r.get("required_validators"),
                    last_submission_created_at=r.get("last_submission_created_at"),
                    last_submission_result_cid=r.get("last_submission_result_cid"),
                    last_submission_tx_hash=r.get("last_submission_tx_hash"),
                )
                for r in rows
            ]
            return CompletedTasksResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"list_completed_tasks failed: {str(e)}")

class ValidatorSubmissionCreate(BaseModel):
    task_id: int
    result_cid: str
    tx_hash: Optional[str] = None
    validator_user_id: Optional[int] = None  # optional if wallet_address provided
    wallet_address: Optional[str] = None     # optional helper to resolve user_id


class ValidatorSubmissionResponse(BaseModel):
    id: int
    task_id: int
    validator_user_id: int
    result_cid: str
    tx_hash: Optional[str] = None
    status: str
    created_at: datetime
    task_completed: bool
    total_submissions: int
    required_validators: int


@router.post("/validator/submissions", response_model=ValidatorSubmissionResponse)
async def create_validator_submission(request: Request, body: ValidatorSubmissionCreate):
    """Record a validator submission for a task.
    Also updates the task status to completed if submissions >= required_validators.
    Requires either validator_user_id or wallet_address (which will be resolved to user_id).
    """
    # Prefer user_id from cookie; fall back to payload fields
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Resolve user_id: cookie > explicit id > wallet lookup
            user_id = None
            cookie_uid = request.cookies.get("user_id")
            if cookie_uid:
                try:
                    user_id = int(cookie_uid)
                except Exception:
                    user_id = None
            if not user_id and body.validator_user_id:
                user_id = int(body.validator_user_id)
            if not user_id and body.wallet_address:
                cur.execute(
                    "SELECT user_id FROM users WHERE LOWER(wallet_address) = LOWER(%s) LIMIT 1",
                    (body.wallet_address.strip(),),
                )
                u = cur.fetchone()
                if not u:
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="user not found for wallet")
                user_id = u["user_id"]
            if not user_id:
                conn.rollback()
                raise HTTPException(status_code=400, detail="Unable to resolve validator user id")

            # Get required_validators for the task and ensure task exists
            cur.execute(
                "SELECT required_validators FROM tasks WHERE task_id = %s LIMIT 1",
                (body.task_id,),
            )
            t = cur.fetchone()
            if not t:
                conn.rollback()
                raise HTTPException(status_code=404, detail="task not found")
            required_validators = int(t["required_validators"])

            # Insert submission (idempotent per (task_id, validator_user_id))
            cur.execute(
                """
                INSERT INTO validator_submissions (task_id, validator_user_id, result_cid, tx_hash, status)
                VALUES (%s, %s, %s, %s, 'submitted')
                ON CONFLICT (task_id, validator_user_id) DO UPDATE
                    SET result_cid = EXCLUDED.result_cid,
                        tx_hash = COALESCE(EXCLUDED.tx_hash, validator_submissions.tx_hash),
                        status = 'submitted',
                        updated_at = NOW()
                RETURNING id, task_id, validator_user_id, result_cid, tx_hash, status, created_at
                """,
                (body.task_id, user_id, body.result_cid, (body.tx_hash or None)),
            )
            row = cur.fetchone()

            # Count submissions for this task
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM validator_submissions WHERE task_id = %s",
                (body.task_id,),
            )
            count = int(cur.fetchone()["cnt"])

            task_completed = False
            if count >= required_validators:
                # Mark task completed if not already
                cur.execute(
                    "UPDATE tasks SET status = 'completed' WHERE task_id = %s AND status <> 'completed'",
                    (body.task_id,),
                )
                task_completed = True

            conn.commit()
            return ValidatorSubmissionResponse(
                id=row["id"],
                task_id=row["task_id"],
                validator_user_id=row["validator_user_id"],
                result_cid=row["result_cid"],
                tx_hash=row.get("tx_hash"),
                status=row["status"],
                created_at=row["created_at"],
                task_completed=task_completed,
                total_submissions=count,
                required_validators=required_validators,
            )
        finally:
            cur.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"create_validator_submission failed: {str(e)}")


# ---- List validator submissions by task ----
class ValidatorSubmissionListItem(BaseModel):
    id: int
    task_id: int
    validator_user_id: int
    result_cid: str
    tx_hash: Optional[str] = None
    status: str
    created_at: datetime
    wallet_address: Optional[str] = None


class ValidatorSubmissionsByTaskResponse(BaseModel):
    items: List[ValidatorSubmissionListItem]


@router.get("/validator/submissions/by-task/{task_id}", response_model=ValidatorSubmissionsByTaskResponse)
async def list_validator_submissions_by_task(task_id: int, include_user: bool = True):
    """List all validator submissions for a given task.
    Optionally includes the submitter's wallet_address when include_user is True.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if include_user:
                cur.execute(
                    
                    """
                    SELECT vs.id, vs.task_id, vs.validator_user_id, vs.result_cid, vs.tx_hash, vs.status, vs.created_at,
                           u.wallet_address
                    FROM validator_submissions vs
                    LEFT JOIN users u ON u.user_id = vs.validator_user_id
                    WHERE vs.task_id = %s
                    ORDER BY vs.created_at DESC
                    """,
                    (task_id,),
                )
            else:
                cur.execute(
                    
                    """
                    SELECT vs.id, vs.task_id, vs.validator_user_id, vs.result_cid, vs.tx_hash, vs.status, vs.created_at,
                           NULL::text AS wallet_address
                    FROM validator_submissions vs
                    WHERE vs.task_id = %s
                    ORDER BY vs.created_at DESC
                    """,
                    (task_id,),
                )
            rows = cur.fetchall()
            items = [
                ValidatorSubmissionListItem(
                    id=r["id"],
                    task_id=r["task_id"],
                    validator_user_id=r["validator_user_id"],
                    result_cid=r["result_cid"],
                    tx_hash=r.get("tx_hash"),
                    status=r["status"],
                    created_at=r["created_at"],
                    wallet_address=r.get("wallet_address"),
                )
                for r in rows
            ]
            return ValidatorSubmissionsByTaskResponse(items=items)
        finally:
            cur.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"list_validator_submissions_by_task failed: {str(e)}")


class ActiveValidationItem(BaseModel):
    task_id: int
    claim_id: Optional[int] = None
    required_validators: int
    current_submissions: int
    contract_address: Optional[str] = None
    reward_pol: Optional[str] = None
    status: str
    created_at: datetime
    report_url: Optional[str] = None
    company_name: Optional[str] = None
    my_submission_created_at: Optional[datetime] = None
    my_submission_result_cid: Optional[str] = None
    my_submission_tx_hash: Optional[str] = None


class ActiveValidationsResponse(BaseModel):
    items: List[ActiveValidationItem]
    total: int
    page: int
    page_size: int


@router.get("/validator/active", response_model=ActiveValidationsResponse)
async def list_active_validations(
    request: Request,
    wallet_address: Optional[str] = None,
    validator_user_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
):
    """List tasks where the given validator has submitted but task is not yet completed."""
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Determine uid: cookie > validator_user_id > wallet
            uid: Optional[int] = None
            cookie_uid = request.cookies.get("user_id")
            if cookie_uid:
                try:
                    uid = int(cookie_uid)
                except Exception:
                    uid = None
            if not uid and validator_user_id:
                uid = int(validator_user_id)
            if not uid and wallet_address:
                cur.execute(
                    "SELECT user_id FROM users WHERE LOWER(wallet_address) = LOWER(%s) LIMIT 1",
                    (wallet_address.strip(),),
                )
                u = cur.fetchone()
                if not u:
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="user not found for wallet")
                uid = u["user_id"]
            if not uid:
                raise HTTPException(status_code=400, detail="Unable to resolve validator user id")

            # Total
            cur.execute(
                """
                SELECT COUNT(DISTINCT t.task_id) AS cnt
                FROM tasks t
                JOIN validator_submissions vs ON vs.task_id = t.task_id AND vs.validator_user_id = %s
                WHERE t.status <> 'completed'
                """,
                (uid,),
            )
            total = int(cur.fetchone()["cnt"])

            offset = max(0, (page - 1) * page_size)
            cur.execute(
                """
                WITH counts AS (
                    SELECT task_id, COUNT(*) AS c
                    FROM validator_submissions
                    GROUP BY task_id
                )
                SELECT t.task_id, t.claim_id, t.required_validators, COALESCE(c.c, 0) AS current_submissions,
                       t.contract_address, t.reward_pol::text AS reward_pol, t.status, t.created_at,
                       c2.report_url, i.company_name,
                       vs.created_at AS my_submission_created_at,
                       vs.result_cid AS my_submission_result_cid,
                       vs.tx_hash    AS my_submission_tx_hash
                FROM tasks t
                JOIN validator_submissions vs ON vs.task_id = t.task_id AND vs.validator_user_id = %s
                LEFT JOIN counts c ON c.task_id = t.task_id
                LEFT JOIN claims c2 ON c2.claim_id = t.claim_id
                LEFT JOIN insurance_basic_info i ON i.insurance_id = c2.insurance_id
                WHERE t.status <> 'completed'
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (uid, page_size, offset),
            )
            rows = cur.fetchall()
            items = [
                ActiveValidationItem(
                    task_id=r["task_id"],
                    claim_id=r.get("claim_id"),
                    required_validators=r["required_validators"],
                    current_submissions=r["current_submissions"],
                    contract_address=r.get("contract_address"),
                    reward_pol=(str(r.get("reward_pol")) if r.get("reward_pol") is not None else None),
                    status=r["status"],
                    created_at=r["created_at"],
                    report_url=r.get("report_url"),
                    company_name=r.get("company_name"),
                    my_submission_created_at=r.get("my_submission_created_at"),
                    my_submission_result_cid=r.get("my_submission_result_cid"),
                    my_submission_tx_hash=r.get("my_submission_tx_hash"),
                )
                for r in rows
            ]
            return ActiveValidationsResponse(items=items, total=total, page=page, page_size=page_size)
        finally:
            cur.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"list_active_validations failed: {str(e)}")

@router.get("/claims/unverified-without-task")
async def get_unverified_without_task(insurance_id: int, page: int = 1, page_size: int = 10, search: Optional[str] = None):
    """List unverified claims that do NOT have a task created."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            where = ["c.insurance_id = %s", "c.is_verified = FALSE"]
            params: list = [insurance_id]
            if search:
                where.append("(CAST(c.claim_id AS TEXT) ILIKE %s OR c.report_url ILIKE %s)")
                like = f"%{search}%"
                params.extend([like, like])
            where_sql = " AND ".join(where)

            # count
            cur.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE {where_sql} AND t.claim_id IS NULL
                """,
                tuple(params),
            )
            total = cur.fetchone()["cnt"]

            # data
            offset = max(0, (page - 1) * page_size)
            cur.execute(
                f"""
                SELECT c.*
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE {where_sql} AND t.claim_id IS NULL
                ORDER BY c.claim_id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [page_size, offset]),
            )
            rows = cur.fetchall()
            return {"ok": True, "total": total, "items": [dict(r) for r in rows]}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_unverified_without_task failed: {str(e)}")
