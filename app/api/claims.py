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
    """List external unverified, pending claims for an insurance excluding items bucketed as 'manual'.
    Conditions:
      - claims.is_verified = FALSE
      - claims.issued_by IS NULL
      - claims.status = 'pending'
      - latest AI bucket is NOT 'manual' (or there is no AI evaluation yet)
    Supports simple search on report_url.
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Build common filter with latest AI eval excluding 'manual'
            # latest_eval subquery returns the latest ai_claim_evaluations row per claim
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
                LEFT JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.issued_by IS NULL
                  AND c.status = 'pending'
                  AND (le.bucket IS NULL OR LOWER(le.bucket) <> 'manual')
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
                WITH latest_eval AS (
                    SELECT DISTINCT ON (claim_id) claim_id, bucket
                    FROM ai_claim_evaluations
                    ORDER BY claim_id, evaluated_at DESC
                )
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified, c.issued_by, c.status, c.created_at
                FROM claims c
                LEFT JOIN latest_eval le ON le.claim_id = c.claim_id
                WHERE c.insurance_id = %s
                  AND c.is_verified = FALSE
                  AND c.issued_by IS NULL
                  AND c.status = 'pending'
                  AND (le.bucket IS NULL OR LOWER(le.bucket) <> 'manual')
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
      - claims.issued_by IS NULL
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
                  AND c.issued_by IS NULL
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
                  AND c.issued_by IS NULL
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
    task_id: int
    contract_address: str
    task_status: Optional[str] = None
    created_at: datetime


class VerificationQueueResponse(BaseModel):
    items: List[VerificationQueueItem]
    total: int
    page: int
    page_size: int


@router.get("/verification-queue", response_model=VerificationQueueResponse)
async def get_verification_queue(insurance_id: int, page: int = 1, page_size: int = 10, search: Optional[str] = None):
    """Return claims (pending, unverified) that already have a task, joined with task info."""
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="invalid pagination params")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            where = [
                "c.insurance_id = %s",
                "c.is_verified = FALSE",
                "c.status = 'pending'",
                "t.claim_id IS NOT NULL",
            ]
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
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total = int(cur.fetchone()["cnt"])

            # data
            offset = max(0, (page - 1) * page_size)
            cur.execute(
                f"""
                SELECT c.claim_id, c.patient_id, c.insurance_id, c.report_url, c.is_verified,
                       t.id AS task_row_id, t.task_id, t.contract_address, t.task_status,
                       c.created_at
                FROM claims c
                LEFT JOIN tasks t ON t.claim_id = c.claim_id
                WHERE {where_sql}
                ORDER BY c.created_at DESC
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
                    contract_address=r["contract_address"],
                    task_status=r.get("task_status"),
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
class SaveContractRequest(BaseModel):
    user_id: int
    wallet_address: str
    contract_address: str


@router.post("/web3/contracts")
async def save_contract(body: SaveContractRequest):
    """Persist a deployed contract address for a user."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Enforce: only one contract per wallet. If exists, return 409.
            cur.execute(
                "SELECT id, contract_address FROM contracts WHERE wallet_address = %s ORDER BY created_at DESC LIMIT 1",
                (body.wallet_address,),
            )
            existing = cur.fetchone()
            if existing:
                raise HTTPException(status_code=409, detail="contract already exists for this wallet")
            cur.execute(
                """
                INSERT INTO contracts (user_id, wallet_address, contract_address)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (body.user_id, body.wallet_address, body.contract_address),
            )
            row = cur.fetchone()
            conn.commit()
            return {"ok": True, "id": row["id"]}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"save_contract failed: {str(e)}")


@router.get("/web3/contracts/by-user/{user_id}")
async def get_contract_by_user(user_id: int):
    """Fetch the most recent contract saved for a user."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, user_id, wallet_address, contract_address, created_at
                FROM contracts
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": True, "contract": None}
            return {"ok": True, "contract": dict(row)}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_contract_by_user failed: {str(e)}")


@router.get("/web3/contracts/by-wallet/{wallet}")
async def get_contract_by_wallet(wallet: str):
    """Fetch the most recent contract saved for a wallet address."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, user_id, wallet_address, contract_address, created_at
                FROM contracts
                WHERE LOWER(wallet_address) = LOWER(%s)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (wallet,),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": True, "contract": None}
            return {"ok": True, "contract": dict(row)}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_contract_by_wallet failed: {str(e)}")


class SaveTaskRequest(BaseModel):
    user_id: int
    contract_address: str
    task_id: int
    doc_cid: str
    required_validators: int
    reward_wei: int
    issuer_wallet: str
    tx_hash: Optional[str] = None
    claim_id: int
    task_status: Optional[str] = None  # e.g., pending, active, completed


@router.post("/web3/tasks")
async def save_task(body: SaveTaskRequest):
    """Persist an on-chain task metadata."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO tasks (user_id, contract_address, task_id, doc_cid, required_validators, reward_wei, issuer_wallet, tx_hash, claim_id, task_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'pending'))
                RETURNING id
                """,
                (
                    body.user_id,
                    body.contract_address,
                    body.task_id,
                    body.doc_cid,
                    body.required_validators,
                    body.reward_wei,
                    body.issuer_wallet,
                    body.tx_hash,
                    body.claim_id,
                    body.task_status,
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
