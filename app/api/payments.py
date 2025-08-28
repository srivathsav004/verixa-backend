from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from ..database import execute_query

router = APIRouter()


class PaymentItem(BaseModel):
    sender_user_id: Optional[int] = None
    receiver_user_id: Optional[int] = None
    sender_wallet: str
    receiver_wallet: str
    amount_pol: float
    tx_hash: str
    payment_type: str  # e.g., "ai_score"
    claim_id: Optional[int] = None
    task_id: Optional[int] = None


class PaymentsRequest(BaseModel):
    payments: List[PaymentItem]


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    sender_user_id INTEGER NULL,
    receiver_user_id INTEGER NULL,
    sender_wallet TEXT NOT NULL,
    receiver_wallet TEXT NOT NULL,
    amount_pol NUMERIC(36, 18) NOT NULL,
    tx_hash TEXT NOT NULL,
    payment_type TEXT NOT NULL,
    claim_id BIGINT NULL,
    task_id BIGINT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_payments_tx_hash ON payments(tx_hash);
CREATE INDEX IF NOT EXISTS idx_payments_sender ON payments(sender_wallet);
CREATE INDEX IF NOT EXISTS idx_payments_receiver ON payments(receiver_wallet);
"""


@router.post("/payments")
async def record_payments(body: PaymentsRequest):
    """Record one or more payment rows.
    Stores amounts in POL (decimal) and raw tx_hash.
    """
    try:
        try:
            execute_query(CREATE_TABLE_SQL)
        except Exception:
            # Ignore table creation race conditions
            pass

        if not body.payments:
            raise HTTPException(status_code=400, detail="payments cannot be empty")

        # Batch insert
        insert_sql = (
            """
            INSERT INTO payments (
                sender_user_id, receiver_user_id, sender_wallet, receiver_wallet,
                amount_pol, tx_hash, payment_type, claim_id, task_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
        )

        inserted = 0
        for p in body.payments:
            try:
                execute_query(
                    insert_sql,
                    (
                        p.sender_user_id,
                        p.receiver_user_id,
                        p.sender_wallet,
                        p.receiver_wallet,
                        float(p.amount_pol),
                        p.tx_hash,
                        p.payment_type,
                        p.claim_id,
                        p.task_id,
                    ),
                    fetch='one',
                )
                inserted += 1
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"insert failed: {str(e)}")

        return {"ok": True, "inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"record_payments failed: {str(e)}")


class PaymentExistQuery(BaseModel):
    sender_user_id: int
    receiver_user_id: int
    claim_id: int
    payment_type: str

class PaymentsExistenceRequest(BaseModel):
    queries: List[PaymentExistQuery]

@router.post("/payments/existence")
async def payments_existence(body: PaymentsExistenceRequest):
    """Return which (sender_user_id, receiver_user_id, claim_id, payment_type) exist in payments."""
    try:
        if not body.queries:
            return {"items": []}
        sql = (
            """
            SELECT sender_user_id, receiver_user_id, claim_id, payment_type
            FROM payments
            WHERE (sender_user_id, receiver_user_id, claim_id, payment_type) IN (
                %s
            )
            """
        )
        # Build tuple list placeholder
        keys = [(q.sender_user_id, q.receiver_user_id, q.claim_id, q.payment_type) for q in body.queries]
        # psycopg2 cannot expand list of tuples in a single %s; build OR chain safely
        ors = []
        params = []
        for t in keys:
            ors.append("(sender_user_id = %s AND receiver_user_id = %s AND claim_id = %s AND payment_type = %s)")
            params.extend(list(t))
        check_sql = (
            "SELECT sender_user_id, receiver_user_id, claim_id, payment_type FROM payments WHERE "
            + " OR ".join(ors)
        )
        rows = execute_query(check_sql, tuple(params), fetch='all') or []
        found = set((r["sender_user_id"], r["receiver_user_id"], r["claim_id"], r["payment_type"]) for r in rows)
        items = []
        for q in body.queries:
            key = (q.sender_user_id, q.receiver_user_id, q.claim_id, q.payment_type)
            items.append({"sender_user_id": q.sender_user_id, "receiver_user_id": q.receiver_user_id, "claim_id": q.claim_id, "payment_type": q.payment_type, "exists": key in found})
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"payments_existence failed: {str(e)}")
