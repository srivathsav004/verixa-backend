from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from ...database import execute_query

router = APIRouter()


class AIContractPayload(BaseModel):
    user_id: int
    wallet_address: str
    ai_contract: str


@router.get("/insurance/{insurance_id}/ai-contract")
async def get_ai_contract(insurance_id: int, wallet_address: str = Query(...), user_id: int | None = Query(None)):
    """
    Fetch the latest AI contract for a given wallet (optionally scoped by user) from the unified `contracts` table.
    Note: `insurance_id` is accepted for routing compatibility but storage is centralized in `contracts`.
    """
    params = [wallet_address]
    user_filter = ""
    if user_id is not None:
        user_filter = " AND user_id = %s"
        params.append(user_id)
    row = execute_query(
        f"""
        SELECT id, user_id, wallet_address, ai_contract, validate_contract, created_at
        FROM contracts
        WHERE LOWER(wallet_address) = LOWER(%s){user_filter}
        ORDER BY created_at DESC
        LIMIT 1
        """,
        tuple(params),
        fetch='one'
    )
    if not row:
        raise HTTPException(status_code=404, detail="ai_contract not found")
    return row


@router.post("/insurance/{insurance_id}/ai-contract")
async def save_ai_contract(insurance_id: int, payload: AIContractPayload):
    """
    Upsert the AI contract into the unified `contracts` table for (user_id, wallet_address).
    """
    if not payload.ai_contract:
        raise HTTPException(status_code=400, detail="ai_contract is required")

    existing = execute_query(
        """
        SELECT id FROM contracts
        WHERE user_id = %s AND LOWER(wallet_address) = LOWER(%s)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (payload.user_id, payload.wallet_address),
        fetch='one'
    )

    if existing:
        execute_query(
            """
            UPDATE contracts
            SET ai_contract = %s
            WHERE user_id = %s AND LOWER(wallet_address) = LOWER(%s)
            """,
            (payload.ai_contract, payload.user_id, payload.wallet_address)
        )
    else:
        execute_query(
            """
            INSERT INTO contracts (user_id, wallet_address, ai_contract)
            VALUES (%s, %s, %s)
            """,
            (payload.user_id, payload.wallet_address, payload.ai_contract)
        )

    return {"status": "ok"}
