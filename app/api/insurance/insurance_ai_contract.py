from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ...database import execute_query

router = APIRouter()

class AIContractPayload(BaseModel):
    user_id: int
    wallet_address: str
    ai_contract: str

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS insurance_ai_contracts (
    id SERIAL PRIMARY KEY,
    insurance_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    wallet_address TEXT NOT NULL,
    ai_contract TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ins_ai_insurance ON insurance_ai_contracts(insurance_id);
CREATE INDEX IF NOT EXISTS idx_ins_ai_wallet ON insurance_ai_contracts(wallet_address);
"""

@router.get("/insurance/{insurance_id}/ai-contract")
async def get_ai_contract(insurance_id: int):
    # ensure table
    try:
        execute_query(CREATE_TABLE_SQL)
    except Exception:
        pass
    row = execute_query(
        """
        SELECT insurance_id, user_id, wallet_address, ai_contract, created_at
        FROM insurance_ai_contracts
        WHERE insurance_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (insurance_id,),
        fetch='one'
    )
    if not row:
        raise HTTPException(status_code=404, detail="ai_contract not found")
    return row

@router.post("/insurance/{insurance_id}/ai-contract")
async def save_ai_contract(insurance_id: int, payload: AIContractPayload):
    # ensure table
    try:
        execute_query(CREATE_TABLE_SQL)
    except Exception:
        pass
    execute_query(
        """
        INSERT INTO insurance_ai_contracts (insurance_id, user_id, wallet_address, ai_contract)
        VALUES (%s, %s, %s, %s)
        """,
        (insurance_id, payload.user_id, payload.wallet_address, payload.ai_contract)
    )
    return {"status": "ok"}
