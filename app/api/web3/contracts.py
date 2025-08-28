from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ...database import execute_query

router = APIRouter()

class SaveContractPayload(BaseModel):
    user_id: int
    wallet_address: str
    ai_contract: str | None = None
    contract_address: str | None = None

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS web3_contracts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    wallet_address TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_w3_wallet ON web3_contracts(wallet_address);
"""

@router.get("/web3/contracts/by-wallet/{wallet}")
async def get_contract_by_wallet(wallet: str):
    try:
        execute_query(CREATE_TABLE_SQL)
    except Exception:
        pass
    row = execute_query(
        """
        SELECT user_id, wallet_address, contract_address AS ai_contract, created_at
        FROM web3_contracts
        WHERE lower(wallet_address) = lower(%s)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (wallet,),
        fetch='one'
    )
    if not row:
        raise HTTPException(status_code=404, detail="contract not found")
    return {"contract": row}

@router.post("/web3/contracts")
async def save_contract(payload: SaveContractPayload):
    try:
        execute_query(CREATE_TABLE_SQL)
    except Exception:
        pass
    address = payload.ai_contract or payload.contract_address
    if not address:
        raise HTTPException(status_code=400, detail="contract address required")
    execute_query(
        """
        INSERT INTO web3_contracts (user_id, wallet_address, contract_address)
        VALUES (%s, %s, %s)
        """,
        (payload.user_id, payload.wallet_address, address)
    )
    return {"status": "ok"}
