from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ...database import execute_query

router = APIRouter()


class SaveContractPayload(BaseModel):
    user_id: int
    wallet_address: str
    # Provide one or both. Keep backward-compat: contract_address maps to validate_contract
    ai_contract: str | None = None
    validate_contract: str | None = None
    contract_address: str | None = None


@router.get("/web3/contracts/by-wallet/{wallet}")
async def get_contract_by_wallet(wallet: str):
    row = execute_query(
        """
        SELECT id, user_id, wallet_address, validate_contract, ai_contract, created_at
        FROM contracts
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
    # Map legacy field name if used (frontend uses explicit columns; this keeps compatibility only)
    validate_contract = payload.validate_contract or payload.contract_address
    ai_contract = payload.ai_contract
    if not (validate_contract or ai_contract):
        raise HTTPException(status_code=400, detail="ai_contract or validate_contract required")

    try:
        # Look for existing record for this user+wallet
        existing = execute_query(
            """
            SELECT id FROM contracts
            WHERE user_id = %s AND lower(wallet_address) = lower(%s)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (payload.user_id, payload.wallet_address),
            fetch='one'
        )

        if existing and (validate_contract or ai_contract):
            # Build dynamic update for provided fields only
            sets = []
            params: list[str | int] = []
            if validate_contract:
                sets.append("validate_contract = %s")
                params.append(validate_contract)
            if ai_contract:
                sets.append("ai_contract = %s")
                params.append(ai_contract)
            params.extend([payload.user_id, payload.wallet_address])
            execute_query(
                f"""
                UPDATE contracts
                SET {', '.join(sets)}
                WHERE user_id = %s AND lower(wallet_address) = lower(%s)
                """,
                tuple(params)
            )
        else:
            # Insert new record (only provided columns)
            cols = ["user_id", "wallet_address"]
            vals = [payload.user_id, payload.wallet_address]
            placeholders = ["%s", "%s"]
            if validate_contract:
                cols.append("validate_contract")
                vals.append(validate_contract)
                placeholders.append("%s")
            if ai_contract:
                cols.append("ai_contract")
                vals.append(ai_contract)
                placeholders.append("%s")
            execute_query(
                f"""
                INSERT INTO contracts ({', '.join(cols)})
                VALUES ({', '.join(placeholders)})
                """,
                tuple(vals)
            )
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"contracts upsert failed: {e}")
