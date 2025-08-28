from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.users import router as users_router
from .api.login import router as login_router
from .api.issuer.issuer_basic_info import router as issuer_basic_info_router
from .api.issuer.issuer_documents import router as issuer_documents_router
from .api.issuer.issuer_report_formats import router as issuer_report_formats_router
from .api.issuer.issuer_issued_medical_docs import router as issuer_issued_docs_router
from .api.patient.patient_basic_info import router as patient_basic_info_router
from .api.patient.patient_identity_insurance import router as patient_identity_insurance_router
from .api.insurance.insurance_basic_info import router as insurance_basic_info_router
from .api.insurance.insurance_business_info import router as insurance_business_info_router
from .api.insurance.insurance_contact_tech import router as insurance_contact_tech_router
from .api.insurance.insurance_documents import router as insurance_documents_router
from .api.insurance.insurance_ai_contract import router as insurance_ai_contract_router
from .api.web3.contracts import router as web3_contracts_router
from .api.validator.validator_basic_info import router as validator_basic_info_router
from .api.validator.validator_documents import router as validator_documents_router
from .api.payments import router as payments_router
from .api.claims import router as claims_router
from .database import get_db_connection
import pyodbc

app = FastAPI(title="Verixa Backend API", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000","https://verixa.vercel.app"],  # Frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(users_router, prefix="/api", tags=["users"])
app.include_router(login_router, prefix="/api", tags=["auth"])
app.include_router(issuer_basic_info_router, prefix="/api", tags=["issuer-basic-info"])
app.include_router(issuer_documents_router, prefix="/api", tags=["issuer-documents"])
app.include_router(issuer_report_formats_router, prefix="/api", tags=["issuer-report-formats"])
app.include_router(issuer_issued_docs_router, prefix="/api", tags=["issuer-issued-docs"])
app.include_router(patient_basic_info_router, prefix="/api", tags=["patient-basic-info"])
app.include_router(patient_identity_insurance_router, prefix="/api", tags=["patient-identity-insurance"])
app.include_router(insurance_basic_info_router, prefix="/api", tags=["insurance-basic-info"])
app.include_router(insurance_business_info_router, prefix="/api", tags=["insurance-business-info"])
app.include_router(insurance_contact_tech_router, prefix="/api", tags=["insurance-contact-tech"])
app.include_router(insurance_documents_router, prefix="/api", tags=["insurance-documents"])
app.include_router(insurance_ai_contract_router, prefix="/api", tags=["insurance-ai-contract"])
app.include_router(web3_contracts_router, prefix="/api", tags=["web3-contracts"])
app.include_router(validator_basic_info_router, prefix="/api", tags=["validator-basic-info"])
app.include_router(validator_documents_router, prefix="/api", tags=["validator-documents"])
app.include_router(claims_router, prefix="/api", tags=["claims"])
app.include_router(payments_router, prefix="/api", tags=["payments"])

@app.get("/")
async def root():
    return {"message": "Verixa Backend API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/test-db")
async def test_database():
    """Test database connection"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE();")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"status": "success", "message": f"Database connected. Current time: {row[0]}"}
    except Exception as e:
        return {"status": "error", "message": f"Database connection failed: {str(e)}"}