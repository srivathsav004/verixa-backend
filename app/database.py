import os
from dotenv import load_dotenv
import pyodbc
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# --- SQL SERVER SETUP ---
DB_SERVER = os.getenv("DB_SERVER")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --- SUPABASE SETUP ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

# Initialize Supabase client
supabase: Client = None
if SUPABASE_URL and SUPABASE_ANON_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    print(f"✅ SUPABASE: Client initialized successfully")
else:
    print(f"❌ SUPABASE: Missing environment variables (SUPABASE_URL or SUPABASE_ANON_KEY)")

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_SERVER},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD}"
)

def get_db_connection():
    """Get SQL Server database connection"""
    try:
        conn = pyodbc.connect(connection_string)
        conn.autocommit = False  # Explicitly set autocommit to False for transaction control
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise e

def execute_query(query, params=None, fetch=False):
    """Execute SQL query with optional parameters"""
    conn = None
    cursor = None
    try:
        print(f"🔍 DATABASE: Attempting to connect...")
        conn = get_db_connection()
        print(f"✅ DATABASE: Connected successfully")
        cursor = conn.cursor()
        
        print(f"🔍 DATABASE: Executing query with params: {params}")
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if fetch:
            if fetch == 'one':
                result = cursor.fetchone()
                print(f"🔍 DATABASE: Fetched one result: {result}")
                # For INSERT with OUTPUT, we still need to commit the transaction
                conn.commit()
                print(f"✅ DATABASE: Transaction committed after fetch")
            else:
                result = cursor.fetchall()
                print(f"🔍 DATABASE: Fetched all results: {result}")
                conn.commit()
                print(f"✅ DATABASE: Transaction committed after fetch")
        else:
            conn.commit()
            result = cursor.rowcount
            print(f"🔍 DATABASE: Committed, affected rows: {result}")
            
        return result
    except Exception as e:
        if conn:
            conn.rollback()
            print(f"❌ DATABASE: Rolled back transaction")
        print(f"❌ DATABASE: Query execution failed: {e}")
        print(f"❌ DATABASE: Error type: {type(e)}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print(f"🔍 DATABASE: Connection closed")

def get_supabase_client() -> Client:
    """Get Supabase client instance"""
    if supabase is None:
        raise Exception("Supabase client not initialized. Check environment variables.")
    return supabase

def upload_file_to_supabase(file_data: bytes, file_name: str, bucket: str = "verixa-documents") -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        print(f"🔍 SUPABASE: Uploading file {file_name} to bucket {bucket}")
        
        # Upload file to Supabase storage
        result = supabase.storage.from_(bucket).upload(
            path=file_name,
            file=file_data,
            file_options={"content-type": "application/octet-stream"}
        )
        
        # Modern Supabase client - check for successful upload
        print(f"🔍 SUPABASE: Upload result type: {type(result)}")
        print(f"🔍 SUPABASE: Upload result: {result}")
        
        # Get public URL (upload was successful if no exception was raised)
        public_url = supabase.storage.from_(bucket).get_public_url(file_name)
        print(f"✅ SUPABASE: File uploaded successfully. URL: {public_url}")
        return public_url
        
    except Exception as e:
        print(f"❌ SUPABASE: File upload error: {e}")
        raise e

def delete_file_from_supabase(file_path: str, bucket: str = "verixa-documents") -> bool:
    """Delete file from Supabase storage"""
    try:
        print(f"🔍 SUPABASE: Deleting file {file_path} from bucket {bucket}")
        
        result = supabase.storage.from_(bucket).remove([file_path])
        
        if result.error:
            print(f"❌ SUPABASE: Delete failed: {result.error}")
            return False
        
        print(f"✅ SUPABASE: File deleted successfully")
        return True
        
    except Exception as e:
        print(f"❌ SUPABASE: File delete error: {e}")
        return False
