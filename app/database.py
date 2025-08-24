import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from supabase import create_client, Client

# Load env
load_dotenv()
print("DEBUG DATABASE_URL:", os.getenv("DATABASE_URL"))

# --- SUPABASE POSTGRES DB ---
DATABASE_URL = os.getenv("DATABASE_URL")

# --- SUPABASE STORAGE ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

# Initialize Supabase storage client
supabase: Client = None
if SUPABASE_URL and SUPABASE_ANON_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    print("✅ SUPABASE: Client initialized successfully")
else:
    print("❌ SUPABASE: Missing SUPABASE_URL or SUPABASE_ANON_KEY")

# --- DB CONNECTION ---
def get_db_connection():
    """Get Supabase Postgres connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        conn.autocommit = False
        # Success log similar to Supabase client init
        try:
            params = conn.get_dsn_parameters()
            host = params.get("host", "?")
            dbname = params.get("dbname", "?")
            print(f"✅ DATABASE: Connection established to {host}/{dbname}")
        except Exception:
            # If fetching parameters fails, still proceed silently
            print("✅ DATABASE: Connection established")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise e

def execute_query(query, params=None, fetch=False):
    """Execute query in Supabase Postgres"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(query, params or ())

        result = None
        if fetch:
            if fetch == 'one':
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
        else:
            # For non-fetch operations, expose affected rows for caller logic (e.g., DELETE)
            result = cursor.rowcount

        conn.commit()
        return result
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Query failed: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# --- SUPABASE STORAGE HELPERS ---
def get_supabase_client() -> Client:
    """Get Supabase client instance"""
    if supabase is None:
        raise Exception("Supabase client not initialized. Check environment variables.")
    return supabase

def upload_file_to_supabase(file_data: bytes, file_name: str, bucket: str = "verixa-documents") -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        result = supabase.storage.from_(bucket).upload(
            path=file_name,
            file=file_data,
            file_options={"content-type": "application/octet-stream"}
        )
        # Get public URL
        public_url = supabase.storage.from_(bucket).get_public_url(file_name)
        print(f"✅ SUPABASE: File uploaded successfully. URL: {public_url}")
        return public_url
    except Exception as e:
        print(f"❌ SUPABASE: File upload error: {e}")
        raise e

def delete_file_from_supabase(file_path: str, bucket: str = "verixa-documents") -> bool:
    """Delete file from Supabase storage"""
    try:
        result = supabase.storage.from_(bucket).remove([file_path])
        if result.get("error"):
            print(f"❌ SUPABASE: Delete failed: {result['error']}")
            return False
        print("✅ SUPABASE: File deleted successfully")
        return True
    except Exception as e:
        print(f"❌ SUPABASE: File delete error: {e}")
        return False
