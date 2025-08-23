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

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_SERVER},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD}"
)


# --- SUPABASE SETUP ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def test_sql_connection():
    """Test SQL Server connection"""
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE();")
        row = cursor.fetchone()
        print("✅ SQL Server connected. Current time:", row[0])
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ SQL Server connection failed:", e)

def test_supabase_connection():
    """Test Supabase connection by listing buckets"""
    try:
        buckets = supabase.storage.list_buckets()
        # Access bucket names using the 'name' property
        bucket_names = [b.name for b in buckets]
        print("✅ Supabase connected. Buckets:", bucket_names)

    except Exception as e:
        print("❌ Supabase connection failed:", e)




if __name__ == "__main__":
    test_sql_connection()
    test_supabase_connection()
