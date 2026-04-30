import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gainesville_crime')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'init_db.sql')

def run_init_db():
    print(f"Connecting to database {POSTGRES_DB} at {POSTGRES_HOST}:{POSTGRES_PORT}...")
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        print(f"Reading SQL file: {SQL_FILE_PATH}")
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        print("Executing SQL schema...")
        cur.execute(sql_content)
        
        print("Database initialization successful!")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        exit(1)

if __name__ == "__main__":
    run_init_db()
