import pyodbc
import os
from dotenv import load_dotenv
import argparse
import logging
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_database():
    """Set up the database tables for the Seeking Alpha scraper"""
    # Get connection details from environment variables
    uid = os.getenv("UID")
    pid = os.getenv("PID")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    
    if not all([uid, pid, server, database]):
        logger.error("Missing database connection details in .env file")
        return False
    
    # Create connection string
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pid}'
    
    try:
        # Connect to database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        # Create tables
        logger.info("Creating tables...")
        
        # Create links table
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.tables 
                WHERE name = 'seekingalpha_links'
            )
            CREATE TABLE seekingalpha_links (
                id INT IDENTITY(1,1) PRIMARY KEY,
                title NVARCHAR(500),
                url NVARCHAR(1000),
                collected_at DATETIME DEFAULT GETDATE(),
                downloaded BIT DEFAULT 0,
                download_time DATETIME NULL,
                extracted BIT DEFAULT 0,
                extraction_time DATETIME NULL
            )
        """)
        
        # Create progress table
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.tables 
                WHERE name = 'seekingalpha_progress'
            )
            CREATE TABLE seekingalpha_progress (
                id INT IDENTITY(1,1) PRIMARY KEY,
                last_page_processed INT DEFAULT 0,
                links_collected INT DEFAULT 0, 
                last_updated DATETIME DEFAULT GETDATE()
            )
        """)
        
        # Initialize progress record if none exists
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM seekingalpha_progress)
            INSERT INTO seekingalpha_progress (last_page_processed, links_collected)
            VALUES (0, 0)
        """)
        
        # Create indices for performance
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.indexes 
                WHERE name = 'IX_seekingalpha_links_url' 
                AND object_id = OBJECT_ID('seekingalpha_links')
            )
            CREATE UNIQUE INDEX IX_seekingalpha_links_url
            ON seekingalpha_links(url)
        """)
        
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.indexes 
                WHERE name = 'IX_seekingalpha_links_downloaded' 
                AND object_id = OBJECT_ID('seekingalpha_links')
            )
            CREATE INDEX IX_seekingalpha_links_downloaded
            ON seekingalpha_links(downloaded)
        """)
        
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.indexes 
                WHERE name = 'IX_seekingalpha_links_extracted' 
                AND object_id = OBJECT_ID('seekingalpha_links')
            )
            CREATE INDEX IX_seekingalpha_links_extracted
            ON seekingalpha_links(extracted)
        """)
        
        conn.commit()
        logger.info("Database setup completed successfully")
        
        # Close connection
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False

def reset_database():
    """Reset the database tables for the Seeking Alpha scraper"""
    # Get connection details from environment variables
    uid = os.getenv("UID")
    pid = os.getenv("PID")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    
    if not all([uid, pid, server, database]):
        logger.error("Missing database connection details in .env file")
        return False
    
    # Create connection string
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pid}'
    
    try:
        # Connect to database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        # Confirm reset
        confirm = input("This will delete all data in the Seeking Alpha tables. Type 'YES' to confirm: ")
        if confirm != "YES":
            logger.info("Reset cancelled")
            return False
        
        # Drop tables
        logger.info("Dropping tables...")
        
        cursor.execute("""
            IF EXISTS (
                SELECT * FROM sys.tables 
                WHERE name = 'seekingalpha_links'
            )
            DROP TABLE seekingalpha_links
        """)
        
        cursor.execute("""
            IF EXISTS (
                SELECT * FROM sys.tables 
                WHERE name = 'seekingalpha_progress'
            )
            DROP TABLE seekingalpha_progress
        """)
        
        conn.commit()
        logger.info("Database reset completed successfully")
        
        # Set up tables again
        setup_database()
        
        # Close connection
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error resetting database: {e}")
        return False

def show_status():
    """Show the current status of the database"""
    # Get connection details from environment variables
    uid = os.getenv("UID")
    pid = os.getenv("PID")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    
    if not all([uid, pid, server, database]):
        logger.error("Missing database connection details in .env file")
        return False
    
    # Create connection string
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pid}'
    
    try:
        # Connect to database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        # Check if tables exist
        cursor.execute("""
            SELECT COUNT(*) 
            FROM sys.tables 
            WHERE name IN ('seekingalpha_links', 'seekingalpha_progress')
        """)
        table_count = cursor.fetchone()[0]
        
        if table_count < 2:
            logger.info("Database tables not fully set up")
            return False
        
        # Get progress
        cursor.execute("""
            SELECT last_page_processed, links_collected, last_updated
            FROM seekingalpha_progress
        """)
        progress = cursor.fetchone()
        
        logger.info(f"Current progress:")
        logger.info(f"  Last page processed: {progress[0]}")
        logger.info(f"  Links collected: {progress[1]}")
        logger.info(f"  Last updated: {progress[2]}")
        
        # Get article stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_links,
                SUM(CASE WHEN downloaded = 1 THEN 1 ELSE 0 END) as downloaded_links,
                SUM(CASE WHEN extracted = 1 THEN 1 ELSE 0 END) as extracted_links
            FROM seekingalpha_links
        """)
        stats = cursor.fetchone()
        
        logger.info(f"Article stats:")
        logger.info(f"  Total links: {stats[0]}")
        logger.info(f"  Downloaded: {stats[1]} ({stats[1]/stats[0]*100:.1f}% if stats[0] > 0 else 0)%)")
        logger.info(f"  Extracted: {stats[2]} ({stats[2]/stats[0]*100:.1f}% if stats[0] > 0 else 0)%)")
        
        # Get recent links
        cursor.execute("""
            SELECT TOP 5 title, collected_at
            FROM seekingalpha_links
            ORDER BY collected_at DESC
        """)
        
        logger.info(f"Recent links:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0][:50]}... (collected at {row[1]})")
        
        # Close connection
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error showing status: {e}")
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Database Setup")
    parser.add_argument("--reset", action="store_true", help="Reset the database tables")
    parser.add_argument("--status", action="store_true", help="Show the current database status")
    
    args = parser.parse_args()
    
    if args.reset:
        reset_database()
    elif args.status:
        show_status()
    else:
        setup_database()


if __name__ == "__main__":
    main()