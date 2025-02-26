import undetected_chromedriver as uc
import time
import random
import argparse
import logging
import traceback
import sys
import os
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("content_downloader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DBConnector:
    def __init__(self):
        """Initialize database connection"""
        self.uid = os.getenv("UID")
        self.pid = os.getenv("PID")
        self.server = os.getenv("SERVER")
        self.database = os.getenv("DATABASE")
        self.conn = None
        self.cursor = None
        
        self.connect()
    
    def connect(self):
        """Connect to database"""
        conn_str = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.uid};PWD={self.pid}'
        try:
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def get_unprocessed_links(self, limit=100):
        """Get links that haven't been downloaded yet"""
        try:
            self.cursor.execute("""
                SELECT id, title, url 
                FROM seekingalpha_links 
                WHERE downloaded = 0 
                ORDER BY collected_at 
                OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
            """, limit)
            
            links = []
            for row in self.cursor.fetchall():
                links.append({
                    "id": row[0],
                    "title": row[1],
                    "url": row[2]
                })
            
            logger.info(f"Retrieved {len(links)} unprocessed links")
            return links
        except Exception as e:
            logger.error(f"Error retrieving unprocessed links: {e}")
            return []
    
    def mark_link_downloaded(self, link_id):
        """Mark a link as downloaded"""
        try:
            self.cursor.execute("""
                UPDATE seekingalpha_links 
                SET downloaded = 1, download_time = GETDATE() 
                WHERE id = ?
            """, link_id)
            self.conn.commit()
            logger.debug(f"Marked link {link_id} as downloaded")
            return True
        except Exception as e:
            logger.error(f"Error marking link {link_id} as downloaded: {e}")
            self.conn.rollback()
            return False
    
    def get_total_stats(self):
        """Get total stats for reporting"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_links,
                    SUM(CASE WHEN downloaded = 1 THEN 1 ELSE 0 END) as downloaded_links,
                    SUM(CASE WHEN extracted = 1 THEN 1 ELSE 0 END) as extracted_links
                FROM seekingalpha_links
            """)
            
            row = self.cursor.fetchone()
            return {
                "total_links": row[0],
                "downloaded_links": row[1],
                "extracted_links": row[2]
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {"total_links": 0, "downloaded_links": 0, "extracted_links": 0}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


class SeekingAlphaContentDownloader:
    def __init__(self, output_dir, batch_size=100, interval=60):
        """Initialize the content downloader"""
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.interval = interval  # Polling interval in seconds
        self.driver = None
        self.db = DBConnector()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def init_browser(self):
        """Initialize the browser"""
        logger.info("Starting browser...")
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        self.driver = uc.Chrome(options=options)
        self.driver.maximize_window()
        logger.info("Browser started")
    
    def manual_login(self):
        """Let the user login manually"""
        self.driver.get("https://seekingalpha.com/login")
        time.sleep(3)
        
        logger.info("\n" + "="*70)
        logger.info("MANUAL LOGIN REQUIRED")
        logger.info("Please log in to your Seeking Alpha premium account in the browser window.")
        logger.info("Take your time to complete any CAPTCHAs or security checks.")
        logger.info("After logging in successfully, return to this console.")
        logger.info("="*70)
        
        input("\nPress Enter AFTER you have successfully logged in...")
        
        # Verify login was successful
        self.driver.get("https://seekingalpha.com")
        time.sleep(3)
        
        if any(x in self.driver.page_source for x in ["Sign Out", "My Portfolio", "My Account", "Premium"]):
            logger.info("✓ Login successful!")
            return True
        else:
            logger.error("✗ Login verification failed. Please make sure you're logged in with premium access.")
            input("Try again and press Enter when done, or Ctrl+C to quit...")
            return self.manual_login()
    
    def download_content(self):
        """Download content for unprocessed links"""
        running = True
        
        while running:
            # Get unprocessed links
            links = self.db.get_unprocessed_links(self.batch_size)
            
            if not links:
                logger.info(f"No unprocessed links found. Waiting {self.interval} seconds before checking again...")
                time.sleep(self.interval)
                continue
            
            logger.info(f"Processing {len(links)} links...")
            
            for link in links:
                try:
                    self.download_single_article(link)
                    
                    # Add a random delay between downloads
                    time.sleep(random.uniform(2, 5))
                    
                except KeyboardInterrupt:
                    logger.info("Download interrupted by user.")
                    running = False
                    break
                except Exception as e:
                    logger.error(f"Error downloading article {link['id']}: {e}")
                    logger.error(traceback.format_exc())
            
            # Report progress
            stats = self.db.get_total_stats()
            logger.info(f"Progress: {stats['downloaded_links']}/{stats['total_links']} articles downloaded")
            
            # Check if we should continue
            if not links or len(links) < self.batch_size:
                logger.info(f"No more links to process. Waiting {self.interval} seconds before checking again...")
                time.sleep(self.interval)
    
    def download_single_article(self, link):
        """Download a single article"""
        title = link['title']
        url = link['url']
        link_id = link['id']
        
        logger.info(f"Downloading: {title}")
        
        try:
            # Generate safe filename
            safe_title = ''.join(c if c.isalnum() else '_' for c in title)
            filename = f"{safe_title[:50]}.html"
            filepath = os.path.join(self.output_dir, filename)
            
            # Check if file already exists (for safety)
            if os.path.exists(filepath):
                logger.info(f"File already exists: {filename}")
                self.db.mark_link_downloaded(link_id)
                return True
            
            # Download the content
            self.driver.get(url)
            time.sleep(5)
            
            # Save the raw HTML
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            
            # Mark as downloaded in database
            self.db.mark_link_downloaded(link_id)
            
            logger.info(f"✓ Saved HTML to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading article {title}: {e}")
            return False
    
    def close(self):
        """Close connections"""
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")
        
        self.db.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Content Downloader")
    parser.add_argument("--output", default="sa_content", help="Output directory for HTML files")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of links to process in each batch")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument("--one-time", action="store_true", help="Run once and exit instead of continuous polling")
    
    args = parser.parse_args()
    
    downloader = None
    try:
        downloader = SeekingAlphaContentDownloader(
            output_dir=args.output,
            batch_size=args.batch_size,
            interval=args.interval
        )
        
        downloader.init_browser()
        
        if downloader.manual_login():
            if args.one_time:
                # Get unprocessed links
                links = downloader.db.get_unprocessed_links(args.batch_size)
                
                if not links:
                    logger.info("No unprocessed links found.")
                else:
                    logger.info(f"Processing {len(links)} links...")
                    
                    for link in links:
                        downloader.download_single_article(link)
                        time.sleep(random.uniform(1, 3))
                    
                    # Report progress
                    stats = downloader.db.get_total_stats()
                    logger.info(f"Progress: {stats['downloaded_links']}/{stats['total_links']} articles downloaded")
            else:
                # Run in continuous mode
                downloader.download_content()
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        if downloader:
            downloader.close()


if __name__ == "__main__":
    main()