import undetected_chromedriver as uc
from bs4 import BeautifulSoup
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("link_collector.log"),
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
        self.initialize_tables()
    
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
    
    def initialize_tables(self):
        """Create tables if they don't exist"""
        try:
            # Check if links table exists
            self.cursor.execute("""
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
            
            # Check if progress table exists
            self.cursor.execute("""
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
            
            # Insert initial progress record if none exists
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM seekingalpha_progress)
                INSERT INTO seekingalpha_progress (last_page_processed, links_collected)
                VALUES (0, 0)
            """)
            
            self.conn.commit()
            logger.info("Database tables initialized")
        except Exception as e:
            logger.error(f"Error initializing tables: {e}")
            self.conn.rollback()
            raise
    
    def get_progress(self):
        """Get current progress"""
        try:
            self.cursor.execute("SELECT last_page_processed, links_collected FROM seekingalpha_progress")
            row = self.cursor.fetchone()
            if row:
                return {"last_page_processed": row[0], "links_collected": row[1]}
            return {"last_page_processed": 0, "links_collected": 0}
        except Exception as e:
            logger.error(f"Error getting progress: {e}")
            return {"last_page_processed": 0, "links_collected": 0}
    
    def update_progress(self, last_page, links_collected):
        """Update progress"""
        try:
            self.cursor.execute("""
                UPDATE seekingalpha_progress 
                SET last_page_processed = ?, links_collected = ?, last_updated = GETDATE()
            """, last_page, links_collected)
            self.conn.commit()
            logger.debug(f"Progress updated: page {last_page}, links {links_collected}")
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
            self.conn.rollback()
    
    def store_link(self, title, url):
        """Store a link in the database if it doesn't exist"""
        try:
            # Check if URL already exists
            self.cursor.execute("SELECT COUNT(*) FROM seekingalpha_links WHERE url = ?", url)
            if self.cursor.fetchone()[0] > 0:
                logger.debug(f"URL already exists: {url}")
                return False
            
            # Insert new link
            self.cursor.execute("""
                INSERT INTO seekingalpha_links (title, url)
                VALUES (?, ?)
            """, title, url)
            self.conn.commit()
            logger.debug(f"Stored link: {title}")
            return True
        except Exception as e:
            logger.error(f"Error storing link: {e}")
            self.conn.rollback()
            return False
    
    def get_links_count(self):
        """Get count of links stored"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM seekingalpha_links")
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting links count: {e}")
            return 0
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


class SeekingAlphaLinkCollector:
    def __init__(self, author_url, max_links=None, headless=False):
        """Initialize the link collector"""
        self.author_url = author_url
        self.max_links = max_links
        self.headless = headless
        self.driver = None
        self.db = DBConnector()
        
    def init_browser(self):
        """Initialize the browser"""
        logger.info("Starting browser...")
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        if self.headless:
            options.add_argument("--headless")
        self.driver = uc.Chrome(options=options)
        self.driver.maximize_window()
        logger.info("Browser started")
    
    def collect_links(self):
        """Collect links from author pages"""
        # Get current progress
        progress = self.db.get_progress()
        current_page = progress["last_page_processed"] + 1
        links_collected = progress["links_collected"]
        
        logger.info(f"Starting collection from page {current_page}, already collected {links_collected} links")
        
        try:
            while True:
                # Check if we've collected enough links
                if self.max_links and links_collected >= self.max_links:
                    logger.info(f"Reached target of {self.max_links} links. Stopping collection.")
                    break
                
                # Format URL for current page
                if "?" in self.author_url:
                    base_url = self.author_url.split("?")[0]
                    current_url = f"{base_url}?page={current_page}"
                else:
                    current_url = f"{self.author_url}?page={current_page}"
                
                logger.info(f"Processing page {current_page}: {current_url}")
                
                # Get links from current page
                try:
                    self.driver.get(current_url)
                    time.sleep(5)
                    
                    # Parse the page
                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    
                    # Look for article links using different selectors
                    links_found = False
                    for selector in ["a[data-test-id='post-list-item-title']", ".title a", "h3 a", ".post-list-item a"]:
                        elements = soup.select(selector)
                        if elements:
                            logger.info(f"Found {len(elements)} articles using selector: {selector}")
                            
                            # Process found links
                            page_links_count = 0
                            for link in elements:
                                url = link.get('href')
                                if not url:
                                    continue
                                
                                # Make URL absolute
                                if not url.startswith('http'):
                                    url = f"https://seekingalpha.com{url}"
                                
                                title = link.text.strip()
                                
                                # Store link in database
                                if self.db.store_link(title, url):
                                    page_links_count += 1
                                    links_collected += 1
                                    logger.info(f"Stored: {title}")
                            
                            logger.info(f"Stored {page_links_count} new links from page {current_page}")
                            links_found = True
                            break
                    
                    if not links_found:
                        logger.info("No articles found on this page. May have reached the end.")
                        
                        # Check if we're at the end
                        if "no results found" in self.driver.page_source.lower() or "no posts found" in self.driver.page_source.lower():
                            logger.info("Reached the end of available articles.")
                            break
                        
                        # Add a warning if no links were found but we don't think we're at the end
                        logger.warning("No links found but no end-of-results detected. May need to check manually.")
                
                except Exception as e:
                    logger.error(f"Error processing page {current_page}: {e}")
                    logger.error(traceback.format_exc())
                    # Continue to next page despite error
                
                # Update progress
                self.db.update_progress(current_page, links_collected)
                
                # Go to next page
                current_page += 1
                time.sleep(random.uniform(2, 5))
            
            logger.info(f"Link collection completed. Total links collected: {links_collected}")
            return True
            
        except KeyboardInterrupt:
            logger.info("Collection interrupted by user.")
            # Save progress before exiting
            self.db.update_progress(current_page - 1, links_collected)
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.error(traceback.format_exc())
            # Save progress before exiting
            self.db.update_progress(current_page - 1, links_collected)
            return False
        finally:
            # Close browser
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")
    
    def close(self):
        """Close connections"""
        self.db.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Link Collector")
    parser.add_argument("--url", required=True, help="Author URL to scrape")
    parser.add_argument("--max-links", type=int, help="Maximum number of links to collect")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    
    args = parser.parse_args()
    
    collector = None
    try:
        collector = SeekingAlphaLinkCollector(
            author_url=args.url,
            max_links=args.max_links,
            headless=args.headless
        )
        
        collector.init_browser()
        collector.collect_links()
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        if collector:
            collector.close()


if __name__ == "__main__":
    main()