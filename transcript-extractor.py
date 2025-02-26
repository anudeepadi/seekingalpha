import os
import re
import time
import json
import argparse
import logging
import traceback
import sys
import pyodbc
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from pathlib import Path
import concurrent.futures

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transcript_extractor.log"),
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
    
    def get_downloaded_links(self, limit=100):
        """Get links that have been downloaded but not extracted"""
        try:
            self.cursor.execute("""
                SELECT id, title, url 
                FROM seekingalpha_links 
                WHERE downloaded = 1 AND extracted = 0 
                ORDER BY download_time 
                OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
            """, limit)
            
            links = []
            for row in self.cursor.fetchall():
                links.append({
                    "id": row[0],
                    "title": row[1],
                    "url": row[2]
                })
            
            logger.info(f"Retrieved {len(links)} downloaded links pending extraction")
            return links
        except Exception as e:
            logger.error(f"Error retrieving downloaded links: {e}")
            return []
    
    def mark_link_extracted(self, link_id):
        """Mark a link as extracted"""
        try:
            self.cursor.execute("""
                UPDATE seekingalpha_links 
                SET extracted = 1, extraction_time = GETDATE() 
                WHERE id = ?
            """, link_id)
            self.conn.commit()
            logger.debug(f"Marked link {link_id} as extracted")
            return True
        except Exception as e:
            logger.error(f"Error marking link {link_id} as extracted: {e}")
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


class SeekingAlphaTranscriptExtractor:
    def __init__(self, html_dir, output_dir, batch_size=100, parallel=1, interval=60):
        """Initialize the transcript extractor"""
        self.html_dir = html_dir
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.parallel = parallel
        self.interval = interval
        self.db = DBConnector()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
    
    def extract_transcripts(self):
        """Extract transcripts from downloaded HTML files"""
        running = True
        
        while running:
            # Get downloaded links that need extraction
            links = self.db.get_downloaded_links(self.batch_size)
            
            if not links:
                logger.info(f"No links pending extraction. Waiting {self.interval} seconds before checking again...")
                time.sleep(self.interval)
                continue
            
            logger.info(f"Extracting transcripts for {len(links)} articles...")
            
            # Use parallel processing if enabled
            if self.parallel > 1:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel) as executor:
                    futures = {}
                    for link in links:
                        futures[executor.submit(self.extract_single_transcript, link)] = link
                    
                    for future in concurrent.futures.as_completed(futures):
                        link = futures[future]
                        try:
                            result = future.result()
                            if result:
                                self.db.mark_link_extracted(link['id'])
                        except KeyboardInterrupt:
                            logger.info("Extraction interrupted by user.")
                            running = False
                            break
                        except Exception as e:
                            logger.error(f"Error extracting transcript for {link['title']}: {e}")
            else:
                # Process sequentially
                for link in links:
                    try:
                        if self.extract_single_transcript(link):
                            self.db.mark_link_extracted(link['id'])
                    except KeyboardInterrupt:
                        logger.info("Extraction interrupted by user.")
                        running = False
                        break
                    except Exception as e:
                        logger.error(f"Error extracting transcript for {link['title']}: {e}")
            
            # Report progress
            stats = self.db.get_total_stats()
            logger.info(f"Progress: {stats['extracted_links']}/{stats['downloaded_links']} transcripts extracted")
            
            # Check if we should continue
            if not links or len(links) < self.batch_size:
                logger.info(f"No more transcripts to extract. Waiting {self.interval} seconds before checking again...")
                time.sleep(self.interval)
    
    def extract_single_transcript(self, link):
        """Extract transcript from a single HTML file"""
        title = link['title']
        url = link['url']
        link_id = link['id']
        
        logger.info(f"Extracting transcript for: {title}")
        
        try:
            # Find HTML file
            safe_title = ''.join(c if c.isalnum() else '_' for c in title)
            html_filename = f"{safe_title[:50]}.html"
            html_filepath = os.path.join(self.html_dir, html_filename)
            
            # Check if HTML file exists
            if not os.path.exists(html_filepath):
                logger.error(f"HTML file not found: {html_filename}")
                return False
            
            # Read HTML file
            with open(html_filepath, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract metadata
            date = self.extract_element_text(soup, ["time", "[data-test-id='post-date']", ".post-date"])
            author = self.extract_element_text(soup, ["[data-test-id='author-name']", ".author-link"])
            
            # Extract transcript content
            content = self.extract_content(soup, html_content)
            
            # Validate content - ensure it's not just the premium teaser
            if len(content) < 500 or ("Make the most of Premium" in content and len(content) < 1000):
                logger.warning(f"Content may be incomplete for {html_filename}")
            
            # Create result object
            result = {
                'title': title,
                'url': url,
                'date': date,
                'author': author,
                'content': content
            }
            
            # Save to JSON
            json_filename = f"{safe_title[:50]}.json"
            json_filepath = os.path.join(self.output_dir, json_filename)
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=4, ensure_ascii=False)
            
            logger.info(f"✓ Saved transcript to {json_filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting transcript for {title}: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def extract_element_text(self, soup, selectors):
        """Extract text from the first matching selector"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        return "Not found"
    
    def extract_content(self, soup, html_content):
        """Extract transcript content using multiple methods"""
        # Method 1: Look for transcript sections
        content = self.extract_from_transcript_sections(soup)
        if content and len(content) > 500:
            return content
        
        # Method 2: Look for content containers
        content = self.extract_from_content_containers(soup)
        if content and len(content) > 500:
            return content
        
        # Method 3: Look for script data
        content = self.extract_from_scripts(html_content)
        if content and len(content) > 500:
            return content
        
        # Method 4: Look for speaker patterns
        content = self.extract_from_speaker_patterns(html_content)
        if content and len(content) > 500:
            return content
        
        # Method 5: Fallback to all paragraphs
        all_paragraphs = soup.select("p")
        if all_paragraphs:
            filtered_paragraphs = []
            for p in all_paragraphs:
                p_text = p.text.strip()
                if p_text and len(p_text) > 20 and not any(x in p_text.lower() for x in [
                    "disclosure:", "disclosure :", "©", "all rights reserved", 
                    "seeking alpha", "editor's note", "make the most of premium"
                ]):
                    filtered_paragraphs.append(p_text)
            
            if filtered_paragraphs:
                return "\n\n".join(filtered_paragraphs)
        
        return "Content extraction failed"
    
    def extract_from_transcript_sections(self, soup):
        """Extract from transcript-specific sections"""
        transcript_sections = soup.select(".transcript-section, .transcript-text, .sa-transcript")
        if transcript_sections:
            return "\n\n".join([section.text.strip() for section in transcript_sections])
        return ""
    
    def extract_from_content_containers(self, soup):
        """Extract from known content containers"""
        container_selectors = [
            "div[data-test-id='content-container']", 
            ".paywall-content",
            ".sa-art", 
            "article.sa-content",
            ".article-content",
            "#a-body"
        ]
        
        for selector in container_selectors:
            container = soup.select_one(selector)
            if container:
                # Skip containers with premium messages only
                if "Make the most of Premium" in container.text and len(container.text) < 100:
                    continue
                    
                paragraphs = container.select("p")
                if paragraphs:
                    filtered_paragraphs = []
                    for p in paragraphs:
                        p_text = p.text.strip()
                        if p_text and len(p_text) > 20 and not any(x in p_text.lower() for x in [
                            "disclosure:", "disclosure :", "©", "all rights reserved", 
                            "seeking alpha", "editor's note", "make the most of premium"
                        ]):
                            filtered_paragraphs.append(p_text)
                    
                    if filtered_paragraphs:
                        return "\n\n".join(filtered_paragraphs)
        
        return ""
    
    def extract_from_scripts(self, html_content):
        """Extract content from script tags"""
        script_pattern = re.compile(r'<script[^>]*>(.*?)</script>', re.DOTALL)
        json_pattern = re.compile(r'\{[^}]*"transcript"[^}]*\}')
        content_pattern = re.compile(r'"content"\s*:\s*"([^"]*)"')
        
        script_matches = script_pattern.findall(html_content)
        
        for script in script_matches:
            json_match = json_pattern.search(script)
            if json_match:
                content_match = content_pattern.search(script)
                if content_match:
                    content = content_match.group(1)
                    # Unescape JSON content
                    content = content.replace('\\"', '"').replace('\\n', '\n')
                    return content
        
        return ""
    
    def extract_from_speaker_patterns(self, html_content):
        """Extract content using speaker patterns"""
        speaker_pattern = re.compile(r'<strong>([^<:]+):</strong>([^<]+)', re.IGNORECASE)
        matches = speaker_pattern.findall(html_content)
        
        if matches and len(matches) > 10:  # Only consider if we find multiple speaker segments
            transcript = []
            for speaker, text in matches:
                transcript.append(f"{speaker.strip()}: {text.strip()}")
            
            return "\n\n".join(transcript)
        
        return ""
    
    def run_one_time(self):
        """Run extraction once and exit"""
        links = self.db.get_downloaded_links(self.batch_size)
        
        if not links:
            logger.info("No links pending extraction.")
            return
        
        logger.info(f"Extracting transcripts for {len(links)} articles...")
        
        # Process links
        processed_count = 0
        for link in links:
            try:
                if self.extract_single_transcript(link):
                    self.db.mark_link_extracted(link['id'])
                    processed_count += 1
            except Exception as e:
                logger.error(f"Error extracting transcript for {link['title']}: {e}")
        
        # Report progress
        stats = self.db.get_total_stats()
        logger.info(f"Extraction completed. Processed {processed_count} articles.")
        logger.info(f"Overall progress: {stats['extracted_links']}/{stats['downloaded_links']} transcripts extracted")
    
    def close(self):
        """Close connections"""
        self.db.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Transcript Extractor")
    parser.add_argument("--html-dir", required=True, help="Directory containing HTML files")
    parser.add_argument("--output", default="sa_transcripts", help="Output directory for JSON files")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of files to process in each batch")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel extraction workers")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument("--one-time", action="store_true", help="Run once and exit instead of continuous polling")
    
    args = parser.parse_args()
    
    extractor = None
    try:
        extractor = SeekingAlphaTranscriptExtractor(
            html_dir=args.html_dir,
            output_dir=args.output,
            batch_size=args.batch_size,
            parallel=args.parallel,
            interval=args.interval
        )
        
        if args.one_time:
            extractor.run_one_time()
        else:
            extractor.extract_transcripts()
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        if extractor:
            extractor.close()


if __name__ == "__main__":
    main()