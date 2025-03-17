import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import csv
import os
import time
import random
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import concurrent.futures
import re
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("seekingalpha_scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SeekingAlphaUnifiedScraper:
    def __init__(self, config):
        """Initialize the scraper with configuration"""
        self.config = config
        self.driver = None
        self.progress_file = os.path.join(config['output_dir'], "progress.json")
        self.links_file = os.path.join(config['output_dir'], "all_links.json")
        self.links_csv = os.path.join(config['output_dir'], "all_links.csv")
        self.html_dir = os.path.join(config['output_dir'], "html_content")
        self.transcript_dir = os.path.join(config['output_dir'], "transcripts")
        
        # Create necessary directories
        os.makedirs(config['output_dir'], exist_ok=True)
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.transcript_dir, exist_ok=True)
        
        # Load or initialize progress
        self.progress = self.load_progress()
        
        # Initialize browser if not in extraction-only mode
        if not config['extract_only']:
            self.init_browser()

    def init_browser(self):
        """Initialize browser with anti-detection settings"""
        logger.info("Starting browser...")
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        if self.config['headless']:
            options.add_argument("--headless")
        self.driver = uc.Chrome(options=options)
        self.driver.maximize_window()
        logger.info("Browser started!")

    def load_progress(self):
        """Load progress from file or initialize new progress"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                logger.info(f"Loaded existing progress. Links collected: {progress['links_collected']}, "
                           f"HTML downloaded: {len(progress['downloaded_urls'])}, "
                           f"Transcripts extracted: {len(progress['extracted_urls'])}")
                return progress
            except Exception as e:
                logger.error(f"Error loading progress file: {e}")

        # Initialize new progress
        return {
            "links_collected": 0,
            "last_page_processed": 0,
            "downloaded_urls": [],
            "extracted_urls": [],
            "last_updated": datetime.now().isoformat()
        }

    def save_progress(self):
        """Save current progress to file"""
        self.progress["last_updated"] = datetime.now().isoformat()
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, indent=4)
        logger.debug("Progress saved")

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

    def extract_links(self):
        """Extract article links from author pages"""
        # Check if we already have links
        all_links = []
        
        if os.path.exists(self.links_file):
            with open(self.links_file, 'r', encoding='utf-8') as f:
                all_links = json.load(f)
            logger.info(f"Loaded {len(all_links)} existing links from {self.links_file}")
            
            # Update progress
            self.progress["links_collected"] = len(all_links)
            self.save_progress()
            
            # If we have enough links, return them
            if self.config['max_links'] and len(all_links) >= self.config['max_links']:
                logger.info(f"Already have {len(all_links)} links, which meets the target of {self.config['max_links']}")
                return all_links
        
        # Start from the last processed page or from page 1
        current_page = self.progress["last_page_processed"] + 1
        
        while True:
            # Check if we've collected enough links
            if self.config['max_links'] and len(all_links) >= self.config['max_links']:
                logger.info(f"Reached target of {self.config['max_links']} links. Stopping link collection.")
                break
            
            # Format URL for current page
            if "?" in self.config['author_url']:
                base_url = self.config['author_url'].split("?")[0]
                current_url = f"{base_url}?page={current_page}"
            else:
                current_url = f"{self.config['author_url']}?page={current_page}"
            
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
                        page_links = []
                        for link in elements:
                            url = link.get('href')
                            if not url:
                                continue
                            
                            # Make URL absolute
                            if not url.startswith('http'):
                                url = f"https://seekingalpha.com{url}"
                            
                            title = link.text.strip()
                            logger.info(f"Found: {title}")
                            
                            page_links.append({
                                'title': title,
                                'url': url
                            })
                        
                        # Add to collection
                        all_links.extend(page_links)
                        links_found = True
                        break
                
                if not links_found:
                    logger.info("No articles found on this page. May have reached the end.")
                    
                    # Check if we're at the end
                    if "no results found" in self.driver.page_source.lower() or "no posts found" in self.driver.page_source.lower():
                        logger.info("Reached the end of available articles.")
                        break
            
            except Exception as e:
                logger.error(f"Error processing page {current_page}: {e}")
                logger.error(traceback.format_exc())
                # Continue to next page despite error
            
            # Update progress
            self.progress["last_page_processed"] = current_page
            self.progress["links_collected"] = len(all_links)
            self.save_progress()
            
            # Save links to file
            with open(self.links_file, 'w', encoding='utf-8') as f:
                json.dump(all_links, f, indent=4)
            logger.info(f"Saved {len(all_links)} links to {self.links_file}")
            
            # Save to CSV also
            with open(self.links_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Title", "URL"])
                for link in all_links:
                    writer.writerow([link['title'], link['url']])
            
            # Go to next page
            current_page += 1
            time.sleep(random.uniform(2, 5))
        
        return all_links

    def download_html(self, links):
        """Download HTML content for links that haven't been processed yet"""
        # Filter out already downloaded URLs
        links_to_download = [link for link in links if link['url'] not in self.progress["downloaded_urls"]]
        
        if not links_to_download:
            logger.info("All links have already been downloaded")
            return
        
        logger.info(f"Downloading HTML content for {len(links_to_download)} links...")
        
        # Use multiple workers for parallel downloading if enabled
        if self.config['parallel'] > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['parallel']) as executor:
                futures = {executor.submit(self.download_single_html, link): link for link in links_to_download}
                
                for future in concurrent.futures.as_completed(futures):
                    link = futures[future]
                    try:
                        result = future.result()
                        if result:
                            # Mark as downloaded in progress
                            self.progress["downloaded_urls"].append(link['url'])
                            self.save_progress()
                    except Exception as e:
                        logger.error(f"Error downloading {link['url']}: {e}")
        else:
            # Sequential downloading
            for link in links_to_download:
                success = self.download_single_html(link)
                if success:
                    # Mark as downloaded in progress
                    self.progress["downloaded_urls"].append(link['url'])
                    self.save_progress()
                
                # Add delay between downloads
                time.sleep(random.uniform(2, 5))

    def download_single_html(self, link):
        """Download HTML for a single link"""
        # Create a new browser instance for each thread if in parallel mode
        local_driver = None
        if self.config['parallel'] > 1:
            try:
                options = uc.ChromeOptions()
                options.add_argument("--disable-blink-features=AutomationControlled")
                if self.config['headless']:
                    options.add_argument("--headless")
                local_driver = uc.Chrome(options=options)
                local_driver.maximize_window()
            except Exception as e:
                logger.error(f"Error creating browser for thread: {e}")
                return False
        
        # Use the appropriate driver
        driver_to_use = local_driver if local_driver else self.driver
        
        title = link['title']
        url = link['url']
        
        try:
            logger.info(f"Downloading: {title}")
            
            # Generate safe filename
            safe_title = ''.join(c if c.isalnum() else '_' for c in title)
            filename = f"{safe_title[:50]}.html"
            filepath = os.path.join(self.html_dir, filename)
            
            # Download the content
            driver_to_use.get(url)
            time.sleep(5)
            
            # Save the raw HTML
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(driver_to_use.page_source)
            
            logger.info(f"✓ Saved HTML to {filename}")
            
            # Close the local driver if we created one
            if local_driver:
                local_driver.quit()
                
            return True
            
        except Exception as e:
            logger.error(f"Error downloading article {title}: {e}")
            logger.error(traceback.format_exc())
            
            # Close the local driver if we created one
            if local_driver:
                try:
                    local_driver.quit()
                except:
                    pass
                    
            return False

    def extract_transcripts(self):
        """Extract transcript content from HTML files"""
        # Get all HTML files
        html_files = list(Path(self.html_dir).glob("*.html"))
        
        # Filter out HTML files that have already been processed
        html_to_process = []
        for html_file in html_files:
            # Find the corresponding link to get the URL
            filename_base = html_file.stem
            found = False
            
            # Load links
            if os.path.exists(self.links_file):
                with open(self.links_file, 'r', encoding='utf-8') as f:
                    links = json.load(f)
                
                for link in links:
                    safe_title = ''.join(c if c.isalnum() else '_' for c in link['title'])
                    if safe_title[:50] == filename_base:
                        # Check if this URL has been extracted
                        if link['url'] not in self.progress["extracted_urls"]:
                            html_to_process.append((html_file, link))
                        found = True
                        break
            
            # If no matching link found, process it anyway
            if not found:
                html_to_process.append((html_file, {'url': 'unknown', 'title': html_file.stem}))
        
        logger.info(f"Found {len(html_to_process)} HTML files to extract transcripts from")
        
        # Use multiple workers for parallel extraction if enabled
        if self.config['parallel'] > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['parallel']) as executor:
                futures = {executor.submit(self.extract_single_transcript, html_file, link): (html_file, link) 
                          for html_file, link in html_to_process}
                
                for future in concurrent.futures.as_completed(futures):
                    html_file, link = futures[future]
                    try:
                        result = future.result()
                        if result:
                            # Mark as extracted in progress
                            self.progress["extracted_urls"].append(link['url'])
                            self.save_progress()
                    except Exception as e:
                        logger.error(f"Error extracting from {html_file}: {e}")
        else:
            # Sequential extraction
            for html_file, link in html_to_process:
                success = self.extract_single_transcript(html_file, link)
                if success:
                    # Mark as extracted in progress
                    self.progress["extracted_urls"].append(link['url'])
                    self.save_progress()

    def extract_single_transcript(self, html_file, link):
        """Extract transcript from a single HTML file"""
        try:
            logger.info(f"Extracting transcript from: {html_file.name}")
            
            # Read HTML file
            with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract metadata
            title = link['title']
            date = self.extract_element_text(soup, ["time", "[data-test-id='post-date']", ".post-date"])
            author = self.extract_element_text(soup, ["[data-test-id='author-name']", ".author-link"])
            
            # Extract transcript content
            content = self.extract_transcript_content(soup, html_content)
            
            # Validate content - ensure it's not just the premium teaser
            if len(content) < 500 or ("Make the most of Premium" in content and len(content) < 1000):
                logger.warning(f"Content may be incomplete for {html_file.name}")
                if self.config['skip_incomplete']:
                    logger.info(f"Skipping incomplete content for {html_file.name}")
                    return False
            
            # Create result object
            result = {
                'title': title,
                'url': link['url'],
                'date': date,
                'author': author,
                'content': content
            }
            
            # Save to JSON
            output_file = os.path.join(self.transcript_dir, f"{html_file.stem}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=4, ensure_ascii=False)
            
            logger.info(f"✓ Saved transcript to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting transcript from {html_file}: {e}")
            logger.error(traceback.format_exc())
            return False

    def extract_element_text(self, soup, selectors):
        """Extract text from the first matching selector"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
        return "Not found"

    def extract_transcript_content(self, soup, html_content):
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

    def run(self):
        """Run the complete scraping workflow"""
        try:
            # Login if we'll be downloading content
            if not self.config['extract_only']:
                if not self.manual_login():
                    logger.error("Login failed. Exiting.")
                    return False
            
            # Extract links if needed
            if not self.config['extract_only']:
                links = self.extract_links()
                logger.info(f"Total links collected: {len(links)}")
            else:
                # In extract-only mode, load existing links
                if os.path.exists(self.links_file):
                    with open(self.links_file, 'r', encoding='utf-8') as f:
                        links = json.load(f)
                    logger.info(f"Loaded {len(links)} links from {self.links_file}")
                else:
                    logger.error(f"No links file found at {self.links_file}. Cannot proceed in extract-only mode.")
                    return False
            
            # Limit links if max_links is specified
            if self.config['max_links'] and len(links) > self.config['max_links']:
                links = links[:self.config['max_links']]
                logger.info(f"Limited to {len(links)} links")
            
            # Download HTML content if needed
            if not self.config['extract_only']:
                self.download_html(links)
            
            # Extract transcripts
            self.extract_transcripts()
            
            logger.info("Scraping workflow completed successfully!")
            return True
            
        except KeyboardInterrupt:
            logger.info("Script interrupted by user.")
            return False
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed.")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Unified Scraper")
    
    parser.add_argument("--url", help="Author URL to scrape")
    parser.add_argument("--output", default="seekingalpha_data", help="Output directory")
    parser.add_argument("--max-links", type=int, help="Maximum number of links to process")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel workers")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--extract-only", action="store_true", help="Only extract transcripts from existing HTML")
    parser.add_argument("--skip-incomplete", action="store_true", help="Skip incomplete transcripts")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.extract_only and not args.url:
        parser.error("--url is required unless --extract-only is specified")
    
    # Create config
    config = {
        'author_url': args.url,
        'output_dir': args.output,
        'max_links': args.max_links,
        'parallel': args.parallel,
        'headless': args.headless,
        'extract_only': args.extract_only,
        'skip_incomplete': args.skip_incomplete
    }
    
    # Run the scraper
    scraper = SeekingAlphaUnifiedScraper(config)
    scraper.run()

if __name__ == "__main__":
    main()