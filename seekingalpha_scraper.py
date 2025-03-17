import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import random
import argparse
import logging
import traceback
import sys
import os
import csv
import pickle
from datetime import datetime
from urllib.parse import urlparse, urljoin
from pathlib import Path

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

class SeekingAlphaScraper:
    def __init__(self, mode, url=None, output_dir=None, csv_file=None, headless=False, cookies_file=None):
        """Initialize the scraper"""
        self.mode = mode  # 'links' or 'articles'
        self.base_url = url
        self.output_dir = output_dir
        self.csv_file = csv_file
        self.headless = headless
        self.cookies_file = cookies_file or "sa_cookies.pkl"
        self.driver = None
        self.captcha_count = 0
        self.failure_count = 0
        
        # Create output directories
        if self.mode == 'links' and self.csv_file:
            os.makedirs(os.path.dirname(os.path.abspath(self.csv_file)), exist_ok=True)
        elif self.mode == 'articles' and self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
        
        # Check if CSV exists for link collection mode
        if self.mode == 'links' and self.csv_file and not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['title', 'url', 'collected_at'])
                logger.info(f"Created new CSV file: {self.csv_file}")
    
    def init_browser(self):
        """Initialize a more stealthy browser with JavaScript and cookies enabled"""
        logger.info("Starting stealth browser...")
        options = uc.ChromeOptions()
        
        # Anti-detection measures
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        
        # Enable JavaScript
        options.add_argument("--enable-javascript")
        
        # Enable cookies
        options.add_argument("--enable-cookies")
        
        # Fix: Instead of add_experimental_option, use arguments
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-automation")
        
        # Make browser appear more like a regular user
        options.add_argument(f"--window-size={random.randint(1050, 1200)},{random.randint(800, 860)}")
        
        # Use a realistic user agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
        ]
        
        user_agent = random.choice(user_agents)
        options.add_argument(f"--user-agent={user_agent}")
        
        if self.headless:
            options.add_argument("--headless")
            logger.warning("Headless mode is not recommended for login-based scraping")
        
        # Create the driver
        self.driver = uc.Chrome(options=options)
        
        # Set custom JS to evade detection
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            // Override webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Add chrome object properties
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // Spoof plugins length
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    return [
                        {
                            0: {type: "application/pdf"},
                            description: "Portable Document Format",
                            filename: "internal-pdf-viewer",
                            name: "Chrome PDF Plugin"
                        }
                    ];
                }
            });
            
            // Spoof languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            """
        })
        
        self.driver.maximize_window()
        logger.info("Browser started with stealth configuration")
        
        # Load cookies if they exist
        self.load_cookies()
    
    def save_cookies(self):
        """Save cookies for future sessions"""
        try:
            cookies = self.driver.get_cookies()
            with open(self.cookies_file, 'wb') as f:
                pickle.dump(cookies, f)
            logger.info(f"Saved {len(cookies)} cookies to {self.cookies_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")
            return False
    
    def load_cookies(self):
        """Load cookies from previous sessions"""
        if not os.path.exists(self.cookies_file):
            logger.info(f"No cookies file found at {self.cookies_file}")
            return False
        
        try:
            with open(self.cookies_file, 'rb') as f:
                cookies = pickle.load(f)
            
            # First access a page on the domain
            self.driver.get("https://seekingalpha.com")
            time.sleep(2)
            
            # Add the cookies
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    logger.debug(f"Could not add cookie {cookie.get('name')}: {e}")
            
            logger.info(f"Loaded {len(cookies)} cookies from {self.cookies_file}")
            
            # Refresh the page to apply cookies
            self.driver.refresh()
            time.sleep(2)
            
            return True
        except Exception as e:
            logger.error(f"Error loading cookies: {e}")
            return False
    
    def check_login_status(self):
        """Check if we're already logged in using cookies"""
        self.driver.get("https://seekingalpha.com")
        time.sleep(3)
        
        if any(x in self.driver.page_source for x in ["Sign Out", "My Portfolio", "My Account", "Premium"]):
            logger.info("✓ Already logged in via cookies")
            return True
        else:
            logger.info("Not logged in, manual login required")
            return False
    
    def manual_login(self):
        """Let the user login manually and handle any captchas"""
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
            # Save cookies after successful login
            self.save_cookies()
            return True
        else:
            logger.error("✗ Login verification failed. Please make sure you're logged in with premium access.")
            input("Try again and press Enter when done, or Ctrl+C to quit...")
            return self.manual_login()
    
    def handle_captcha_with_js(self):
        """Try to handle common captcha types using JavaScript"""
        try:
            # Try to find and click 'I am not a robot' checkbox
            self.driver.execute_script("""
                var captchas = document.querySelectorAll('iframe[src*="recaptcha"], iframe[title*="recaptcha"], .g-recaptcha, .recaptcha');
                if (captchas.length > 0) {
                    console.log('Found reCAPTCHA elements, clicking if possible');
                    for (var i = 0; i < captchas.length; i++) {
                        var captcha = captchas[i];
                        if (captcha.tagName === 'IFRAME') {
                            // Try to focus and click within the iframe
                            captcha.focus();
                        } else {
                            // Try to find and click checkbox
                            var checkbox = captcha.querySelector('.recaptcha-checkbox');
                            if (checkbox) {
                                checkbox.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            """)
            
            # Check for Cloudflare challenge
            cloudflare_detected = self.driver.execute_script("""
                return document.querySelector('#cf-challenge-running, .cf-browser-verification, .cf-captcha-container') !== null;
            """)
            
            if cloudflare_detected:
                logger.warning("Cloudflare challenge detected!")
                return False
            
            # Try to identify and handle "Press and Hold" challenges
            press_hold_detected = self.driver.execute_script("""
                return document.querySelector('[data-testid="human-challenge-press-and-hold"]') !== null;
            """)
            
            if press_hold_detected:
                logger.warning("Press and Hold challenge detected!")
                return False
            
            return False  # No supported captcha handled automatically
        except Exception as e:
            logger.error(f"Error in JavaScript captcha handling: {e}")
            return False
    
    def check_and_handle_captcha(self):
        """Check if a captcha is present and handle it if needed"""
        # First try automatic JS handling
        if self.handle_captcha_with_js():
            logger.info("Captcha handled automatically with JavaScript")
            time.sleep(2)  # Wait for captcha to complete
            return True
        
        # Check for common captcha indicators
        captcha_indicators = [
            "captcha", "robot", "human verification", "security check",
            "prove you're not a robot", "verify you are human", "challenge",
            "press and hold", "human challenge"
        ]
        
        page_source = self.driver.page_source.lower()
        
        if any(indicator in page_source for indicator in captcha_indicators):
            logger.warning("Captcha detected! Please solve it manually.")
            self.captcha_count += 1
            input("Press Enter after solving the captcha...")
            return True
        
        return False
    
    def calculate_delay(self):
        """Calculate adaptive delay based on captcha and failure counts"""
        base_delay = 3  # Base delay in seconds
        jitter = random.uniform(0.8, 1.2)  # Random jitter
        
        # Increase delay if we've hit captchas
        captcha_factor = 1 + (self.captcha_count * 0.5)
        
        # Increase delay if we've had failures
        failure_factor = 1 + (self.failure_count * 0.3)
        
        # Calculate final delay
        delay = base_delay * captcha_factor * failure_factor * jitter
        
        # Cap at reasonable maximum
        return min(delay, 15)

    # ---------- LINK COLLECTION METHODS ----------
    
    def get_existing_links(self):
        """Get links that are already in the CSV file"""
        existing_links = set()
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get('url', '')
                    if url:
                        existing_links.add(url)
            
            logger.info(f"Found {len(existing_links)} existing links in CSV file")
            return existing_links
        except Exception as e:
            logger.error(f"Error reading existing links: {e}")
            return set()
    
    def store_link(self, title, url):
        """Store a link in the CSV file"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                collected_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([title, url, collected_at])
                logger.debug(f"Stored link: {title}")
            return True
        except Exception as e:
            logger.error(f"Error storing link: {e}")
            return False
    
    def collect_links(self, max_pages=None):
        """Collect links from author pages"""
        current_page = 1
        links_collected = 0
        links_already_in_csv = self.get_existing_links()
        
        # Check login status first
        if not self.check_login_status():
            self.manual_login()
        
        try:
            while True:
                # Check if we've reached the maximum number of pages
                if max_pages and current_page > max_pages:
                    logger.info(f"Reached target of {max_pages} pages. Stopping collection.")
                    break
                
                # Format URL for current page
                if "?" in self.base_url:
                    base_url = self.base_url.split("?")[0]
                    current_url = f"{base_url}?page={current_page}"
                else:
                    current_url = f"{self.base_url}?page={current_page}"
                
                logger.info(f"Processing page {current_page}: {current_url}")
                
                # Get links from current page
                try:
                    self.driver.get(current_url)
                    time.sleep(5)
                    
                    # Check for captcha
                    if self.check_and_handle_captcha():
                        logger.info("Captcha handled, continuing...")
                        # Reload page after captcha
                        self.driver.get(current_url)
                        time.sleep(3)
                    
                    # Execute JavaScript to ensure page is fully loaded
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    # Parse the page
                    page_source = self.driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Look for article links using different selectors
                    links_found = False
                    page_links_count = 0
                    
                    # Try JavaScript to extract links first
                    js_links = self.driver.execute_script("""
                        var links = [];
                        var articleLinks = document.querySelectorAll('a[data-test-id="post-list-item-title"], .title a, h3 a, .post-list-item a, a.jl0rkd-0');
                        
                        for (var i = 0; i < articleLinks.length; i++) {
                            var link = articleLinks[i];
                            links.push({
                                url: link.href,
                                title: link.innerText.trim()
                            });
                        }
                        
                        return links;
                    """)
                    
                    if js_links and len(js_links) > 0:
                        logger.info(f"Found {len(js_links)} articles using JavaScript")
                        
                        for link_data in js_links:
                            url = link_data.get('url', '')
                            title = link_data.get('title', '')
                            
                            if not url:
                                continue
                            
                            # Check if we already have this URL
                            if url in links_already_in_csv:
                                logger.debug(f"Skipping duplicate URL: {url}")
                                continue
                            
                            # Store link in CSV
                            self.store_link(title, url)
                            links_already_in_csv.add(url)
                            page_links_count += 1
                            links_collected += 1
                        
                        links_found = True
                    
                    # Fallback to BeautifulSoup if JavaScript didn't find anything
                    if not links_found:
                        # Try several possible selectors
                        for selector in ["a[data-test-id='post-list-item-title']", ".title a", "h3 a", ".post-list-item a", "a.jl0rkd-0"]:
                            elements = soup.select(selector)
                            if elements:
                                logger.info(f"Found {len(elements)} articles using selector: {selector}")
                                
                                # Process found links
                                for link in elements:
                                    url = link.get('href')
                                    if not url:
                                        continue
                                    
                                    # Make URL absolute
                                    if not url.startswith('http'):
                                        url = urljoin("https://seekingalpha.com", url)
                                    
                                    title = link.text.strip()
                                    
                                    # Check if we already have this URL
                                    if url in links_already_in_csv:
                                        logger.debug(f"Skipping duplicate URL: {url}")
                                        continue
                                    
                                    # Store link in CSV
                                    self.store_link(title, url)
                                    links_already_in_csv.add(url)
                                    page_links_count += 1
                                    links_collected += 1
                                
                                links_found = True
                                break
                    
                    logger.info(f"Stored {page_links_count} new links from page {current_page}")
                    
                    if not links_found:
                        logger.info("No articles found on this page. May have reached the end.")
                        
                        # Check if we're at the end
                        if "no results found" in page_source.lower() or "no posts found" in page_source.lower():
                            logger.info("Reached the end of available articles.")
                            break
                        
                        # Add a warning if no links were found but we don't think we're at the end
                        logger.warning("No links found but no end-of-results detected. May need to check manually.")
                    
                    # If we didn't find any new links, we might be at the end
                    if page_links_count == 0:
                        logger.info("No new links found on this page, may have reached the end.")
                        if current_page > 1:  # Only break if we've processed at least one page
                            break
                
                except Exception as e:
                    logger.error(f"Error processing page {current_page}: {e}")
                    logger.error(traceback.format_exc())
                    # Continue to next page despite error
                
                # Go to next page
                current_page += 1
                time.sleep(random.uniform(2, 5))
            
            # Save cookies one more time at the end
            self.save_cookies()
            logger.info(f"Link collection completed. Total links collected: {links_collected}")
            return links_collected
            
        except KeyboardInterrupt:
            logger.info("Collection interrupted by user.")
            self.save_cookies()  # Save cookies before exiting
            return links_collected
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.error(traceback.format_exc())
            self.save_cookies()  # Save cookies before exiting
            return links_collected
    
    # ---------- ARTICLE DOWNLOAD METHODS ----------
    
    def read_csv(self):
        """Read links and titles from CSV file"""
        links = []
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Adjust these field names to match your CSV structure
                    if 'title' in row and 'url' in row:
                        title = row['title']
                        url = row['url']
                    elif len(row) >= 2:  # Fallback if column names don't match
                        keys = list(row.keys())
                        title = row[keys[0]]
                        url = row[keys[1]]
                    else:
                        logger.warning(f"Skipping row with unexpected format: {row}")
                        continue
                    
                    if url:  # Only add if URL is not empty
                        links.append({
                            "title": title,
                            "url": url
                        })
            
            logger.info(f"Read {len(links)} links from CSV file")
            return links
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return []
    
    def download_single_article(self, link):
        """Download a single article"""
        title = link['title']
        url = link['url']
        
        logger.info(f"Downloading: {title}")
        
        try:
            # Generate safe filename
            safe_title = ''.join(c if c.isalnum() else '_' for c in title)
            if not safe_title:  # If title is empty or contains only non-alphanumeric chars
                safe_title = f"article_{int(time.time())}"
            
            filename = f"{safe_title[:50]}.html"
            filepath = os.path.join(self.output_dir, filename)
            
            # Check if file already exists (for safety)
            if os.path.exists(filepath):
                logger.info(f"File already exists: {filename}")
                return True
            
            # Download the content
            self.driver.get(url)
            time.sleep(5)  # Wait for page to load
            
            # Execute JavaScript to ensure page is fully loaded
            self.driver.execute_script("""
                // Scroll down to ensure all lazy-loaded content is loaded
                window.scrollTo(0, document.body.scrollHeight / 2);
                setTimeout(() => { window.scrollTo(0, document.body.scrollHeight); }, 1000);
                
                // Try to dismiss any popups or banners
                var closeButtons = document.querySelectorAll('.close-button, .dismiss, .modal-close, button[aria-label="Close"]');
                for (var i = 0; i < closeButtons.length; i++) {
                    closeButtons[i].click();
                }
                
                // Try to expand any "read more" sections
                var readMoreButtons = document.querySelectorAll('.read-more, .show-more, .expand');
                for (var i = 0; i < readMoreButtons.length; i++) {
                    readMoreButtons[i].click();
                }
            """)
            time.sleep(2)  # Wait for JavaScript to execute
            
            # Check for captcha before proceeding
            if self.check_and_handle_captcha():
                logger.info("Captcha handled, continuing...")
                # Reload the page after captcha
                self.driver.get(url)
                time.sleep(3)
            
            # Save the raw HTML
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            
            # Check if we got the content or just a paywall
            if "premium content requires a subscription" in self.driver.page_source.lower() or "make the most of premium" in self.driver.page_source.lower():
                logger.warning(f"⚠ Paywall detected for {filename}, may need to check login status")
                # Still returning True because we saved the HTML
            
            logger.info(f"✓ Saved HTML to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading article {title}: {e}")
            return False
    
    def download_articles(self):
        """Download content for links in the CSV file"""
        links = self.read_csv()
        
        if not links:
            logger.error("No links found in CSV file. Exiting.")
            return False
        
        # Check login status first
        if not self.check_login_status():
            self.manual_login()
        
        logger.info(f"Starting download for {len(links)} articles...")
        success_count = 0
        
        for i, link in enumerate(links, 1):
            try:
                logger.info(f"Processing link {i}/{len(links)}: {link['title']}")
                
                # Download the article
                success = self.download_single_article(link)
                if success:
                    success_count += 1
                    # Reset failure count on success
                    self.failure_count = max(0, self.failure_count - 1)
                else:
                    self.failure_count += 1
                
                # Add an adaptive delay between downloads to avoid detection
                delay = self.calculate_delay()
                logger.info(f"Waiting {delay:.2f} seconds before next article...")
                time.sleep(delay)
                
                # Save cookies periodically
                if i % 10 == 0:
                    self.save_cookies()
                
            except KeyboardInterrupt:
                logger.info("Download interrupted by user.")
                self.save_cookies()
                return False
            except Exception as e:
                logger.error(f"Error downloading article: {e}")
                logger.error(traceback.format_exc())
                self.failure_count += 1
        
        # Save cookies at the end
        self.save_cookies()
        logger.info(f"Download complete! Successfully downloaded {success_count}/{len(links)} articles")
        logger.info(f"Encountered {self.captcha_count} captchas during download")
        return True
    
    def close(self):
        """Close browser"""
        if self.driver:
            self.save_cookies()  # Save cookies before closing
            self.driver.quit()
            logger.info("Browser closed")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Seeking Alpha Scraper")
    subparsers = parser.add_subparsers(dest='mode', help='Scraping mode')
    
    # Link collector mode arguments
    links_parser = subparsers.add_parser('links', help='Collect article links')
    links_parser.add_argument("--url", required=True, help="Base URL to scrape (author page, search results, etc.)")
    links_parser.add_argument("--output", required=True, help="Output CSV file path")
    links_parser.add_argument("--max-pages", type=int, help="Maximum number of pages to scrape")
    
    # Articles downloader mode arguments
    articles_parser = subparsers.add_parser('articles', help='Download articles from CSV')
    articles_parser.add_argument("--csv", required=True, help="CSV file containing links and titles")
    articles_parser.add_argument("--output", required=True, help="Output directory for HTML files")
    
    # Common arguments
    for subparser in [links_parser, articles_parser]:
        subparser.add_argument("--headless", action="store_true", help="Run in headless mode (not recommended)")
        subparser.add_argument("--login", action="store_true", help="Force manual login even if cookies exist")
        subparser.add_argument("--cookies", help="Cookies file path (default: sa_cookies.pkl)")
    
    args = parser.parse_args()
    
    if not args.mode:
        parser.print_help()
        return
    
    scraper = None
    try:
        if args.mode == 'links':
            scraper = SeekingAlphaScraper(
                mode='links',
                url=args.url,
                csv_file=args.output,
                headless=args.headless,
                cookies_file=args.cookies
            )
            
            scraper.init_browser()
            
            # Force login if requested
            if args.login:
                scraper.manual_login()
            
            # Collect links
            links_collected = scraper.collect_links(max_pages=args.max_pages)
            logger.info(f"Successfully collected {links_collected} links.")
            
        elif args.mode == 'articles':
            scraper = SeekingAlphaScraper(
                mode='articles',
                csv_file=args.csv,
                output_dir=args.output,
                headless=args.headless,
                cookies_file=args.cookies
            )
            
            scraper.init_browser()
            
            # Force login if requested
            if args.login:
                scraper.manual_login()
            
            # Download articles
            scraper.download_articles()
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()