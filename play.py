"""
Ultimate PerimeterX Bypass Solution for Seeking Alpha Content Downloader

This script combines multiple advanced techniques to bypass PerimeterX protection:
1. Browser fingerprint spoofing
2. Persistent user profiles
3. Human-like behavior simulation
4. CAPTCHA detection and handling
5. Progressive scraping with intelligent delays
6. Database integration for progress tracking

Usage:
1. First run: python ultimate_perimeter_bypass.py --login
   - This will open a browser for you to login manually
   - Your session will be saved for future use

2. Regular scraping: python ultimate_perimeter_bypass.py
   - Uses saved session to download content
   - Maintains your database state

3. Specific articles: python ultimate_perimeter_bypass.py --url "https://seekingalpha.com/article/XXXX"
   - Download a specific article
"""

import os
import sys
import time
import json
import random
import logging
import argparse
import traceback
import subprocess
import pyodbc
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("seekingalpha_bypass.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Try to import optional dependencies
try:
    import pyautogui
    PYAUTOGUI_AVAILABLE = True
except ImportError:
    PYAUTOGUI_AVAILABLE = False
    logger.warning("PyAutoGUI not installed. Mouse simulation will be disabled.")

try:
    from playsound import playsound
    PLAYSOUND_AVAILABLE = True
except ImportError:
    PLAYSOUND_AVAILABLE = False
    logger.warning("Playsound not installed. Sound alerts will be disabled.")

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright not installed. Using fallback browser methods.")

# Define a sound alert file path
ALERT_SOUND_FILE = "alert.mp3"

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


class StealthChromeLauncher:
    """Launches Chrome with stealth settings to bypass detection"""
    
    def __init__(self, user_data_dir="chrome_profile"):
        self.user_data_dir = os.path.abspath(user_data_dir)
        self.chrome_process = None
        self.chrome_path = self._find_chrome_path()
        
        # Create profile directory if it doesn't exist
        os.makedirs(self.user_data_dir, exist_ok=True)
    
    def _find_chrome_path(self):
        """Find the Chrome executable path based on the operating system"""
        if os.name == 'nt':  # Windows
            paths = [
                os.path.join(os.environ.get('PROGRAMFILES', 'C:\\Program Files'), 'Google\\Chrome\\Application\\chrome.exe'),
                os.path.join(os.environ.get('PROGRAMFILES(X86)', 'C:\\Program Files (x86)'), 'Google\\Chrome\\Application\\chrome.exe'),
                os.path.join(os.environ.get('LOCALAPPDATA', ''), 'Google\\Chrome\\Application\\chrome.exe')
            ]
        elif os.name == 'posix':  # macOS or Linux
            if sys.platform == 'darwin':  # macOS
                paths = ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome']
            else:  # Linux
                paths = [
                    '/usr/bin/google-chrome',
                    '/usr/bin/google-chrome-stable',
                    '/usr/bin/chromium',
                    '/usr/bin/chromium-browser'
                ]
        else:
            raise OSError(f"Unsupported operating system: {os.name}")
        
        for path in paths:
            if os.path.exists(path):
                return path
        
        raise FileNotFoundError("Could not find Google Chrome. Please install it or provide the path manually.")
    
    def launch(self, url):
        """Launch Chrome with stealth settings"""
        # Generate a random user agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        ]
        user_agent = random.choice(user_agents)
        
        # Define viewport sizes
        viewport_sizes = ["1920,1080", "1366,768", "1536,864", "1440,900", "1280,720"]
        viewport_size = random.choice(viewport_sizes)
        
        # Configure Chrome arguments for stealth mode
        chrome_args = [
            self.chrome_path,
            f"--user-data-dir={self.user_data_dir}",
            f"--user-agent={user_agent}",
            f"--window-size={viewport_size}",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
            "--disable-web-security",
            "--allow-running-insecure-content",
            "--disable-infobars",
            "--disable-popup-blocking",
            "--disable-extensions-http-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            url
        ]
        
        logger.info(f"Launching Chrome with stealth settings to: {url}")
        
        try:
            # Launch Chrome with the stealth arguments
            self.chrome_process = subprocess.Popen(chrome_args)
            
            # Allow time for Chrome to start
            time.sleep(5)
            logger.info("Chrome launched successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to launch Chrome: {e}")
            return False
    
    def is_running(self):
        """Check if the Chrome process is still running"""
        if self.chrome_process:
            return self.chrome_process.poll() is None
        return False
    
    def close(self):
        """Close Chrome browser"""
        if self.chrome_process and self.is_running():
            self.chrome_process.terminate()
            time.sleep(1)
            if self.is_running():
                self.chrome_process.kill()
            logger.info("Chrome terminated")
            return True
        return False


class PlaywrightStealth:
    """Playwright-based stealth browser for advanced bypass"""
    
    def __init__(self, user_data_dir="playwright_profile"):
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is not installed. Run 'pip install playwright' and 'playwright install chromium'")
        
        self.user_data_dir = os.path.abspath(user_data_dir)
        self.playwright = None
        self.browser_context = None
        self.page = None
        
        # Create profile directory if it doesn't exist
        os.makedirs(self.user_data_dir, exist_ok=True)
    
    def launch(self, url):
        """Launch a stealth browser using Playwright"""
        logger.info(f"Launching Playwright browser to: {url}")
        
        try:
            self.playwright = sync_playwright().start()
            
            # Configure browser for stealth
            self.browser_context = self.playwright.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=False,  # Must use headed mode to allow manual CAPTCHA solving
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
                java_script_enabled=True,
                locale="en-US",
                timezone_id="America/New_York",
                color_scheme="light",
                bypass_csp=True,
                accept_downloads=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-web-security",
                    "--disable-site-isolation-trials",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            
            # Create a new page and add stealth scripts
            self.page = self.browser_context.new_page()
            
            # Add script to avoid detection
            self.page.add_init_script("""
            // Override JS detection methods
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
            
            // Override Chrome-specific properties
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {},
            };
            
            // Override permissions API
            if (navigator.permissions) {
                navigator.permissions.query = (parameters) => {
                    return Promise.resolve({ state: 'granted' });
                };
            }
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    return [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                        { name: 'Native Client', filename: 'internal-nacl-plugin' }
                    ];
                }
            });
            
            // Override the language settings
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            // Override hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8,
            });
            """)
            
            # Navigate to the URL
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            logger.info("Playwright browser launched successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to launch Playwright browser: {e}")
            self.close()
            return False
    
    def download_page(self, url, output_path):
        """Navigate to a URL and save the page content"""
        try:
            # Navigate to the URL
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Wait for content to load
            time.sleep(random.uniform(5, 10))
            
            # Check for CAPTCHA
            if self.check_for_captcha():
                time.sleep(5)  # Additional time after CAPTCHA
            
            # Simulate human behavior
            self.simulate_human_behavior()
            
            # Save the page content
            content = self.page.content()
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Saved page content to: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading page: {e}")
            return False
    
    def check_for_captcha(self):
        """Check and handle CAPTCHA challenges"""
        captcha_indicators = [
            "press and hold", "prove you're human", "captcha", 
            "are you a robot", "not a robot", "security check"
        ]
        
        # Check for text indicators
        page_content = self.page.content().lower()
        captcha_detected = any(indicator in page_content for indicator in captcha_indicators)
        
        # Check for common CAPTCHA elements
        if not captcha_detected:
            try:
                captcha_selectors = [
                    "iframe[src*='captcha']",
                    "div[class*='captcha']",
                    "div[id*='captcha']",
                    "div[class*='px-captcha']",
                    "div[id*='px-captcha']"
                ]
                
                for selector in captcha_selectors:
                    if self.page.query_selector(selector):
                        captcha_detected = True
                        break
            except Exception:
                pass
        
        if captcha_detected:
            logger.info("CAPTCHA detected! Waiting for manual intervention...")
            
            # Take a screenshot
            try:
                screenshot_name = f"captcha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.page.screenshot(path=screenshot_name)
                logger.info(f"Screenshot saved as {screenshot_name}")
            except Exception as e:
                logger.error(f"Failed to save screenshot: {e}")
            
            # Play alert sound
            if PLAYSOUND_AVAILABLE and os.path.exists(ALERT_SOUND_FILE):
                try:
                    playsound(ALERT_SOUND_FILE)
                except Exception:
                    pass
            
            input("\n>>> CAPTCHA DETECTED! Solve the CAPTCHA manually in the browser window, then press Enter to continue...")
            
            # Wait for page to update after CAPTCHA
            time.sleep(5)
            return True
        
        return False
    
    def simulate_human_behavior(self):
        """Simulate human-like behavior"""
        # Random scrolling
        for _ in range(random.randint(2, 5)):
            scroll_amount = random.randint(100, 500) * (1 if random.random() < 0.8 else -1)  # Mostly down
            self.page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            time.sleep(random.uniform(0.5, 2.0))
        
        # Random mouse movements (if page is active)
        if self.page:
            for _ in range(random.randint(2, 4)):
                viewport = self.page.viewport_size
                x = random.randint(50, viewport["width"] - 50)
                y = random.randint(50, viewport["height"] - 50)
                self.page.mouse.move(x, y)
                time.sleep(random.uniform(0.3, 1.0))
        
        # Additional random wait
        time.sleep(random.uniform(1.0, 3.0))
    
    def close(self):
        """Close the browser and clean up resources"""
        try:
            if self.browser_context:
                self.browser_context.close()
            if self.playwright:
                self.playwright.stop()
            logger.info("Playwright browser closed")
        except Exception as e:
            logger.error(f"Error closing Playwright browser: {e}")


class SeekingAlphaDownloader:
    """Main class for downloading Seeking Alpha content"""
    
    def __init__(self, output_dir="sa_content", user_data_dir="chrome_profile", use_playwright=True):
        self.output_dir = output_dir
        self.user_data_dir = user_data_dir
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.browser = None
        self.db = None
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Connect to database if needed
        try:
            self.db = DBConnector()
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            logger.warning("Continuing without database connection.")
    
    def _init_browser(self, url):
        """Initialize the appropriate browser"""
        if self.browser:
            self.close_browser()
        
        if self.use_playwright:
            try:
                self.browser = PlaywrightStealth(user_data_dir=self.user_data_dir)
                return self.browser.launch(url)
            except Exception as e:
                logger.error(f"Playwright browser initialization failed: {e}")
                logger.info("Falling back to Chrome launcher.")
                self.use_playwright = False
        
        # Fall back to Chrome launcher
        self.browser = StealthChromeLauncher(user_data_dir=self.user_data_dir)
        return self.browser.launch(url)
    
    def close_browser(self):
        """Close the browser"""
        if self.browser:
            self.browser.close()
            self.browser = None
    
    def manual_login(self):
        """Launch browser for manual login"""
        logger.info("Launching browser for manual login to Seeking Alpha...")
        
        # Initialize browser
        success = self._init_browser("https://seekingalpha.com/login")
        if not success:
            logger.error("Failed to launch browser for login.")
            return False
        
        # Display instructions
        print("\n" + "="*70)
        print("MANUAL LOGIN REQUIRED")
        print("Please log in to your Seeking Alpha premium account in the browser window.")
        print("Take your time to complete any CAPTCHAs or security checks.")
        print("After logging in successfully, return to this console.")
        print("="*70)
        
        input("\nPress Enter AFTER you have successfully logged in...")
        
        # Verify login
        if self.use_playwright and isinstance(self.browser, PlaywrightStealth):
            self.browser.page.goto("https://seekingalpha.com")
            time.sleep(3)
            
            # Check for CAPTCHA
            self.browser.check_for_captcha()
            
            # Check login status
            page_content = self.browser.page.content()
            if any(x in page_content for x in ["Sign Out", "My Portfolio", "My Account", "Premium"]):
                logger.info("✓ Login successful!")
                
                # Keep browser open for a while to confirm
                time.sleep(5)
                return True
            else:
                logger.error("✗ Login verification failed.")
                
                # Ask user to confirm if logged in
                if input("Are you sure you're logged in? (y/n): ").lower() == 'y':
                    return True
                return False
        else:
            # For Chrome launcher, we just trust the user
            return True
    
    def download_specific_article(self, url, title=None):
        """Download a specific article by URL"""
        logger.info(f"Downloading specific article: {url}")
        
        # Generate filename
        if title:
            safe_title = ''.join(c if c.isalnum() else '_' for c in title)
        else:
            # Extract title from URL
            url_parts = url.split('/')
            if len(url_parts) > 0:
                safe_title = url_parts[-1]
            else:
                safe_title = f"article_{int(time.time())}"
        
        filename = f"{safe_title[:50]}.html"
        filepath = os.path.join(self.output_dir, filename)
        
        # Check if file already exists
        if os.path.exists(filepath):
            logger.info(f"File already exists: {filename}")
            return True
        
        # Initialize browser with the URL
        if not self._init_browser(url):
            logger.error("Failed to initialize browser.")
            return False
        
        # Download the article using appropriate method
        success = False
        try:
            if self.use_playwright and isinstance(self.browser, PlaywrightStealth):
                # Use Playwright download method
                success = self.browser.download_page(url, filepath)
            else:
                # Manual download with Chrome launcher
                print("Manual download mode: Please manually save the page content when loaded.")
                print(f"The file should be saved to: {filepath}")
                input("Press Enter when you've saved the page content...")
                success = os.path.exists(filepath)
        except Exception as e:
            logger.error(f"Error downloading article: {e}")
            success = False
        finally:
            # Close browser
            self.close_browser()
        
        return success
    
    def download_content(self, batch_size=5, max_articles=None):
        """Download content from database links"""
        if not self.db:
            logger.error("Database connection is required for batch downloads.")
            return False
        
        articles_downloaded = 0
        total_retries = 0
        max_retries = 3
        
        # Main download loop
        while True:
            # Check if we've reached the maximum articles limit
            if max_articles and articles_downloaded >= max_articles:
                logger.info(f"Reached maximum articles limit ({max_articles}). Stopping.")
                break
            
            # Get unprocessed links
            remaining_count = max_articles - articles_downloaded if max_articles else batch_size
            current_batch_size = min(batch_size, remaining_count if max_articles else batch_size)
            links = self.db.get_unprocessed_links(current_batch_size)
            
            if not links:
                logger.info("No more unprocessed links. Stopping.")
                break
            
            logger.info(f"Processing batch of {len(links)} links...")
            
            # Process each link
            for link in links:
                link_id = link['id']
                title = link['title']
                url = link['url']
                
                logger.info(f"Downloading article: {title}")
                
                # Generate safe filename
                safe_title = ''.join(c if c.isalnum() else '_' for c in title)
                filename = f"{safe_title[:50]}.html"
                filepath = os.path.join(self.output_dir, filename)
                
                # Check if file already exists
                if os.path.exists(filepath):
                    logger.info(f"File already exists: {filename}")
                    self.db.mark_link_downloaded(link_id)
                    articles_downloaded += 1
                    continue
                
                # Attempt to download with retries
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        # Initialize browser
                        if not self._init_browser(url):
                            logger.error("Failed to initialize browser. Retrying...")
                            retry_count += 1
                            time.sleep(random.uniform(30, 60))
                            continue
                        
                        # Download the page
                        if self.use_playwright and isinstance(self.browser, PlaywrightStealth):
                            success = self.browser.download_page(url, filepath)
                        else:
                            # Manual save with Chrome launcher
                            input("\nPlease manually save the page content and press Enter when done...")
                            success = os.path.exists(filepath)
                        
                        # Mark as downloaded if successful
                        if success:
                            self.db.mark_link_downloaded(link_id)
                            articles_downloaded += 1
                        else:
                            retry_count += 1
                            total_retries += 1
                    except Exception as e:
                        logger.error(f"Error downloading article: {e}")
                        retry_count += 1
                        total_retries += 1
                    finally:
                        # Always close browser between downloads
                        self.close_browser()
                        
                        # Add longer delay between articles
                        delay = random.uniform(60, 180)  # 1-3 minutes
                        logger.info(f"Waiting {delay:.1f} seconds before next download...")
                        time.sleep(delay)
            
            # Take a longer break between batches
            batch_break = random.uniform(300, 900)  # 5-15 minutes
            logger.info(f"Batch completed. Taking a {batch_break/60:.1f} minute break...")
            time.sleep(batch_break)
            
            # Report progress
            stats = self.db.get_total_stats()
            logger.info(f"Progress: {stats['downloaded_links']}/{stats['total_links']} articles downloaded")
            
            # Decide whether to continue based on retry rate
            if total_retries > articles_downloaded * 2:  # High failure rate
                logger.warning("High failure rate detected. Please check your account status and connectivity.")
                if input("Continue downloading? (y/n): ").lower() != 'y':
                    break
        
        return articles_downloaded
    
    def close(self):
        """Clean up resources"""
        self.close_browser()
        if self.db:
            self.db.close()


def create_notification_sound():
    """Create a simple notification sound file if it doesn't exist"""
    if PLAYSOUND_AVAILABLE and not os.path.exists(ALERT_SOUND_FILE):
        try:
            # Try to generate a simple beep sound file
            import numpy as np
            from scipy.io import wavfile
            
            sample_rate = 44100
            duration = 1.0  # seconds
            frequency = 440  # Hz (A4)
            
            t = np.linspace(0, duration, int(sample_rate * duration), False)
            tone = np.sin(2 * np.pi * frequency * t) * 0.5
            fade = np.ones_like(tone)
            fade[:int(0.1 * sample_rate)] = np.linspace(0, 1, int(0.1 * sample_rate))
            fade[-int(0.1 * sample_rate):] = np.linspace(1, 0, int(0.1 * sample_rate))
            tone = tone * fade
            
            # Convert to 16-bit PCM
            tone = (tone * 32767).astype(np.int16)
            
            # Save as WAV
            wavfile.write(ALERT_SOUND_FILE.replace('.mp3', '.wav'), sample_rate, tone)
            logger.info(f"Created notification sound: {ALERT_SOUND_FILE.replace('.mp3', '.wav')}")
        except ImportError:
            logger.debug("Scipy not installed. Could not create notification sound.")
        except Exception as e:
            logger.debug(f"Failed to create notification sound: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Ultimate PerimeterX Bypass for Seeking Alpha")
    parser.add_argument("--output", default="sa_content", help="Output directory for HTML files")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of links to process in each batch")
    parser.add_argument("--max-articles", type=int, help="Maximum number of articles to download")
    parser.add_argument("--login", action="store_true", help="Launch browser for manual login only")
    parser.add_argument("--url", help="Download a specific article by URL")
    parser.add_argument("--no-playwright", action="store_true", help="Disable Playwright and use Chrome directly")
    parser.add_argument("--profile", default="chrome_profile", help="Browser profile directory")
    
    args = parser.parse_args()
    
    try:
        # Create notification sound
        create_notification_sound()
        
        # Initialize downloader
        downloader = SeekingAlphaDownloader(
            output_dir=args.output,
            user_data_dir=args.profile,
            use_playwright=not args.no_playwright
        )
        
        if args.login:
            # Just do manual login and exit
            if downloader.manual_login():
                logger.info("Login successful. Session saved for future use.")
            else:
                logger.error("Login failed. Please try again.")
        elif args.url:
            # Download specific article
            if downloader.download_specific_article(args.url):
                logger.info("Article downloaded successfully.")
            else:
                logger.error("Failed to download article.")
        else:
            # Regular batch download
            articles_downloaded = downloader.download_content(
                batch_size=args.batch_size,
                max_articles=args.max_articles
            )
            logger.info(f"Download session completed. Downloaded {articles_downloaded} articles.")
    
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        if 'downloader' in locals():
            downloader.close()


if __name__ == "__main__":
    main()