# Seeking Alpha Scraper Suite

A comprehensive suite of tools for collecting, downloading, and extracting earnings call transcripts from Seeking Alpha.

## Overview

This project provides a set of Python scripts to automate the process of:

1. Collecting article links from Seeking Alpha
2. Downloading article HTML content
3. Extracting earnings call transcripts from the HTML content
4. Storing data in a structured format (SQL Server database and/or local files)

## Components

### Link Collection

- **link-collector.py**: Collects article links from Seeking Alpha author pages and stores them in a SQL Server database.
- **seekingalpha_scraper.py**: More advanced scraper with link collection functionality and improved anti-detection features.

### Content Downloading

- **content-downloader.py**: Downloads HTML content for collected links and stores them as local files.
- **seekingalpha_scraper.py**: Also provides content downloading capabilities with additional features.

### Transcript Extraction

- **transcript-extractor.py**: Extracts transcript content from downloaded HTML files and saves as JSON.
- **html_unified.py**: Standalone utility for extracting transcript content using various parsing techniques.

### Database Management

- **db-setup.py**: Sets up and manages the SQL Server database tables required for the project.

### All-in-One Solutions

- **unified.py**: Combined workflow for link collection, content downloading, and transcript extraction.

## Requirements

- Python 3.7+
- SQL Server database (for database storage mode)
- Chrome browser (for web scraping)

## Dependencies

- undetected_chromedriver
- BeautifulSoup4
- pyodbc
- python-dotenv
- And other standard Python libraries

## Setup

1. Create a `.env` file with your database credentials:
   ```
   UID="your_username"
   PID="your_password"
   SERVER="your_server"
   DATABASE="your_database"
   ```

2. Initialize the database:
   ```
   python db-setup.py
   ```

3. Use the scraper components as needed (see Usage section)

## Usage

### Link Collection

```bash
# Using link-collector.py (database mode)
python link-collector.py --url "https://seekingalpha.com/author/your-author" --max-links 500 --headless

# Using seekingalpha_scraper.py (CSV mode)
python seekingalpha_scraper.py links --url "https://seekingalpha.com/author/your-author" --output "links.csv" --max-pages 10
```

### Content Downloading

```bash
# Using content-downloader.py (database mode)
python content-downloader.py --output "sa_content" --batch-size 50 --one-time

# Using seekingalpha_scraper.py (CSV mode)
python seekingalpha_scraper.py articles --csv "links.csv" --output "sa_content"
```

### Transcript Extraction

```bash
# Using transcript-extractor.py (database mode)
python transcript-extractor.py --html-dir "sa_content" --output "transcripts" --one-time

# Using html_unified.py (standalone mode)
python html_unified.py --input "sa_content" --output "transcripts"
```

### Unified Workflow

```bash
# Using unified.py (combined workflow)
python unified.py --author "https://seekingalpha.com/author/your-author" --output "output_directory" --max-links 500
```

## Advanced Features

- **Anti-Detection Measures**: Uses undetected_chromedriver and various browser fingerprinting techniques to avoid detection.
- **Captcha Handling**: Includes manual and automated captcha-handling capabilities.
- **Parallel Processing**: Supports multi-threaded downloads and extractions for improved performance.
- **Adaptive Delays**: Implements smart delays based on captcha encounters to avoid rate limiting.
- **Multiple Extraction Methods**: Uses various techniques to extract transcript content even when page structure changes.

## Project Structure

```
alpha/
├── .env                    # Environment variables
├── link-collector.py       # Link collection script (DB mode)
├── seekingalpha_scraper.py # Advanced scraper with multiple modes
├── content-downloader.py   # Content downloading script (DB mode)
├── transcript-extractor.py # Transcript extraction script (DB mode)
├── db-setup.py             # Database initialization script
├── unified.py              # All-in-one workflow script
├── html_unified.py         # Standalone HTML extraction utility
├── sa_content/             # Downloaded HTML content
└── logs/                   # Log files
```

## Notes

- Premium Seeking Alpha access is required to download full transcript content.
- The scripts handle login sessions and maintain cookies between runs.
- Be respectful of Seeking Alpha's servers by using reasonable delays between requests.
- This tool is designed for research and personal use only.

## Legal Disclaimer

This software is intended for personal research and educational purposes only. Users are responsible for ensuring their use of this software complies with Seeking Alpha's Terms of Service and all applicable laws. The authors of this software are not responsible for any misuse or violation of terms.
