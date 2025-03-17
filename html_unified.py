import os
import json
import re
import argparse
from bs4 import BeautifulSoup
from pathlib import Path

class TranscriptExtractor:
    def __init__(self, debug=False):
        self.debug = debug
    
    def extract_from_file(self, html_file):
        """Extract transcript content from an HTML file"""
        print(f"Processing: {html_file}")
        
        with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
            html_content = f.read()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract basic metadata
        title = self.extract_title(soup)
        date = self.extract_date(soup)
        author = self.extract_author(soup)
        
        # Try multiple methods to extract the transcript content
        content = self.extract_content(soup, html_content)
        
        # Create result object
        result = {
            'title': title,
            'date': date,
            'author': author,
            'content': content
        }
        
        return result
    
    def extract_title(self, soup):
        """Extract the article title"""
        title_selectors = ["h1", "h1.title", "[data-test-id='post-title']", ".title"]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                return title_elem.text.strip()
        
        return "Title not found"
    
    def extract_date(self, soup):
        """Extract the publication date"""
        date_selectors = ["time", "[data-test-id='post-date']", ".post-date", ".sa-art-date"]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                return date_elem.text.strip()
        
        return "Date not found"
    
    def extract_author(self, soup):
        """Extract the author name"""
        author_selectors = ["[data-test-id='author-name']", ".author-link", ".author-name"]
        
        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                return author_elem.text.strip()
        
        return "Author not found"
    
    def extract_content(self, soup, html_content):
        """Extract the transcript content using multiple methods"""
        # Method 1: Look for transcript-specific markers
        content = self.extract_transcript_sections(soup)
        if content and len(content) > 500:
            return content
        
        # Method 2: Look for article content containers
        content = self.extract_from_content_containers(soup)
        if content and len(content) > 500:
            return content
        
        # Method 3: Look for paragraphs after header patterns
        content = self.extract_after_header_patterns(soup)
        if content and len(content) > 500:
            return content
        
        # Method 4: Extract content from script tags (for dynamic content)
        content = self.extract_from_scripts(html_content)
        if content and len(content) > 500:
            return content
        
        # Method 5: Use pre-formatted elements that may contain transcript text
        content = self.extract_from_pre_elements(soup)
        if content and len(content) > 500:
            return content
        
        # Method 6: Direct HTML parsing for specific transcript patterns
        content = self.extract_transcript_pattern(html_content)
        if content and len(content) > 500:
            return content
        
        # If all else fails, try to get all paragraph text
        all_paragraphs = soup.select("p")
        if all_paragraphs:
            return "\n\n".join([p.text.strip() for p in all_paragraphs if len(p.text.strip()) > 20])
        
        return "Content extraction failed"
    
    def extract_transcript_sections(self, soup):
        """Extract content from specific transcript sections"""
        # Look for elements that typically contain transcript sections
        transcript_sections = soup.select(".transcript-section, .transcript-text, .sa-transcript, [data-id='sa-transcript'], [id='sa-transcript']")
        if transcript_sections:
            return "\n\n".join([section.text.strip() for section in transcript_sections])
        
        # Look for Q&A section markers
        qa_sections = soup.select("strong:contains('Question-and-Answer'), h2:contains('Q&A'), h3:contains('Questions and Answers')")
        if qa_sections:
            # If we find Q&A sections, try to extract the entire transcript
            content_parts = []
            current_section = qa_sections[0].parent
            while current_section:
                if current_section.name in ['div', 'section']:
                    content_parts.append(current_section.text.strip())
                current_section = current_section.find_next_sibling()
            if content_parts:
                return "\n\n".join(content_parts)
        
        return ""
    
    def extract_from_content_containers(self, soup):
        """Extract from known content container selectors"""
        container_selectors = [
            "div[data-test-id='content-container']", 
            ".paywall-content",
            "#content-container",
            ".sa-art", 
            "article.sa-content",
            ".article-content",
            "#a-body",
            "div.media-article-content"
        ]
        
        for selector in container_selectors:
            container = soup.select_one(selector)
            if container:
                # Skip containers with premium messages only
                if "Make the most of Premium" in container.text and len(container.text) < 100:
                    continue
                    
                # Extract paragraphs
                paragraphs = container.select("p")
                if paragraphs:
                    filtered_paragraphs = []
                    for p in paragraphs:
                        p_text = p.text.strip()
                        # Filter out common non-content paragraphs
                        if p_text and len(p_text) > 20 and not any(x in p_text.lower() for x in [
                            "disclosure:", "disclosure :", "©", "all rights reserved", 
                            "seeking alpha", "editor's note", "make the most of premium"
                        ]):
                            filtered_paragraphs.append(p_text)
                    
                    if filtered_paragraphs:
                        return "\n\n".join(filtered_paragraphs)
        
        return ""
    
    def extract_after_header_patterns(self, soup):
        """Extract content after transcript headers"""
        # Look for headers that typically indicate the start of a transcript
        transcript_headers = soup.select("h2:contains('Transcript'), h2:contains('Earnings Call'), h3:contains('Transcript'), h3:contains('Earnings Call')")
        if transcript_headers:
            header = transcript_headers[0]
            
            # Get all following paragraphs
            paragraphs = []
            current_elem = header.find_next_sibling()
            while current_elem:
                if current_elem.name == 'p':
                    paragraphs.append(current_elem.text.strip())
                current_elem = current_elem.find_next_sibling()
            
            if paragraphs:
                return "\n\n".join(paragraphs)
        
        return ""
    
    def extract_from_scripts(self, html_content):
        """Try to extract content from script tags (for dynamic content)"""
        # Look for JSON data in script tags that might contain the transcript
        script_pattern = re.compile(r'<script[^>]*>(.*?)</script>', re.DOTALL)
        json_pattern = re.compile(r'\{[^}]*"transcript"[^}]*\}')
        content_pattern = re.compile(r'"content"\s*:\s*"([^"]*)"')
        text_pattern = re.compile(r'"text"\s*:\s*"([^"]*)"')
        
        script_matches = script_pattern.findall(html_content)
        
        for script in script_matches:
            # Look for JSON with transcript data
            json_match = json_pattern.search(script)
            if json_match:
                # Try to extract content
                content_match = content_pattern.search(script)
                if content_match:
                    content = content_match.group(1)
                    # Unescape JSON content
                    content = content.replace('\\"', '"').replace('\\n', '\n')
                    return content
                
                # Try to extract text
                text_match = text_pattern.search(script)
                if text_match:
                    text = text_match.group(1)
                    # Unescape JSON content
                    text = text.replace('\\"', '"').replace('\\n', '\n')
                    return text
        
        return ""
    
    def extract_from_pre_elements(self, soup):
        """Extract from pre-formatted elements that might contain the transcript"""
        pre_elements = soup.select("pre")
        if pre_elements:
            pre_texts = [pre.text.strip() for pre in pre_elements if len(pre.text.strip()) > 500]
            if pre_texts:
                return "\n\n".join(pre_texts)
        
        return ""
    
    def extract_transcript_pattern(self, html_content):
        """Look for specific transcript patterns in the HTML"""
        # Try to find sections with speaker names followed by text
        speaker_pattern = re.compile(r'<strong>([^<:]+):</strong>([^<]+)', re.IGNORECASE)
        matches = speaker_pattern.findall(html_content)
        
        if matches and len(matches) > 10:  # Only consider if we find multiple speaker segments
            transcript = []
            for speaker, text in matches:
                transcript.append(f"{speaker.strip()}: {text.strip()}")
            
            return "\n\n".join(transcript)
        
        return ""
    
    def process_directory(self, input_dir, output_dir):
        """Process all HTML files in a directory"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Get all HTML files
        html_files = list(input_path.glob("*.html"))
        print(f"Found {len(html_files)} HTML files to process")
        
        success_count = 0
        for html_file in html_files:
            try:
                # Extract content
                result = self.extract_from_file(html_file)
                
                # Generate output filename
                json_file = output_path / f"{html_file.stem}_extracted.json"
                
                # Save JSON
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=4, ensure_ascii=False)
                
                print(f"✓ Saved extracted content to {json_file}")
                
                # Check if content was meaningful
                if len(result['content']) > 500 and "Make the most of Premium" not in result['content']:
                    success_count += 1
                else:
                    print(f"⚠ Warning: Content may be incomplete for {html_file.name}")
                
            except Exception as e:
                print(f"❌ Error processing {html_file}: {e}")
        
        print(f"\nProcessing complete! Successfully extracted {success_count} out of {len(html_files)} files")


def main():
    parser = argparse.ArgumentParser(description="Extract transcript content from HTML files")
    parser.add_argument("--input", required=True, help="Input directory containing HTML files")
    parser.add_argument("--output", default="extracted_transcripts", help="Output directory for JSON files")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    extractor = TranscriptExtractor(debug=args.debug)
    extractor.process_directory(args.input, args.output)


if __name__ == "__main__":
    main()