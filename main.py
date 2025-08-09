import os
import json
import glob
import asyncio
import hashlib
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright
from markdownify import markdownify as md
from google import genai
import dotenv

# Load environment variables
dotenv.load_dotenv()

app = FastAPI(title="Website FAQ API", description="API for retrieving website FAQs and last updated information")

def generate_faq_from_markdown(md_path: str, model_name: str = "gemini-1.5-flash") -> str:
    """Generate FAQ from markdown content using Google Gemini AI"""
    api_key = os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_GENERATIVE_AI_API_KEY not found in environment variables.")
    
    client = genai.Client(api_key=api_key)
    with open(md_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()
    
    prompt = (
        """
        You are an expert at summarizing website content and generating helpful FAQs for users.\n
        Given the following page content in markdown, generate a concise FAQ (5-10 Q&A pairs) that covers the most important and relevant information for a user.\n
        Format the output as markdown, with each question as a bold heading and the answer as a paragraph below.\n
        Markdown content:\n\n""" + markdown_content
    )
    
    response = client.models.generate_content(
        model=model_name,
        contents=prompt
    )
    
    faq_md = response.text
    faq_dir = os.path.join("storage", "datasets", "faqs")
    os.makedirs(faq_dir, exist_ok=True)
    base_name = os.path.basename(md_path).replace(".md", "_faq.md")
    faq_path = os.path.join(faq_dir, base_name)
    
    with open(faq_path, "w", encoding="utf-8") as f:
        f.write(faq_md)
    
    return faq_path

async def crawl_and_generate_faq(url: str) -> Dict[str, str]:
    """Crawl a single URL and generate FAQ for it"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
            page = await browser.new_page()
            
            # Navigate to the page
            await page.goto(url, wait_until="networkidle")
            
            # Get response headers
            response = await page.context.request.get(url)
            headers = response.headers if response else {}
            
            last_modified = headers.get("last-modified")
            etag = headers.get("etag")
            
            # Get page content
            content = await page.content()
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            
            # Create identifier for change detection
            identifier_parts = []
            if last_modified:
                identifier_parts.append(f"last_modified:{last_modified}")
            if etag:
                identifier_parts.append(f"etag:{etag}")
            identifier_parts.append(f"content_hash:{content_hash}")
            identifier = "|".join(identifier_parts)
            
            # Convert to markdown
            markdown_content = md(content)
            
            # Save markdown content
            md_dir = os.path.join("storage", "datasets", "page_content")
            os.makedirs(md_dir, exist_ok=True)
            parsed_url = urlparse(url)
            path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
            base_name = '_'.join(filter(None, [
                ''.join(c if c.isalnum() or c in '-_' else '_' for c in (path_parts[-1] if path_parts else 'index'))
            ])) or 'index'
            domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
            md_filename = f"{domain_prefix}_{base_name}.md"
            md_path = os.path.join(md_dir, md_filename[:255])
            
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(f"# {await page.title()}\n\n")
                f.write(f"**URL:** {url}\n\n")
                f.write(markdown_content)
            
            # Generate FAQ
            faq_path = generate_faq_from_markdown(md_path)
            
            # Update change detection data
            change_detection_file = os.path.join("storage", "change_detection.json")
            os.makedirs(os.path.dirname(change_detection_file), exist_ok=True)
            
            try:
                with open(change_detection_file, 'r') as f:
                    change_detection_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                change_detection_data = {}
            
            change_detection_data[url] = identifier
            
            with open(change_detection_file, 'w') as f:
                json.dump(change_detection_data, f, indent=2)
            
            await browser.close()
            
            return {
                "url": url,
                "last_modified": last_modified,
                "md_path": md_path,
                "faq_path": faq_path,
                "identifier": identifier
            }
            
    except Exception as e:
        raise Exception(f"Failed to crawl {url}: {str(e)}")

async def crawl_entire_website(base_url: str, max_pages: int = 50) -> List[Dict[str, str]]:
    """Crawl an entire website starting from the base URL"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
            context = await browser.new_context()
            page = await context.new_page()
            
            # Set up storage for change detection
            change_detection_file = os.path.join("storage", "change_detection.json")
            os.makedirs(os.path.dirname(change_detection_file), exist_ok=True)
            
            try:
                with open(change_detection_file, 'r') as f:
                    change_detection_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                change_detection_data = {}
            
            crawled_urls = []
            urls_to_crawl = [base_url]
            crawled_count = 0
            
            while urls_to_crawl and crawled_count < max_pages:
                current_url = urls_to_crawl.pop(0)
                
                # Skip if already crawled
                if current_url in change_detection_data:
                    continue
                
                try:
                    # Navigate to the page
                    await page.goto(current_url, wait_until="networkidle", timeout=30000)
                    
                    # Get response headers
                    response = await context.request.get(current_url)
                    headers = response.headers if response else {}
                    
                    last_modified = headers.get("last-modified")
                    etag = headers.get("etag")
                    
                    # Get page content
                    content = await page.content()
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    
                    # Create identifier for change detection
                    identifier_parts = []
                    if last_modified:
                        identifier_parts.append(f"last_modified:{last_modified}")
                    if etag:
                        identifier_parts.append(f"etag:{etag}")
                    identifier_parts.append(f"content_hash:{content_hash}")
                    identifier = "|".join(identifier_parts)
                    
                    # Convert to markdown
                    markdown_content = md(content)
                    
                    # Save markdown content
                    md_dir = os.path.join("storage", "datasets", "page_content")
                    os.makedirs(md_dir, exist_ok=True)
                    parsed_url = urlparse(current_url)
                    path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
                    base_name = '_'.join(filter(None, [
                        ''.join(c if c.isalnum() or c in '-_' else '_' for c in (path_parts[-1] if path_parts else 'index'))
                    ])) or 'index'
                    domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
                    md_filename = f"{domain_prefix}_{base_name}.md"
                    md_path = os.path.join(md_dir, md_filename[:255])
                    
                    with open(md_path, "w", encoding="utf-8") as f:
                        f.write(f"# {await page.title()}\n\n")
                        f.write(f"**URL:** {current_url}\n\n")
                        f.write(markdown_content)
                    
                    # Generate FAQ
                    faq_path = generate_faq_from_markdown(md_path)
                    
                    # Update change detection data
                    change_detection_data[current_url] = identifier
                    
                    crawled_urls.append({
                        "url": current_url,
                        "last_modified": last_modified,
                        "md_path": md_path,
                        "faq_path": faq_path,
                        "identifier": identifier
                    })
                    
                    crawled_count += 1
                    
                    # Find links to crawl (same domain only)
                    links = await page.query_selector_all("a[href]")
                    base_domain = urlparse(base_url).netloc
                    
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            # Resolve relative URLs
                            if href.startswith('/'):
                                full_url = f"{urlparse(base_url).scheme}://{base_domain}{href}"
                            elif href.startswith('http'):
                                full_url = href
                            else:
                                continue
                            
                            # Only crawl same domain
                            if urlparse(full_url).netloc == base_domain:
                                if full_url not in change_detection_data and full_url not in urls_to_crawl:
                                    urls_to_crawl.append(full_url)
                    
                except Exception as e:
                    print(f"Failed to crawl {current_url}: {str(e)}")
                    continue
            
            # Save final change detection data
            with open(change_detection_file, 'w') as f:
                json.dump(change_detection_data, f, indent=2)
            
            await browser.close()
            return crawled_urls
            
    except Exception as e:
        raise Exception(f"Failed to crawl website {base_url}: {str(e)}")

def get_change_detection_data() -> Dict[str, str]:
    """Load change detection data from crawler storage"""
    change_detection_file = os.path.join("storage", "change_detection.json")
    try:
        with open(change_detection_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def get_last_updated_from_identifier(identifier: str) -> Optional[str]:
    """Extract last_modified timestamp from change detection identifier"""
    if not identifier:
        return None
    
    parts = identifier.split("|")
    for part in parts:
        if part.startswith("last_modified:"):
            return part.replace("last_modified:", "")
    return None

def find_faq_file_for_url(url: str) -> Optional[str]:
    """Find the FAQ file corresponding to a specific URL"""
    parsed_url = urlparse(url)
    path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
    base_name = '_'.join(filter(None, [
        ''.join(c if c.isalnum() or c in '-_' else '_' for c in (path_parts[-1] if path_parts else 'index'))
    ])) or 'index'
    domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
    expected_filename = f"{domain_prefix}_{base_name}_faq.md"
    
    faq_dir = os.path.join("storage", "datasets", "faqs")
    faq_path = os.path.join(faq_dir, expected_filename[:255])
    
    if os.path.exists(faq_path):
        return faq_path
    
    # Fallback: search for files that might match
    pattern = os.path.join(faq_dir, f"{domain_prefix}_*_faq.md")
    matching_files = glob.glob(pattern)
    if matching_files:
        return matching_files[0]  # Return first match
    
    return None

def read_faq_content(faq_path: str) -> List[Dict[str, str]]:
    """Read and parse FAQ content from markdown file"""
    try:
        with open(faq_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Simple parsing of markdown FAQ format
        faqs = []
        lines = content.split('\n')
        current_question = None
        current_answer = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('**') and line.endswith('**'):
                # Save previous FAQ if exists
                if current_question and current_answer:
                    faqs.append({
                        "question": current_question,
                        "answer": ' '.join(current_answer).strip()
                    })
                
                # Start new FAQ
                current_question = line.strip('*')
                current_answer = []
            elif line and current_question:
                current_answer.append(line)
        
        # Add the last FAQ
        if current_question and current_answer:
            faqs.append({
                "question": current_question,
                "answer": ' '.join(current_answer).strip()
            })
        
        return faqs
    except Exception as e:
        return [{"error": f"Failed to read FAQ file: {str(e)}"}]

def get_all_faqs_for_domain(base_url: str) -> List[Dict[str, str]]:
    """Get all FAQs for a specific domain"""
    parsed_url = urlparse(base_url)
    domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
    
    faq_dir = os.path.join("storage", "datasets", "faqs")
    pattern = os.path.join(faq_dir, f"{domain_prefix}_*_faq.md")
    matching_files = glob.glob(pattern)
    
    all_faqs = []
    for faq_file in matching_files:
        try:
            with open(faq_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # Extract URL from the content if available
                url_match = None
                for line in content.split('\n'):
                    if line.startswith('**URL:**'):
                        url_match = line.replace('**URL:**', '').strip()
                        break
                
                faqs = read_faq_content(faq_file)
                for faq in faqs:
                    if url_match:
                        faq['source_url'] = url_match
                    all_faqs.append(faq)
        except Exception as e:
            all_faqs.append({"error": f"Failed to read {faq_file}: {str(e)}"})
    
    return all_faqs

@app.get("/last-updated")
async def last_updated(url: str = Query(..., description="The URL to check for last updated time")):
    """
    Get the last updated time for a specific URL.
    
    If the URL hasn't been crawled yet, it will be crawled automatically.
    Returns the last_modified header timestamp if available, otherwise returns None.
    """
    change_data = get_change_detection_data()
    
    if url not in change_data:
        # URL not found, crawl it automatically
        try:
            crawl_result = await crawl_and_generate_faq(url)
            last_updated_time = crawl_result.get("last_modified")
            return {
                "url": url, 
                "last_updated": last_updated_time,
                "has_been_crawled": True,
                "just_crawled": True
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to crawl URL: {str(e)}")
    
    identifier = change_data[url]
    last_updated_time = get_last_updated_from_identifier(identifier)
    
    return {
        "url": url, 
        "last_updated": last_updated_time,
        "has_been_crawled": True,
        "just_crawled": False
    }

@app.get("/page-faqs")
async def page_faqs(url: str = Query(..., description="The URL to get FAQs for")):
    """
    Get the last updated time and FAQs for a specific page.
    
    If the URL hasn't been crawled yet, it will be crawled automatically and FAQs will be generated.
    Returns both the last updated timestamp and the generated FAQs for the page.
    """
    change_data = get_change_detection_data()
    just_crawled = False
    
    if url not in change_data:
        # URL not found, crawl it automatically
        try:
            crawl_result = await crawl_and_generate_faq(url)
            last_updated_time = crawl_result.get("last_modified")
            faq_path = crawl_result.get("faq_path")
            just_crawled = True
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to crawl URL: {str(e)}")
    else:
        identifier = change_data[url]
        last_updated_time = get_last_updated_from_identifier(identifier)
        faq_path = find_faq_file_for_url(url)
    
    if not faq_path:
        return {
            "url": url,
            "last_updated": last_updated_time,
            "faqs": [],
            "message": "No FAQ file found for this URL",
            "just_crawled": just_crawled
        }
    
    faqs = read_faq_content(faq_path)
    
    return {
        "url": url,
        "last_updated": last_updated_time,
        "faqs": faqs,
        "faq_file": faq_path,
        "just_crawled": just_crawled
    }

@app.get("/site-faqs")
async def site_faqs(base_url: str = Query(..., description="The base URL to get all FAQs for")):
    """
    Get all FAQs for an entire website.
    
    If the base URL hasn't been crawled yet, it will be crawled automatically.
    Returns all generated FAQs across all pages of the website.
    """
    # Validate that the base URL has been crawled
    change_data = get_change_detection_data()
    crawled_urls = [url for url in change_data.keys() if urlparse(url).netloc == urlparse(base_url).netloc]
    just_crawled = False
    
    if not crawled_urls:
        # Domain not found, crawl the entire website automatically
        try:
            crawl_results = await crawl_entire_website(base_url)
            just_crawled = True
            # Refresh the data after crawling
            change_data = get_change_detection_data()
            crawled_urls = [url for url in change_data.keys() if urlparse(url).netloc == urlparse(base_url).netloc]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to crawl website: {str(e)}")
    
    all_faqs = get_all_faqs_for_domain(base_url)
    
    return {
        "base_url": base_url,
        "total_faqs": len(all_faqs),
        "crawled_pages": len(crawled_urls),
        "faqs": all_faqs,
        "just_crawled": just_crawled,
        "crawled_urls": crawled_urls
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
