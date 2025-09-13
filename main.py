import os
import json
import glob
import asyncio
import hashlib
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse, urljoin
from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright
from markdownify import markdownify as md
from google import genai
import dotenv
from change_detection import change_detector
from language_detection import language_detector
from url_filters import same_domain, strip_query, is_media, is_blocked
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables
dotenv.load_dotenv()

app = FastAPI(title="Website FAQ API", description="API for retrieving website last updated times and generating FAQs")

# Allow cross-origin requests so the static ui.html can call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def generate_faq_from_markdown(md_path: str, detected_language: str = "en", confidence: float = 1.0, target_language: str = None, model_name: str = "gemini-1.5-flash", script_hint: str = None) -> str:
    """Generate FAQ from markdown content using Google Gemini AI with language detection"""
    print(f"[generate_faq] Starting FAQ generation for: {md_path}")
    api_key = os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY")
    if not api_key:
        print("[generate_faq] ERROR: No API key found")
        raise ValueError("GOOGLE_GENERATIVE_AI_API_KEY not found in environment variables.")
    
    client = genai.Client(api_key=api_key)
    with open(md_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()
    
    print(f"[generate_faq] Read markdown content, length: {len(markdown_content)}")
    
    # Extract title and URL from the markdown header written during crawl
    title = None
    page_url = None
    for line in markdown_content.splitlines():
        if not title and line.startswith("# "):
            title = line[2:].strip()
        elif not page_url and line.startswith("**URL:**"):
            page_url = line.replace("**URL:**", "").strip()
        if title and page_url:
            break
    if not title:
        title = "FAQ"
    
    print(f"[generate_faq] Extracted title: {title}")
    print(f"[generate_faq] Extracted URL: {page_url}")
    
    # Determine the language to use for FAQ generation
    if target_language:
        # Use explicit target language (highest priority)
        final_language = target_language
        language_instruction = f"LANGUAGE REQUIREMENT: Generate FAQs in {target_language.upper()} language. Both questions and answers must be in {target_language.upper()}."
        print(f"[generate_faq] Using target language override: {target_language}")
    else:
        # Use detected language with improved directive
        final_language = detected_language
        language_instruction = language_detector.create_language_directive(detected_language, confidence, script_hint)
        print(f"[generate_faq] Using detected language: {detected_language} (confidence: {confidence:.2f}, script_hint: {script_hint})")
    
    prompt = (
        f"""
        You are an expert at summarizing website content and generating helpful FAQs for users.\n
        {language_instruction}\n
        Given the following page content in markdown, generate a concise FAQ (5-10 Q&A pairs) that covers the most important and relevant information for a user.\n
        Format the output as markdown, with each question as a bold heading and the answer as a paragraph below.\n
        Markdown content:\n\n""" + markdown_content
    )
    
    print(f"[generate_faq] Sending request to Gemini API...")
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt
        )
        
        faq_md = response.text
        print(f"[generate_faq] Received response from Gemini, length: {len(faq_md)}")
        print(f"[generate_faq] Response preview: {faq_md[:200]}...")
        
    except Exception as e:
        print(f"[generate_faq] ERROR calling Gemini API: {e}")
        raise e

    # Prepend the original page title and URL to the saved FAQ file for reliable source mapping
    header_lines = f"# {title}\n\n"
    if page_url:
        header_lines += f"**URL:** {page_url}\n\n"
    faq_output = header_lines + faq_md
    
    faq_dir = os.path.join("storage", "datasets", "faqs")
    os.makedirs(faq_dir, exist_ok=True)
    base_name = os.path.basename(md_path).replace(".md", "_faq.md")
    faq_path = os.path.join(faq_dir, base_name)
    
    print(f"[generate_faq] Saving FAQ to: {faq_path}")
    with open(faq_path, "w", encoding="utf-8") as f:
        f.write(faq_output)
    
    print(f"[generate_faq] FAQ generation completed successfully")
    return faq_path

async def crawl_and_generate_faq(url: str, skip_faq: bool = False, target_language: str = None) -> Dict[str, str]:
    """Crawl a single URL and generate FAQ for it using advanced change detection"""
    try:
        # Upfront URL filtering
        parsed_for_filter = urlparse(url)
        base_netloc = parsed_for_filter.netloc
        if is_media(url) or is_blocked(url, base_netloc):
            print(f"Skip filtered URL: {url}")
            existing_faq = find_faq_file_for_url(url)
            # Return quickly without crawling
            return {
                "url": url,
                "last_updated": None,
                "timestamp_source": None,
                "md_path": None,
                "faq_path": existing_faq,
                "identifier": None,
                "content_hash": None,
                "structured_hash": None,
            }
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
            page = await browser.new_page()
            
            # Block non-essential resources
            async def route_handler(route):
                req = route.request
                u = req.url
                rtype = req.resource_type
                if rtype in ("image", "media", "font", "stylesheet") or is_media(u) or is_blocked(u, base_netloc):
                    await route.abort()
                    return
                if rtype in ("xhr", "fetch") and not same_domain(u, url):
                    await route.abort()
                    return
                await route.continue_()
            try:
                await page.route("**/*", route_handler)
            except Exception:
                pass
            
            # Load stored data for lightweight check
            change_detection_file = os.path.join("storage", "change_detection.json")
            try:
                with open(change_detection_file, 'r') as f:
                    stored_all = json.load(f)
                    stored_data = stored_all.get(url, {}) if isinstance(stored_all, dict) else {}
            except Exception:
                stored_data = {}

            # If existing FAQ and headers unchanged, skip full crawl
            existing_faq = find_faq_file_for_url(url)
            if existing_faq and isinstance(stored_data, dict) and (stored_data.get("last_modified_header") or stored_data.get("etag_header")):
                try:
                    lw = await change_detector.check_page_changes_lightweight(page, url, stored_data)
                    if not lw.get("needs_deep_check", True):
                        print(f"Headers unchanged for {url}, using existing FAQ")
                        await browser.close()
                        return {
                            "url": url,
                            "last_updated": stored_data.get("last_updated"),
                            "timestamp_source": stored_data.get("timestamp_source"),
                            "md_path": None,
                            "faq_path": existing_faq,
                            "identifier": stored_data.get("identifier"),
                            "content_hash": stored_data.get("content_hash"),
                            "structured_hash": stored_data.get("structured_hash"),
                        }
                except Exception:
                    pass
            
            # Navigate to the page (faster)
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)
            
            # Use efficient change detection
            analysis = await change_detector.analyze_page_efficient(page, url, stored_data if isinstance(stored_data, dict) else None)
            
            # Detect language from page content
            content = await page.content()
            language_result = language_detector.detect_language(content, url)
            print(f"Language detected: {language_result.detected_lang} (confidence: {language_result.confidence:.2f}, source: {language_result.source})")
            
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
            
            # Generate FAQ only if not skipped
            faq_path = None
            if not skip_faq:
                try:
                    faq_path = generate_faq_from_markdown(md_path, language_result.detected_lang, language_result.confidence, target_language, script_hint=language_result.script_hint)
                except Exception as e:
                    print(f"FAQ generation failed: {e}")
                    # Continue without FAQ generation
            
            # Update change detection data with enhanced information
            os.makedirs(os.path.dirname(change_detection_file), exist_ok=True)
            
            try:
                with open(change_detection_file, 'r') as f:
                    change_detection_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                change_detection_data = {}
            
            # Store enhanced change detection data
            change_detection_data[url] = {
                "identifier": analysis["identifier"],
                "last_updated": analysis["last_updated"],
                "timestamp_source": analysis["timestamp_source"],
                "content_hash": analysis["content_hash"],
                "structured_hash": analysis["structured_hash"],
                "last_modified_header": analysis.get("last_modified_header"),
                "etag_header": analysis.get("etag_header"),
                "crawl_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "detected_language": language_result.detected_lang,
                "language_confidence": language_result.confidence,
                "language_source": language_result.source,
                "is_rtl": language_result.is_rtl,
                "script_hint": language_result.script_hint,
            }
            
            with open(change_detection_file, 'w') as f:
                json.dump(change_detection_data, f, indent=2)
            
            await browser.close()
            
            return {
                "url": url,
                "last_updated": analysis["last_updated"],
                "timestamp_source": analysis["timestamp_source"],
                "md_path": md_path,
                "faq_path": faq_path,
                "identifier": analysis["identifier"],
                "content_hash": analysis["content_hash"],
                "structured_hash": analysis["structured_hash"],
            }
            
    except Exception as e:
        raise Exception(f"Failed to crawl {url}: {str(e)}")

async def crawl_entire_website(base_url: str, max_pages: int = 50, target_language: str = None) -> List[Dict[str, str]]:
    """Crawl an entire website starting from the base URL"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
            context = await browser.new_context()
            page = await context.new_page()
            
            # Block non-essential resources
            base_netloc = urlparse(base_url).netloc
            async def route_handler(route):
                req = route.request
                u = req.url
                rtype = req.resource_type
                if rtype in ("image", "media", "font", "stylesheet") or is_media(u) or is_blocked(u, base_netloc):
                    await route.abort()
                    return
                if rtype in ("xhr", "fetch") and not same_domain(u, base_url):
                    await route.abort()
                    return
                await route.continue_()
            try:
                await page.route("**/*", route_handler)
            except Exception:
                pass
            
            # Set up storage for change detection
            change_detection_file = os.path.join("storage", "change_detection.json")
            os.makedirs(os.path.dirname(change_detection_file), exist_ok=True)
            
            try:
                with open(change_detection_file, 'r') as f:
                    change_detection_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                change_detection_data = {}
            
            crawled_urls = []
            urls_to_crawl: List[tuple[str, int]] = [(base_url, 0)]
            seen_normalized = set([strip_query(base_url)])
            crawled_count = 0
            
            while urls_to_crawl and crawled_count < max_pages:
                current_url, depth = urls_to_crawl.pop(0)
                
                # Upfront URL filtering
                if is_media(current_url) or is_blocked(current_url, base_netloc):
                    print(f"Skip filtered URL: {current_url}")
                    continue
                
                # Skip if already crawled and unchanged (lightweight)
                try:
                    old_data = change_detection_data.get(current_url) if isinstance(change_detection_data, dict) else None
                    existing_faq = find_faq_file_for_url(current_url)
                    if existing_faq and isinstance(old_data, dict) and (old_data.get("last_modified_header") or old_data.get("etag_header")):
                        lw = await change_detector.check_page_changes_lightweight(page, current_url, old_data)
                        if not lw.get("needs_deep_check", True):
                            # Unchanged: use existing FAQ and skip deep crawl
                            crawled_urls.append({
                                "url": current_url,
                                "last_updated": old_data.get("last_updated"),
                                "timestamp_source": old_data.get("timestamp_source"),
                                "md_path": None,
                                "faq_path": existing_faq,
                                "identifier": old_data.get("identifier"),
                                "content_hash": old_data.get("content_hash"),
                                "structured_hash": old_data.get("structured_hash"),
                            })
                            crawled_count += 1
                            # Do not enqueue links from this page (no navigation)
                            continue
                except Exception:
                    pass
                
                # Skip if already crawled (no FAQ or changed detection will proceed)
                if current_url in change_detection_data:
                    # We still may need to generate missing FAQ; handle via backfill later
                    pass
                
                try:
                    # Navigate to the page (faster)
                    await page.goto(current_url, wait_until="domcontentloaded", timeout=10000)
                    
                    # Use efficient change detection
                    old_data = change_detection_data.get(current_url) if isinstance(change_detection_data, dict) else None
                    analysis = await change_detector.analyze_page_efficient(page, current_url, old_data if isinstance(old_data, dict) else None)
                    
                    # Detect language from page content
                    content = await page.content()
                    language_result = language_detector.detect_language(content, current_url)
                    print(f"Language detected for {current_url}: {language_result.detected_lang} (confidence: {language_result.confidence:.2f}, source: {language_result.source})")
                    
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
                    faq_path = generate_faq_from_markdown(md_path, language_result.detected_lang, language_result.confidence, target_language, script_hint=language_result.script_hint)
                    
                    # Update change detection data with enhanced information
                    change_detection_data[current_url] = {
                        "identifier": analysis["identifier"],
                        "last_updated": analysis["last_updated"],
                        "timestamp_source": analysis["timestamp_source"],
                        "content_hash": analysis["content_hash"],
                        "structured_hash": analysis["structured_hash"],
                        "last_modified_header": analysis.get("last_modified_header"),
                        "etag_header": analysis.get("etag_header"),
                        "crawl_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "detected_language": language_result.detected_lang,
                        "language_confidence": language_result.confidence,
                        "language_source": language_result.source,
                        "is_rtl": language_result.is_rtl,
                        "script_hint": language_result.script_hint,
                    }
                    
                    crawled_urls.append({
                        "url": current_url,
                        "last_updated": analysis["last_updated"],
                        "timestamp_source": analysis["timestamp_source"],
                        "md_path": md_path,
                        "faq_path": faq_path,
                        "identifier": analysis["identifier"],
                        "content_hash": analysis["content_hash"],
                        "structured_hash": analysis["structured_hash"],
                    })
                    
                    crawled_count += 1
                    
                    # Find links to crawl (same domain only), depth cap 2, dedupe, strip query, cap total
                    if depth < 2 and crawled_count < max_pages:
                        links = await page.query_selector_all("a[href]")
                        base_domain = urlparse(base_url).netloc
                        for link in links:
                            href = await link.get_attribute("href")
                            if not href:
                                continue
                            if href.startswith('#') or href.startswith('mailto:') or href.startswith('tel:'):
                                continue
                            if href.startswith('/'):
                                full_url = f"{urlparse(base_url).scheme}://{base_domain}{href}"
                            elif href.startswith('http'):
                                full_url = href
                            else:
                                # Resolve relative URLs
                                full_url = urljoin(current_url, href)
                            # Only crawl same-domain http(s)
                            parsed = urlparse(full_url)
                            if parsed.scheme not in ("http", "https"):
                                print(f"Skip non-http(s): {full_url}")
                                continue
                            if not same_domain(full_url, base_url):
                                print(f"Skip off-domain: {full_url}")
                                continue
                            if is_media(full_url):
                                print(f"Skip media: {full_url}")
                                continue
                            if is_blocked(full_url, base_domain):
                                print(f"Skip blocked: {full_url}")
                                continue
                            normalized = strip_query(full_url)
                            if normalized in seen_normalized or normalized in change_detection_data:
                                continue
                            seen_normalized.add(normalized)
                            if len(seen_normalized) <= max_pages:
                                urls_to_crawl.append((normalized, depth + 1))
                
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

def get_change_detection_data() -> Dict[str, Any]:
    """Load change detection data from crawler storage"""
    change_detection_file = os.path.join("storage", "change_detection.json")
    try:
        with open(change_detection_file, 'r') as f:
            data = json.load(f)
            
        # Migrate legacy data to new format
        migrated_data = {}
        for url, value in data.items():
            if isinstance(value, str):
                # Legacy format - migrate to new format
                migrated_data[url] = migrate_legacy_data(url, value)
            else:
                # Already in new format
                migrated_data[url] = value
        
        # Save migrated data if any migration occurred
        if migrated_data != data:
            with open(change_detection_file, 'w') as f:
                json.dump(migrated_data, f, indent=2)
        
        return migrated_data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def migrate_legacy_data(url: str, legacy_identifier: str) -> Dict[str, Any]:
    """Migrate legacy change detection data to new format"""
    # Parse legacy identifier
    parts = legacy_identifier.split("|")
    legacy_data = {}
    
    for part in parts:
        if ":" in part:
            key, value = part.split(":", 1)
            legacy_data[key] = value
    
    # Convert to new format
    return {
        "identifier": legacy_identifier,
        "last_updated": legacy_data.get("last_modified"),
        "timestamp_source": "http_header" if "last_modified" in legacy_data else "none",
        "content_hash": legacy_data.get("content_hash"),
        "structured_hash": None,  # Not available in legacy data
        "last_modified_header": legacy_data.get("last_modified"),
        "etag_header": legacy_data.get("etag"),
        "crawl_timestamp": None,  # Not available in legacy data
        "needs_recrawl": True,  # Flag to indicate this should be re-crawled
    }

def get_last_updated_from_identifier(identifier: str) -> Optional[str]:
    """Extract last_modified timestamp from change detection identifier (legacy support)"""
    if not identifier:
        return None
    
    # Handle new format (dictionary)
    if isinstance(identifier, dict):
        return identifier.get("last_updated")
    
    # Handle legacy format (string)
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
    print(f"[read_faq] Reading FAQ content from: {faq_path}")
    try:
        with open(faq_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"[read_faq] Read content, length: {len(content)}")
        print(f"[read_faq] Content preview: {content[:500]}...")
        
        # Simple parsing of markdown FAQ format
        faqs = []
        lines = content.split('\n')
        current_question = None
        current_answer = []
        
        for line in lines:
            line = line.strip()
            # Handle both formats: "**Question**" and "# **Question**"
            if (line.startswith('**') and line.endswith('**')) or \
               (line.startswith('# **') and line.endswith('**')):
                # Save previous FAQ if exists
                if current_question and current_answer:
                    faqs.append({
                        "question": current_question,
                        "answer": ' '.join(current_answer).strip()
                    })
                
                # Start new FAQ - strip # and ** from the question
                current_question = line.replace('# ', '').strip('*')
                current_answer = []
            elif line and current_question:
                current_answer.append(line)
        
        # Add the last FAQ
        if current_question and current_answer:
            faqs.append({
                "question": current_question,
                "answer": ' '.join(current_answer).strip()
            })
        
        print(f"[read_faq] Parsed {len(faqs)} FAQs")
        for i, faq in enumerate(faqs[:3]):  # Show first 3 FAQs
            print(f"[read_faq] FAQ {i+1}: {faq.get('question', '')[:50]}...")
        
        return faqs
    except Exception as e:
        print(f"[read_faq] ERROR reading FAQ file: {e}")
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

async def generate_missing_faqs_for_domain(base_url: str, target_language: str = None) -> int:
    """Generate missing FAQs for all crawled URLs in a domain"""
    change_data = get_change_detection_data()
    domain = urlparse(base_url).netloc
    crawled_urls = [url for url in change_data.keys() if urlparse(url).netloc == domain]
    
    generated_count = 0
    
    for url in crawled_urls:
        # Skip filtered URLs entirely
        if is_media(url) or is_blocked(url, domain):
            print(f"Skip filtered URL (backfill): {url}")
            continue
        
        # Check if FAQ exists for this URL
        faq_path = find_faq_file_for_url(url)
        
        if not faq_path:
            try:
                # Find the markdown file for this URL
                parsed_url = urlparse(url)
                path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
                base_name = '_'.join(filter(None, [
                    ''.join(c if c.isalnum() or c in '-_' else '_' for c in (path_parts[-1] if path_parts else 'index'))
                ])) or 'index'
                domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
                md_filename = f"{domain_prefix}_{base_name}.md"
                md_dir = os.path.join("storage", "datasets", "page_content")
                md_path = os.path.join(md_dir, md_filename[:255])
                
                if os.path.exists(md_path):
                    # Get language info from change detection data
                    url_data = change_data.get(url, {})
                    detected_lang = url_data.get("detected_language", "en")
                    confidence = url_data.get("language_confidence", 1.0)
                    script_hint = url_data.get("script_hint")
                    
                    generate_faq_from_markdown(md_path, detected_lang, confidence, target_language, script_hint=script_hint)
                    generated_count += 1
                else:
                    # No markdown file found, need to re-crawl this specific URL
                    await crawl_and_generate_faq(url, skip_faq=False, target_language=target_language)
                    generated_count += 1
            except Exception as e:
                print(f"Failed to generate FAQ for {url}: {e}")
                continue
    
    return generated_count

@app.get("/last-updated")
async def last_updated(
    url: str = Query(..., description="The URL to check for last updated time"),
    force_recrawl: bool = Query(False, description="Force re-crawl to use new change detection system")
):
    """
    Get the last updated time for a specific URL.
    
    If the URL hasn't been crawled yet, it will be crawled automatically.
    Returns the most accurate last updated timestamp from multiple sources:
    - Meta tags (highest priority)
    - Content extraction
    - HTTP headers (lowest priority)
    """
    change_data = get_change_detection_data()
    
    # Force re-crawl if requested or if URL not found
    if force_recrawl or url not in change_data:
        # URL not found or force recrawl requested, crawl it automatically
        try:
            crawl_result = await crawl_and_generate_faq(url, skip_faq=True)
            last_updated_time = crawl_result.get("last_updated")
            timestamp_source = crawl_result.get("timestamp_source")
            return {
                "url": url, 
                "last_updated": last_updated_time,
                "timestamp_source": timestamp_source,
                "has_been_crawled": True,
                "just_crawled": True,
                "timestamp_reliability": "high" if last_updated_time else "unknown"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to crawl URL: {str(e)}")
    
    # URL exists and no force recrawl requested
    url_data = change_data[url]
    
    # Handle both new and legacy data formats
    if isinstance(url_data, dict):
        last_updated_time = url_data.get("last_updated")
        timestamp_source = url_data.get("timestamp_source")
        crawl_timestamp = url_data.get("crawl_timestamp")
    else:
        # Legacy format
        last_updated_time = get_last_updated_from_identifier(url_data)
        timestamp_source = "legacy"
        crawl_timestamp = None
    
    return {
        "url": url, 
        "last_updated": last_updated_time,
        "timestamp_source": timestamp_source,
        "crawl_timestamp": crawl_timestamp,
        "has_been_crawled": True,
        "just_crawled": False,
        "timestamp_reliability": "high" if last_updated_time else "unknown"
    }

@app.get("/page-faqs")
async def page_faqs(
    url: str = Query(..., description="The URL to get FAQs for"),
    target_language: str = Query(None, description="Optional target language for FAQ generation (ISO code, e.g., 'es', 'fr')"),
    force_refresh: bool = Query(False, description="Force refresh of FAQ content")
):
    """
    Get the last updated time and FAQs for a specific page.
    
    If the URL hasn't been crawled yet, it will be crawled automatically and FAQs will be generated.
    If the URL has been crawled but no FAQ exists, it will generate the FAQ on the spot.
    Returns both the last updated timestamp and the generated FAQs for the page.
    """
    print(f"[page-faqs] Processing URL: {url}, force_refresh: {force_refresh}")
    change_data = get_change_detection_data()
    just_crawled = False
    faq_generated = False
    
    if url not in change_data:
        print(f"[page-faqs] URL not found in change data, crawling...")
        # URL not found, crawl it automatically
        try:
            crawl_result = await crawl_and_generate_faq(url, skip_faq=False, target_language=target_language)  # Generate FAQ
            last_updated_time = crawl_result.get("last_updated")
            timestamp_source = crawl_result.get("timestamp_source")
            faq_path = crawl_result.get("faq_path")
            just_crawled = True
            faq_generated = True
            print(f"[page-faqs] Crawl completed. FAQ path: {faq_path}")
        except Exception as e:
            print(f"[page-faqs] Crawl failed: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to crawl URL: {str(e)}")
    else:
        print(f"[page-faqs] URL found in change data")
        url_data = change_data[url]
        
        # Handle both new and legacy data formats
        if isinstance(url_data, dict):
            last_updated_time = url_data.get("last_updated")
            timestamp_source = url_data.get("timestamp_source")
        else:
            # Legacy format
            last_updated_time = get_last_updated_from_identifier(url_data)
            timestamp_source = "legacy"
        
        faq_path = find_faq_file_for_url(url)
        print(f"[page-faqs] Found FAQ path: {faq_path}")
        
        # If FAQ doesn't exist but we have page data, generate it on the spot
        if not faq_path:
            print(f"[page-faqs] No FAQ found, attempting to generate...")
            try:
                # Find the markdown file for this URL
                parsed_url = urlparse(url)
                path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
                base_name = '_'.join(filter(None, [
                    ''.join(c if c.isalnum() or c in '-_' else '_' for c in (path_parts[-1] if path_parts else 'index'))
                ])) or 'index'
                domain_prefix = parsed_url.netloc.replace('www.', '').split('.')[0]
                md_filename = f"{domain_prefix}_{base_name}.md"
                md_dir = os.path.join("storage", "datasets", "page_content")
                md_path = os.path.join(md_dir, md_filename[:255])
                print(f"[page-faqs] Looking for markdown file: {md_path}")
                
                if os.path.exists(md_path):
                    print(f"[page-faqs] Markdown file found, generating FAQ...")
                    # Get language info from change detection data
                    detected_lang = url_data.get("detected_language", "en")
                    confidence = url_data.get("language_confidence", 1.0)
                    script_hint = url_data.get("script_hint")
                    
                    faq_path = generate_faq_from_markdown(md_path, detected_lang, confidence, target_language, script_hint=script_hint)
                    faq_generated = True
                    print(f"[page-faqs] FAQ generated: {faq_path}")
                else:
                    print(f"[page-faqs] No markdown file found, re-crawling...")
                    # No markdown file found, need to re-crawl
                    crawl_result = await crawl_and_generate_faq(url, skip_faq=False, target_language=target_language)
                    last_updated_time = crawl_result.get("last_updated")
                    timestamp_source = crawl_result.get("timestamp_source")
                    faq_path = crawl_result.get("faq_path")
                    just_crawled = True
                    faq_generated = True
                    print(f"[page-faqs] Re-crawl completed. FAQ path: {faq_path}")
            except Exception as e:
                print(f"[page-faqs] FAQ generation failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to generate FAQ: {str(e)}")
    
    print(f"[page-faqs] Final FAQ path: {faq_path}")
    if not faq_path:
        print(f"[page-faqs] No FAQ path available, returning empty result")
        return {
            "url": url,
            "last_updated": last_updated_time,
            "timestamp_source": timestamp_source,
            "faqs": [],
            "message": "No FAQ file found for this URL and could not generate one",
            "just_crawled": just_crawled,
            "faq_generated": faq_generated
        }
    
    print(f"[page-faqs] Reading FAQ content from: {faq_path}")
    faqs = read_faq_content(faq_path)
    print(f"[page-faqs] Read {len(faqs)} FAQs")
    
    # If force_refresh is true and we got empty FAQs, try to re-read the file
    if force_refresh and len(faqs) == 0:
        print(f"[page-faqs] Force refresh requested and no FAQs found, trying to re-read file...")
        # Simply re-read the file without module reloading
        faqs = read_faq_content(faq_path)
        print(f"[page-faqs] After force refresh, read {len(faqs)} FAQs")
    
    return {
        "url": url,
        "last_updated": last_updated_time,
        "timestamp_source": timestamp_source,
        "faqs": faqs,
        "faq_file": faq_path,
        "just_crawled": just_crawled,
        "faq_generated": faq_generated,
        "timestamp_reliability": "high" if last_updated_time else "unknown"
    }



@app.get("/site-faqs")
async def site_faqs(
    base_url: str = Query(..., description="The base URL to get all FAQs for"),
    target_language: str = Query(None, description="Optional target language for FAQ generation (ISO code, e.g., 'es', 'fr')"),
    max_pages: int = Query(50, description="Maximum number of pages to crawl for this domain"),
    force_recrawl: bool = Query(False, description="Force re-crawl before generating FAQs")
):
    """
    Generate and return complete FAQs for an entire website in a single call.

    - If force_recrawl is true or the domain has no entries, perform a full crawl first.
    - Always backfill any missing FAQ files for already-crawled pages.
    - Return the combined set of FAQs for the domain.
    """
    start_time = time.perf_counter()
    domain_netloc = urlparse(base_url).netloc

    # Load current change detection data
    change_data = get_change_detection_data()
    domain_urls = [u for u in change_data.keys() if urlparse(u).netloc == domain_netloc]

    crawled_pages_count = 0

    # Crawl if requested or if domain is unseen
    if force_recrawl or not domain_urls:
        try:
            crawl_results = await crawl_entire_website(base_url, max_pages=max_pages, target_language=target_language)
            crawled_pages_count = len(crawl_results)
            # Refresh after crawl
            change_data = get_change_detection_data()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to crawl website: {str(e)}")

    # Always backfill any missing FAQs for already-crawled pages
    try:
        backfilled_count = await generate_missing_faqs_for_domain(base_url, target_language=target_language)
    except Exception as e:
        print(f"Warning: Failed to generate missing FAQs: {e}")
        backfilled_count = 0

    # Gather all FAQs for the domain
    all_faqs = get_all_faqs_for_domain(base_url)

    # Total pages known for this domain after any crawl backfill
    change_data = get_change_detection_data()
    total_pages = len([u for u in change_data.keys() if urlparse(u).netloc == domain_netloc])

    elapsed_s = time.perf_counter() - start_time
    print(f"[site-faqs] domain={domain_netloc} crawled_pages={crawled_pages_count} backfilled_faqs={backfilled_count} elapsed={elapsed_s:.2f}s")

    return {
        "domain": domain_netloc,
        "generated_now": backfilled_count,
        "total_pages": total_pages,
        "faqs": all_faqs,
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
# Test reload
