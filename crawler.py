from __future__ import annotations

# Prevent purging of storage between runs to maintain change detection state
import os
os.environ['CRAWLEE_PURGE_ON_START'] = '0'
os.environ['PURGE_ON_START'] = '0'

import asyncio
import hashlib
import json
from typing import List
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import dotenv

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from apify import Actor
from markdownify import markdownify as md
from google import genai
from change_detection import change_detector
from language_detection import language_detector

dotenv.load_dotenv()

executor = ThreadPoolExecutor()

def generate_faq_from_markdown(md_path: str, detected_language: str = "en", confidence: float = 1.0, target_language: str = None, model_name: str = "gemini-1.5-flash") -> str:
    api_key = os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_GENERATIVE_AI_API_KEY not found in environment variables.")
    client = genai.Client(api_key=api_key)
    with open(md_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()
    
    # Determine the language to use for FAQ generation
    if target_language:
        # Use explicit target language
        final_language = target_language
        language_instruction = f"LANGUAGE REQUIREMENT: Generate FAQs in {target_language.upper()} language. Both questions and answers must be in {target_language.upper()}."
    else:
        # Use detected language
        final_language = detected_language
        language_instruction = language_detector.create_language_directive(detected_language, confidence)
    
    prompt = (
        f"""
        You are an expert at summarizing website content and generating helpful FAQs for users.\n
        {language_instruction}\n
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

async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls: List[str] = [
            url.get("url") for url in actor_input.get("start_urls", [{"url": "https://www.inhotel.io/"}])
        ]
        
        # Get optional target language for FAQ generation
        target_language = actor_input.get("target_language")
        if target_language:
            Actor.log.info(f"Target language for FAQ generation: {target_language}")

        if not start_urls:
            Actor.log.info("No start URLs provided, exiting.")
            await Actor.exit()

        parsed_url = urlparse(start_urls[0])
        base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

        change_detection_file = os.path.join("storage", "change_detection.json")
        os.makedirs(os.path.dirname(change_detection_file), exist_ok=True)
        try:
            with open(change_detection_file, 'r') as f:
                change_detection_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            change_detection_data = {}

        Actor.log.info(f"Loaded change detection data for {len(change_detection_data)} URLs")
        processed_count = 0
        skipped_count = 0

        crawler = PlaywrightCrawler(
            max_requests_per_crawl=5000,
            headless=True,
            browser_launch_options={"args": ["--disable-gpu"]},
        )

        @crawler.router.default_handler
        async def handle_request(context: PlaywrightCrawlingContext) -> None:
            nonlocal processed_count, skipped_count
            url = context.request.url
            page = context.page
            Actor.log.info(f"Visiting {url}")

            # Check if we have stored data for conditional requests
            stored_data = change_detection_data.get(url)
            
            if stored_data and isinstance(stored_data, dict):
                # Try conditional request first
                last_modified = stored_data.get("last_modified_header")
                etag = stored_data.get("etag_header")
                
                if last_modified or etag:
                    try:
                        analysis = await change_detector.make_conditional_request(
                            page, url, last_modified, etag
                        )
                        
                        if analysis.get("is_not_modified"):
                            Actor.log.info(f"Content not modified for {url} (304 response), skipping.")
                            skipped_count += 1
                            await context.enqueue_links()
                            return
                    except Exception as e:
                        Actor.log.warning(f"Conditional request failed for {url}: {e}")
                        # Fall back to full analysis
            
            # Use advanced change detection to analyze the page
            analysis = await change_detector.analyze_page_content(page, url)
            
            # Detect language from page content
            content = await page.content()
            language_result = language_detector.detect_language(content, url)
            Actor.log.info(f"Language detected: {language_result.detected_lang} (confidence: {language_result.confidence:.2f}, source: {language_result.source})")
            
            # Check if page should be re-crawled using intelligent heuristics
            if stored_data and isinstance(stored_data, dict):
                if not change_detector.should_recrawl_page(url, stored_data, analysis):
                    Actor.log.info(f"No significant changes for {url}, skipping.")
                    skipped_count += 1
                    await context.enqueue_links()
                    return
            elif stored_data:  # Legacy format
                stored_identifier = stored_data
                if not change_detector.has_content_changed(stored_identifier, analysis["identifier"]):
                    Actor.log.info(f"No changes for {url}, skipping.")
                    skipped_count += 1
                    await context.enqueue_links()
                    return

            processed_count += 1
            content = await page.content()
            markdown_content = md(content)

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

            try:
                faq_path = await asyncio.get_event_loop().run_in_executor(
                    executor, generate_faq_from_markdown, md_path, language_result.detected_lang, language_result.confidence, target_language
                )
                Actor.log.info(f"FAQ saved to {faq_path}")
            except Exception as e:
                Actor.log.warning(f"FAQ generation failed for {md_path}: {e}")

            # Store enhanced change detection data
            change_detection_data[url] = {
                "identifier": analysis["identifier"],
                "last_updated": analysis["last_updated"],
                "timestamp_source": analysis["timestamp_source"],
                "content_hash": analysis["content_hash"],
                "structured_hash": analysis["structured_hash"],
                "last_modified_header": analysis["last_modified_header"],
                "etag_header": analysis["etag_header"],
                "crawl_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "detected_language": language_result.detected_lang,
                "language_confidence": language_result.confidence,
                "language_source": language_result.source,
                "is_rtl": language_result.is_rtl,
            }
            
            with open(change_detection_file, 'w') as f:
                json.dump(change_detection_data, f, indent=2)

            await context.enqueue_links()

        await crawler.run(start_urls)

        try:
            with open(change_detection_file, 'w') as f:
                json.dump(change_detection_data, f, indent=2)
            Actor.log.info(f"Crawl finished: {processed_count} processed, {skipped_count} skipped.")
        except Exception as e:
            Actor.log.warning(f"Could not save final change detection file: {e}")

if __name__ == "__main__":
    asyncio.run(main())

def page_data_to_markdown(page_data: dict) -> str:
    md = []
    md.append(f"# {page_data.get('title', '')}\n")
    md.append(f"**URL:** [{page_data.get('url', '')}]({page_data.get('url', '')})\n")
    for h1 in page_data.get('h1s', []):
        if h1: md.append(f"# {h1}\n")
    for h2 in page_data.get('h2s', []):
        if h2: md.append(f"## {h2}\n")
    for h3 in page_data.get('h3s', []):
        if h3: md.append(f"### {h3}\n")
    for p in page_data.get('paragraphs', []):
        if p: md.append(f"{p}\n")
    if page_data.get('links'):
        md.append("\n**Links:**\n")
        for link in page_data['links']:
            if link: md.append(f"- [{link}]({link})\n")
    md.append("\n---\n")
    return ''.join(md)
