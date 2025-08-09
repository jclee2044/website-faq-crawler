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

import dotenv

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from apify import Actor
from markdownify import markdownify as md
from google import genai

dotenv.load_dotenv()

executor = ThreadPoolExecutor()

def generate_faq_from_markdown(md_path: str, model_name: str = "gemini-1.5-flash") -> str:
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

async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls: List[str] = [
            url.get("url") for url in actor_input.get("start_urls", [{"url": "https://www.inhotel.io/"}])
        ]

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

            try:
                response = await page.context.request.get(url)
                headers = response.headers if response else {}
            except Exception as e:
                Actor.log.warning(f"Failed to fetch headers for {url}: {e}")
                headers = {}

            last_modified = headers.get("last-modified")
            etag = headers.get("etag")

            content = await page.content()
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

            identifier_parts = []
            if last_modified:
                identifier_parts.append(f"last_modified:{last_modified}")
            if etag:
                identifier_parts.append(f"etag:{etag}")
            identifier_parts.append(f"content_hash:{content_hash}")
            identifier = "|".join(identifier_parts)

            stored_identifier = change_detection_data.get(url)

            if stored_identifier == identifier:
                Actor.log.info(f"No changes for {url}, skipping.")
                skipped_count += 1
                await context.enqueue_links()
                return

            processed_count += 1
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
                    executor, generate_faq_from_markdown, md_path
                )
                Actor.log.info(f"FAQ saved to {faq_path}")
            except Exception as e:
                Actor.log.warning(f"FAQ generation failed for {md_path}: {e}")

            change_detection_data[url] = identifier
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
