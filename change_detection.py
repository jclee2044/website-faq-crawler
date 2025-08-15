import re
import hashlib
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
import asyncio
from playwright.async_api import Page
import logging
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class AdvancedChangeDetector:
    """Advanced website change detection with dynamic content filtering"""
    
    def __init__(self):
        # History tracking for de-bouncing flapping pages
        self.page_history = {}  # URL -> list of recent timestamps/hashes
        self.max_history_size = 5  # Keep last 5 entries per page
        
        # Site-specific recrawl frequencies (hours)
        self.site_recrawl_frequencies = {
            # News sites - check frequently
            "news.example.com": 2,
            "blog.example.com": 4,
            # Static sites - check less frequently
            "docs.example.com": 24,
            "help.example.com": 48,
            # Default frequency for unknown sites
            "default": 12
        }
        # Patterns to remove dynamic content
        self.dynamic_patterns = [
            # Session IDs, tokens, CSRF tokens
            r'[?&](?:session|token|csrf|auth|key|id)=[a-zA-Z0-9_-]+',
            r'[?&]captcha=[a-zA-Z0-9_-]+',
            r'[?&]t=\d+',
            r'[?&]v=\d+',
            r'[?&]cache=\d+',
            r'[?&]timestamp=\d+',
            
            # Common dynamic attributes
            r'data-[a-zA-Z-]+="[^"]*"',
            r'id="[a-zA-Z0-9_-]*dynamic[a-zA-Z0-9_-]*"',
            r'class="[^"]*dynamic[^"]*"',
            
            # Timestamps and dates in content
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{4}-\d{2}-\d{2}\b',      # YYYY-MM-DD
            r'\b\d{1,2}:\d{2}:\d{2}\b',    # HH:MM:SS
            r'\b\d{1,2}:\d{2}\s*(?:AM|PM)\b',  # HH:MM AM/PM
            
            # Common dynamic text patterns
            r'Last updated:.*?(?=\n|$)',
            r'Updated:.*?(?=\n|$)',
            r'Modified:.*?(?=\n|$)',
            r'Published:.*?(?=\n|$)',
            r'Created:.*?(?=\n|$)',
            
            # Social media counters, view counts
            r'\d+\s*(?:views?|likes?|shares?|comments?)',
            r'[0-9,]+ views',
            r'[0-9,]+ likes',
            
            # Ad content patterns
            r'Advertisement',
            r'Sponsored',
            r'Ad by',
            r'Promoted',
            
            # Analytics and tracking
            r'google-analytics\.com',
            r'googletagmanager\.com',
            r'facebook\.net',
            r'connect\.facebook\.net',
            r'twitter\.com/widgets',
            r'platform\.twitter\.com',
        ]
        
        # Compile patterns for efficiency
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.dynamic_patterns]
        
        # Meta tags that might contain last updated info
        self.last_updated_meta_patterns = [
            r'<meta[^>]*name=["\'](?:last-modified|lastmod|modified|updated|date)["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*property=["\'](?:article:modified_time|og:updated_time)["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*http-equiv=["\']last-modified["\'][^>]*content=["\']([^"\']+)["\']',
        ]
        
        self.compiled_meta_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.last_updated_meta_patterns]
    
    def clean_content(self, content: str) -> str:
        """Remove dynamic content from HTML to get stable content for comparison"""
        cleaned = content
        
        # Remove dynamic patterns
        for pattern in self.compiled_patterns:
            cleaned = pattern.sub('', cleaned)
        
        # Remove script and style tags completely
        cleaned = re.sub(r'<script[^>]*>.*?</script>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove comments
        cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()
        
        return cleaned
    
    def extract_last_updated_from_meta(self, content: str) -> Optional[str]:
        """Extract last updated timestamp from meta tags"""
        for pattern in self.compiled_meta_patterns:
            match = pattern.search(content)
            if match:
                timestamp = match.group(1)
                # Try to parse the timestamp
                try:
                    # Common timestamp formats
                    formats = [
                        '%Y-%m-%dT%H:%M:%S%z',  # ISO format with timezone
                        '%Y-%m-%dT%H:%M:%SZ',   # ISO format UTC
                        '%Y-%m-%dT%H:%M:%S',    # ISO format without timezone
                        '%Y-%m-%d %H:%M:%S',    # MySQL format
                        '%a, %d %b %Y %H:%M:%S %Z',  # RFC format
                        '%a, %d %b %Y %H:%M:%S GMT', # RFC format GMT
                        '%Y-%m-%d',             # Date only
                    ]
                    
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(timestamp, fmt)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            
                            # Validate the timestamp is reasonable
                            if self.is_reasonable_timestamp(dt.isoformat()):
                                return dt.isoformat()
                        except ValueError:
                            continue
                except Exception as e:
                    logger.warning(f"Failed to parse timestamp {timestamp}: {e}")
                    continue
        
        return None
    
    def extract_last_updated_from_content(self, content: str) -> Optional[str]:
        """Extract last updated timestamp from page content using various strategies"""
        # Look for common patterns in the content
        patterns = [
            # Structured data (JSON-LD) - most reliable
            r'"dateModified":\s*"([^"]+)"',
            r'"lastModified":\s*"([^"]+)"',
            r'"updated":\s*"([^"]+)"',
            r'"modified":\s*"([^"]+)"',
            
            # Common text patterns - more specific to avoid false positives
            r'Last updated:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Updated:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Modified:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Last modified:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Last changed:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Revision date:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            r'Update date:\s*([^\n\r<]+?)(?:\s*ago|\s*\([^)]*\))?',
            
            # More specific patterns
            r'<time[^>]*datetime=["\']([^"\']+)["\'][^>]*>',
            r'<span[^>]*class=["\'][^"\']*date[^"\']*["\'][^>]*>([^<]+)</span>',
            r'<div[^>]*class=["\'][^"\']*date[^"\']*["\'][^>]*>([^<]+)</div>',
            
            # WordPress and CMS patterns
            r'<meta[^>]*property=["\']article:modified_time["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*name=["\']modified_date["\'][^>]*content=["\']([^"\']+)["\']',
            
            # Avoid birth dates and other irrelevant dates - only look for recent patterns
            # Skip general date patterns that could be birth dates, creation dates, etc.
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                timestamp = match.group(1)
                # Try to parse the timestamp
                try:
                    # Common timestamp formats
                    formats = [
                        '%Y-%m-%dT%H:%M:%S%z',
                        '%Y-%m-%dT%H:%M:%SZ',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S',
                        '%a, %d %b %Y %H:%M:%S %Z',
                        '%a, %d %b %Y %H:%M:%S GMT',
                        '%Y-%m-%d',
                        '%m/%d/%Y',
                        '%B %d, %Y',
                        '%B %d %Y',
                    ]
                    
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(timestamp, fmt)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            
                            # Validate the timestamp is reasonable
                            if self.is_reasonable_timestamp(dt.isoformat()):
                                return dt.isoformat()
                        except ValueError:
                            continue
                except Exception as e:
                    logger.warning(f"Failed to parse content timestamp {timestamp}: {e}")
                    continue
        
        # Try to parse relative time expressions
        relative_timestamp = self.parse_relative_time(content)
        if relative_timestamp:
            return relative_timestamp
        
        return None
    
    def _parse_timestamp_for_comparison(self, timestamp: str) -> Optional[datetime]:
        """Parse timestamp for internal comparisons - handles both ISO and new UTC format"""
        try:
            from datetime import datetime, timezone
            
            if not timestamp:
                return None
                
            # Handle new UTC format: "YYYY-MM-DD HH:MM:SS UTC"
            if ' UTC' in timestamp:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
                return dt.replace(tzinfo=timezone.utc)
            
            # Handle ISO format with timezone
            if 'T' in timestamp and ('Z' in timestamp or '+' in timestamp or '-' in timestamp):
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            # Handle ISO format without timezone
            if 'T' in timestamp:
                dt = datetime.fromisoformat(timestamp)
                return dt.replace(tzinfo=timezone.utc)
            
            # Handle other formats
            formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%B %d, %Y',
                '%B %d %Y',
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            
            return None
            
        except Exception:
            return None

    def is_reasonable_timestamp(self, timestamp: str) -> bool:
        """Validate if a timestamp is reasonable (not too old, not in the future)"""
        try:
            from datetime import datetime, timezone
            
            # Parse the timestamp using the helper function
            dt = self._parse_timestamp_for_comparison(timestamp)
            if dt is None:
                return False
            
            now = datetime.now(timezone.utc)
            
            # Check if timestamp is in the future (more than 1 day ahead)
            if dt > now + timedelta(days=1):
                return False
            
            # Check if timestamp is too old (before 1990 - before widespread internet)
            internet_era = datetime(1990, 1, 1, tzinfo=timezone.utc)
            if dt < internet_era:
                return False
            
            return True
            
        except Exception:
            return False

    def extract_structured_content(self, content: str) -> Dict[str, Any]:
        """Extract structured content focusing on semantically stable elements"""
        import re
        
        # Extract canonical content using readability-like approach
        canonical_content = self._extract_canonical_content(content)
        
        # Extract structured data (schema.org, JSON-LD, etc.)
        structured_data = self._extract_structured_data(content)
        
        # Extract stable DOM elements
        stable_elements = self._extract_stable_elements(content)
        
        # Extract listing/hub content for index pages
        listing_content = self._extract_listing_content(content)
        
        return {
            'canonical_content': canonical_content,
            'structured_data': structured_data,
            'stable_elements': stable_elements,
            'listing_content': listing_content,
        }
    
    def _extract_canonical_content(self, content: str) -> str:
        """Extract main content using readability-like approach"""
        import re
        
        # Remove known volatile content
        content = self._remove_volatile_content(content)
        
        # Try to find main content areas using common selectors
        main_selectors = [
            'main',
            'article',
            '.content',
            '.main-content',
            '#content',
            '#main',
            '.post-content',
            '.entry-content',
            '.article-content',
            '.story-content',
        ]
        
        # For now, extract text content from the body
        # In a full implementation, you'd use BeautifulSoup or similar
        text_content = re.sub(r'<[^>]+>', ' ', content)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        
        return text_content
    
    def _remove_volatile_content(self, content: str) -> str:
        """Remove known volatile content that shouldn't affect change detection"""
        import re
        
        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove known dynamic content patterns
        volatile_patterns = [
            # Ad content
            r'<div[^>]*class=["\'][^"\']*ad[^"\']*["\'][^>]*>.*?</div>',
            r'<div[^>]*id=["\'][^"\']*ad[^"\']*["\'][^>]*>.*?</div>',
            # Cookie banners
            r'<div[^>]*class=["\'][^"\']*cookie[^"\']*["\'][^>]*>.*?</div>',
            r'<div[^>]*id=["\'][^"\']*cookie[^"\']*["\'][^>]*>.*?</div>',
            # Consent banners
            r'<div[^>]*class=["\'][^"\']*consent[^"\']*["\'][^>]*>.*?</div>',
            # Live tickers
            r'<div[^>]*class=["\'][^"\']*ticker[^"\']*["\'][^>]*>.*?</div>',
            # Timestamp widgets
            r'<div[^>]*class=["\'][^"\']*timestamp[^"\']*["\'][^>]*>.*?</div>',
            # Social media widgets
            r'<div[^>]*class=["\'][^"\']*social[^"\']*["\'][^>]*>.*?</div>',
            # Analytics/tracking
            r'<div[^>]*class=["\'][^"\']*analytics[^"\']*["\'][^>]*>.*?</div>',
        ]
        
        for pattern in volatile_patterns:
            content = re.sub(pattern, '', content, flags=re.DOTALL | re.IGNORECASE)
        
        return content
    
    def _extract_structured_data(self, content: str) -> Dict[str, Any]:
        """Extract structured data (schema.org, JSON-LD, etc.)"""
        import re
        import json
        
        structured_data = {}
        
        # Extract JSON-LD
        json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        json_ld_matches = re.findall(json_ld_pattern, content, re.IGNORECASE | re.DOTALL)
        
        for match in json_ld_matches:
            try:
                data = json.loads(match)
                if isinstance(data, dict):
                    # Extract relevant fields
                    for key in ['dateModified', 'datePublished', 'lastModified', 'updated', 'modified']:
                        if key in data:
                            structured_data[f'json_ld_{key}'] = data[key]
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            for key in ['dateModified', 'datePublished', 'lastModified', 'updated', 'modified']:
                                if key in item:
                                    structured_data[f'json_ld_{key}'] = item[key]
            except json.JSONDecodeError:
                continue
        
        # Extract schema.org microdata
        schema_patterns = [
            (r'itemprop=["\']dateModified["\'][^>]*content=["\']([^"\']+)["\']', 'schema_dateModified'),
            (r'itemprop=["\']datePublished["\'][^>]*content=["\']([^"\']+)["\']', 'schema_datePublished'),
            (r'itemprop=["\']updated["\'][^>]*content=["\']([^"\']+)["\']', 'schema_updated'),
        ]
        
        for pattern, key in schema_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                structured_data[key] = match.group(1)
        
        return structured_data
    
    def _extract_stable_elements(self, content: str) -> Dict[str, Any]:
        """Extract stable DOM elements that are unlikely to change frequently"""
        import re
        
        stable_elements = {}
        
        # Extract title
        title_match = re.search(r'<title[^>]*>(.*?)</title>', content, re.IGNORECASE)
        if title_match:
            stable_elements['title'] = title_match.group(1).strip()
        
        # Extract headings (h1-h6)
        headings = re.findall(r'<h([1-6])[^>]*>(.*?)</h\1>', content, re.IGNORECASE)
        stable_elements['headings'] = [h[1].strip() for h in headings]
        
        # Extract canonical URL
        canonical_match = re.search(r'<link[^>]*rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if canonical_match:
            stable_elements['canonical_url'] = canonical_match.group(1)
        
        # Extract Open Graph URL
        og_url_match = re.search(r'<meta[^>]*property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if og_url_match:
            stable_elements['og_url'] = og_url_match.group(1)
        
        return stable_elements
    
    def _extract_listing_content(self, content: str) -> Dict[str, Any]:
        """Extract listing/hub content for index pages"""
        import re
        
        listing_content = {}
        
        # Extract article links (common in blog/news sites)
        article_links = re.findall(r'<a[^>]*href=["\']([^"\']*article[^"\']*)["\'][^>]*>(.*?)</a>', content, re.IGNORECASE)
        listing_content['article_links'] = [{"href": link[0], "text": link[1].strip()} for link in article_links]
        
        # Extract item IDs from common patterns
        item_ids = re.findall(r'data-id=["\']([^"\']+)["\']', content, re.IGNORECASE)
        listing_content['item_ids'] = item_ids
        
        # Extract pagination info
        pagination_match = re.search(r'page[^>]*>(\d+)</[^>]*>', content, re.IGNORECASE)
        if pagination_match:
            listing_content['page_number'] = pagination_match.group(1)
        
        return listing_content
    
    async def analyze_page_content(self, page: Page, url: str) -> Dict[str, Any]:
        """Analyze page content and extract change detection information (Phase 2 - Deep Check)"""
        # Get the full HTML content
        content = await page.content()
        
        # Clean content for stable comparison
        cleaned_content = self.clean_content(content)
        
        # Generate content hash
        content_hash = hashlib.sha256(cleaned_content.encode("utf-8")).hexdigest()
        
        # Generate fuzzy similarity hash for better change detection
        fuzzy_hash = self._generate_fuzzy_hash(cleaned_content)
        
        # Extract structured content with improved semantic analysis
        structured_content = self.extract_structured_content(content)
        
        # Extract last updated (async + source)
        last_updated, timestamp_source = await self.extract_last_updated_with_priority(page, content, url)

        # HTTP headers (non-conditional here)
        response = await page.context.request.get(url)
        headers = response.headers if response else {}
        last_modified_header = headers.get("last-modified")
        etag_header = headers.get("etag")
        is_not_modified = bool(response and response.status == 304)

        # If still no timestamp, try HTTP header, then page-date fallback
        if not last_updated and last_modified_header:
            norm = self._normalize_timestamp(last_modified_header)
            if norm and self.is_reasonable_timestamp(norm):
                last_updated = norm
                timestamp_source = "http_header"

        if not last_updated:
            page_date = self.find_most_recent_date_on_page(content)
            if page_date:
                last_updated = page_date
                timestamp_source = "page_date_extraction"
            elif not timestamp_source:
                timestamp_source = "none"
        
        # Create comprehensive identifier
        identifier_parts = []
        if last_modified_header:
            identifier_parts.append(f"last_modified_header:{last_modified_header}")
        if etag_header:
            identifier_parts.append(f"etag_header:{etag_header}")
        identifier_parts.append(f"content_hash:{content_hash}")
        
        # Add structured content hash for better change detection
        # Ensure all content is JSON serializable
        serializable_content = {
            'canonical_content': structured_content.get('canonical_content', ''),
            'structured_data': structured_content.get('structured_data', {}),
            'stable_elements': structured_content.get('stable_elements', {}),
            'listing_content': structured_content.get('listing_content', {}),
        }
        structured_hash = hashlib.sha256(
            json.dumps(serializable_content, sort_keys=True).encode("utf-8")
        ).hexdigest()
        identifier_parts.append(f"structured_hash:{structured_hash}")
        identifier_parts.append(f"fuzzy_hash:{fuzzy_hash}")
        
        identifier = "|".join(identifier_parts)
        
        analysis_result = {
            "url": url,
            "content_hash": content_hash,
            "fuzzy_hash": fuzzy_hash,
            "structured_hash": structured_hash,
            "last_updated": last_updated,
            "timestamp_source": timestamp_source,
            "last_modified_header": last_modified_header,
            "etag_header": etag_header,
            "identifier": identifier,
            "structured_content": serializable_content,  # Use the serializable version
            "cleaned_content_length": len(cleaned_content),
            "is_not_modified": is_not_modified,
            "response_status": response.status if response else None,
        }
        
        # Add to history for de-bouncing
        self.add_to_history(url, analysis_result)
        
        return analysis_result
    
    async def check_page_changes_lightweight(self, page: Page, url: str, old_data: dict = None) -> Dict[str, Any]:
        """Phase 1: Lightweight checks to determine if deep analysis is needed"""
        
        # Check if we have previous data to compare against
        if not old_data:
            return {"needs_deep_check": True, "reason": "no_previous_data"}
        
        # 1. HEAD request for headers only
        try:
            head_response = await page.context.request.head(url, timeout=10000)
            if head_response:
                current_last_modified = head_response.headers.get("last-modified")
                current_etag = head_response.headers.get("etag")
                content_type = head_response.headers.get("content-type", "")
                
                # Skip non-HTML content
                if not content_type.startswith("text/html"):
                    return {"needs_deep_check": True, "reason": "non_html_content", "content_type": content_type}
                
                # Check if headers indicate no change
                old_last_modified = old_data.get("last_modified_header")
                old_etag = old_data.get("etag_header")
                
                if (old_last_modified and current_last_modified and 
                    old_last_modified == current_last_modified and
                    old_etag and current_etag and 
                    old_etag == current_etag):
                    return {"needs_deep_check": False, "reason": "headers_unchanged"}
                    
        except Exception as e:
            # Continue with other checks if HEAD fails
            pass
        
        # 2. Conditional GET with previous headers
        if old_data.get("last_modified_header") or old_data.get("etag_header"):
            try:
                headers = {}
                if old_data.get("last_modified_header"):
                    headers['If-Modified-Since'] = old_data["last_modified_header"]
                if old_data.get("etag_header"):
                    headers['If-None-Match'] = old_data["etag_header"]
                
                response = await page.context.request.get(url, headers=headers, timeout=15000)
                if response and response.status == 304:
                    return {"needs_deep_check": False, "reason": "304_not_modified"}
                    
            except Exception as e:
                # Continue with other checks if conditional GET fails
                pass
        
        # 3. Check RSS/Atom feeds for recent updates
        try:
            # Get a minimal HTML sample for feed discovery
            response = await page.context.request.get(url, timeout=15000)
            if response and response.status == 200:
                html_sample = await response.text()
                # Only get first 10KB to avoid heavy downloads
                html_sample = html_sample[:10240]
                
                rss_timestamp = await self._extract_rss_timestamp(page, url, html_sample)
                if rss_timestamp:
                    # Check if RSS timestamp is newer than last crawl
                    last_crawl = old_data.get("crawl_timestamp")
                    if last_crawl:
                        try:
                            rss_dt = self._parse_timestamp_for_comparison(rss_timestamp)
                            crawl_dt = self._parse_timestamp_for_comparison(last_crawl)
                            if rss_dt and crawl_dt and rss_dt > crawl_dt:
                                return {"needs_deep_check": True, "reason": "rss_newer_than_crawl", "rss_timestamp": rss_timestamp}
                        except Exception:
                            pass
                            
        except Exception as e:
            # Continue with other checks if RSS check fails
            pass
        
        # 4. Check if last crawl was a long time ago
        last_crawl = old_data.get("crawl_timestamp")
        if last_crawl:
            try:
                crawl_dt = self._parse_timestamp_for_comparison(last_crawl)
                if crawl_dt:
                    now = datetime.now(timezone.utc)
                    days_since_crawl = (now - crawl_dt).days
                    
                    # Recrawl if more than 7 days old
                    if days_since_crawl > 7:
                        return {"needs_deep_check": True, "reason": "old_crawl", "days_since_crawl": days_since_crawl}
                        
            except Exception:
                pass
        
        # 5. Check site-specific recrawl frequency
        url_domain = urlparse(url).netloc
        if url_domain in self.site_recrawl_frequencies:
            frequency = self.site_recrawl_frequencies[url_domain]
            if last_crawl:
                try:
                    crawl_dt = self._parse_timestamp_for_comparison(last_crawl)
                    if crawl_dt:
                        now = datetime.now(timezone.utc)
                        hours_since_crawl = (now - crawl_dt).total_seconds() / 3600
                        
                        if hours_since_crawl < frequency:
                            return {"needs_deep_check": False, "reason": "within_recrawl_frequency", "hours_since_crawl": hours_since_crawl}
                            
                except Exception:
                    pass
        
        # Default: need deep check
        return {"needs_deep_check": True, "reason": "default_check_needed"}

    async def make_conditional_request(self, page: Page, url: str, last_modified: str = None, etag: str = None) -> Dict[str, Any]:
        """Make a conditional HTTP request using HEAD preflight followed by conditional GET"""
        
        # Step 1: HEAD preflight for lightweight header checks
        try:
            head_response = await page.context.request.head(url)
            
            # Check if headers indicate no change
            if head_response.status == 304:
                return {
                    "url": url,
                    "is_not_modified": True,
                    "response_status": 304,
                    "last_modified_header": last_modified,
                    "etag_header": etag,
                    "message": "Content not modified (HEAD preflight)"
                }
            
            # Check if headers suggest content might have changed
            current_last_modified = head_response.headers.get("last-modified")
            current_etag = head_response.headers.get("etag")
            
            if (last_modified and current_last_modified and 
                current_last_modified == last_modified and
                etag and current_etag and 
                current_etag == etag):
                return {
                    "url": url,
                    "is_not_modified": True,
                    "response_status": 200,
                    "last_modified_header": last_modified,
                    "etag_header": etag,
                    "message": "Headers suggest no change (HEAD preflight)"
                }
                
        except Exception as e:
            # Continue with conditional GET if HEAD fails
            pass
        
        # Step 2: Conditional GET as follow-up
        headers = {}
        if last_modified:
            headers['If-Modified-Since'] = last_modified
        if etag:
            headers['If-None-Match'] = etag
        
        try:
            response = await page.context.request.get(url, headers=headers)
            
            if response.status == 304:
                return {
                    "url": url,
                    "is_not_modified": True,
                    "response_status": 304,
                    "last_modified_header": last_modified,
                    "etag_header": etag,
                    "message": "Content not modified since last request"
                }
            elif response.status == 200:
                # Content has changed - proceed with full analysis
                return await self.analyze_page_content(page, url)
            else:
                # Unexpected status - proceed with full analysis
                return await self.analyze_page_content(page, url)
                
        except Exception as e:
            # Fallback to full analysis if conditional request fails
            return await self.analyze_page_content(page, url)
    
    def has_content_changed(self, old_identifier: str, new_identifier: str) -> bool:
        """Compare old and new identifiers to determine if content has changed"""
        if not old_identifier:
            return True
        
        old_parts = dict(part.split(':', 1) for part in old_identifier.split('|') if ':' in part)
        new_parts = dict(part.split(':', 1) for part in new_identifier.split('|') if ':' in part)
        
        # Priority 1: Check HTTP headers first (most reliable for change detection)
        if 'last_modified_header' in old_parts and old_parts.get('last_modified_header') != new_parts.get('last_modified_header'):
            return True
        
        if 'etag_header' in old_parts and old_parts.get('etag_header') != new_parts.get('etag_header'):
            return True
        
        # Priority 2: Check structured hash (meaningful content changes)
        if old_parts.get('structured_hash') != new_parts.get('structured_hash'):
            return True
        
        # Priority 3: Check content hash (but be more lenient for dynamic content)
        if old_parts.get('content_hash') != new_parts.get('content_hash'):
            # For pages with no reliable timestamp, be more conservative
            # Only consider it changed if the difference is significant
            return True
        
        return False
    
    def should_recrawl_page(self, url: str, old_data: dict, new_analysis: dict) -> bool:
        """Determine if a page should be re-crawled based on intelligent heuristics"""
        
        # Priority 1: Check if content has actually changed (most reliable indicator)
        if old_data.get("structured_hash") != new_analysis.get("structured_hash"):
            return True
        
        # Check fuzzy similarity for small changes with site-specific thresholds
        if old_data.get("fuzzy_hash") and new_analysis.get("fuzzy_hash"):
            if old_data.get("fuzzy_hash") != new_analysis.get("fuzzy_hash"):
                # Calculate similarity to see if change is significant
                old_content = old_data.get("structured_content", {}).get("canonical_content", "")
                new_content = new_analysis.get("structured_content", {}).get("canonical_content", "")
                
                if old_content and new_content:
                    similarity = self.calculate_similarity(old_content, new_content)
                    # Use site-specific threshold
                    threshold = self.get_site_specific_threshold(url)
                    if similarity < threshold:
                        return True
        
        # Check for page flapping
        if self.is_page_flapping(url, new_analysis):
            # If page is flapping, be more conservative about recrawling
            return False
        
        if old_data.get("content_hash") != new_analysis.get("content_hash"):
            return True
        
        # Priority 2: Check if we have a reliable timestamp and it hasn't changed
        if old_data.get("last_updated") and new_analysis.get("last_updated"):
            if old_data.get("last_updated") == new_analysis.get("last_updated"):
                return False
        
        # Priority 3: Check if HTTP headers haven't changed
        if (old_data.get("last_modified_header") and 
            old_data.get("last_modified_header") == new_analysis.get("last_modified_header")):
            return False
        
        if (old_data.get("etag_header") and 
            old_data.get("etag_header") == new_analysis.get("etag_header")):
            return False
        
        # Priority 4: Check if the determined last updated date is before the last scraped date
        # This prevents re-scraping when we detect an old date that's older than our last crawl
        if (old_data.get("crawl_timestamp") and new_analysis.get("last_updated")):
            try:
                last_crawl_dt = self._parse_timestamp_for_comparison(old_data["crawl_timestamp"])
                new_timestamp_dt = self._parse_timestamp_for_comparison(new_analysis["last_updated"])
                
                if last_crawl_dt and new_timestamp_dt:
                    # If the detected timestamp is older than our last crawl, don't recrawl
                    if new_timestamp_dt < last_crawl_dt:
                        return False
            except (ValueError, TypeError):
                # If timestamp parsing fails, continue with other checks
                pass
        
        # Priority 5: For pages with no reliable timestamps (null/unknown), use a time-based approach
        has_reliable_timestamp = (
            old_data.get("last_updated") or 
            old_data.get("last_modified_header") or 
            old_data.get("etag_header")
        )
        
        if not has_reliable_timestamp:
            # Check when it was last crawled
            last_crawl = old_data.get("crawl_timestamp")
            if last_crawl:
                try:
                    last_crawl_dt = self._parse_timestamp_for_comparison(last_crawl)
                    if last_crawl_dt:
                        now = datetime.now(timezone.utc)
                        
                        # If it was crawled less than 24 hours ago, don't recrawl
                        if now - last_crawl_dt < timedelta(hours=24):
                            return False
                        
                        # If it was crawled more than 24 hours ago, recrawl to check for updates
                        return True
                except:
                    pass
            else:
                # No crawl timestamp - recrawl to establish baseline
                return True
        else:
            # We have a reliable timestamp, but let's also check if it's reasonable compared to crawl time
            # This helps catch cases where we detect a timestamp that's suspiciously old
            if old_data.get("crawl_timestamp") and new_analysis.get("last_updated"):
                try:
                    last_crawl_dt = self._parse_timestamp_for_comparison(old_data["crawl_timestamp"])
                    new_timestamp_dt = self._parse_timestamp_for_comparison(new_analysis["last_updated"])
                    
                    if last_crawl_dt and new_timestamp_dt:
                        # If the detected timestamp is significantly older than our last crawl (more than 1 year),
                        # it might be a false positive - don't recrawl
                        if new_timestamp_dt < last_crawl_dt - timedelta(days=365):
                            return False
                except (ValueError, TypeError):
                    # If timestamp parsing fails, continue with other checks
                    pass
        
        # Default: recrawl if we reach this point (conservative approach)
        return True

    def find_most_recent_date_on_page(self, content: str) -> Optional[str]:
        """Find the most recent date on the page as a fallback strategy"""
        import re
        from datetime import datetime, timezone
        
        # Common date patterns
        date_patterns = [
            # ISO format
            r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})\b',
            r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\b',
            # RFC format
            r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT\b',
            # Common date formats
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',
            r'\b\d{4}-\d{2}-\d{2}\b',
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2},? \d{4}\b',
            r'\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}\b',
        ]
        
        found_dates = []
        
        for pattern in date_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                try:
                    # Try to parse the date
                    if 'T' in match and ('Z' in match or '+' in match or '-' in match):
                        # ISO format
                        dt = datetime.fromisoformat(match.replace('Z', '+00:00'))
                    elif 'T' in match:
                        # ISO format without timezone
                        dt = datetime.fromisoformat(match)
                        dt = dt.replace(tzinfo=timezone.utc)
                    elif ',' in match and 'GMT' in match:
                        # RFC format
                        dt = datetime.strptime(match, '%a, %d %b %Y %H:%M:%S GMT')
                        dt = dt.replace(tzinfo=timezone.utc)
                    elif '/' in match:
                        # MM/DD/YYYY format
                        dt = datetime.strptime(match, '%m/%d/%Y')
                        dt = dt.replace(tzinfo=timezone.utc)
                    elif '-' in match and len(match.split('-')[0]) == 4:
                        # YYYY-MM-DD format
                        dt = datetime.strptime(match, '%Y-%m-%d')
                        dt = dt.replace(tzinfo=timezone.utc)
                    elif any(month in match for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
                        # Month name format
                        try:
                            dt = datetime.strptime(match, '%B %d, %Y')
                        except:
                            try:
                                dt = datetime.strptime(match, '%B %d %Y')
                            except:
                                dt = datetime.strptime(match, '%d %B %Y')
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        continue
                    
                    found_dates.append(dt)
                except:
                    continue
        
        if found_dates:
            # Return the most recent date
            most_recent = max(found_dates)
            return most_recent.isoformat()
        
        return None
    
    def parse_relative_time(self, text: str) -> Optional[str]:
        """Parse relative time expressions like '3 ani ago', '2 days ago', etc."""
        import re
        from datetime import datetime, timezone, timedelta
        
        # Common patterns for relative time
        patterns = [
            # English patterns
            (r'(\d+)\s*years?\s*ago', 'years'),
            (r'(\d+)\s*months?\s*ago', 'months'),
            (r'(\d+)\s*weeks?\s*ago', 'weeks'),
            (r'(\d+)\s*days?\s*ago', 'days'),
            (r'(\d+)\s*hours?\s*ago', 'hours'),
            (r'(\d+)\s*minutes?\s*ago', 'minutes'),
        ]
        
        for pattern, unit in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    amount = int(match.group(1))
                    now = datetime.now(timezone.utc)
                    
                    if unit == 'years':
                        result = now - timedelta(days=amount * 365)
                    elif unit == 'months':
                        result = now - timedelta(days=amount * 30)
                    elif unit == 'weeks':
                        result = now - timedelta(weeks=amount)
                    elif unit == 'days':
                        result = now - timedelta(days=amount)
                    elif unit == 'hours':
                        result = now - timedelta(hours=amount)
                    elif unit == 'minutes':
                        result = now - timedelta(minutes=amount)
                    else:
                        continue
                    
                    # Validate the calculated timestamp is reasonable
                    if self.is_reasonable_timestamp(result.isoformat()):
                        return result.strftime("%Y-%m-%d %H:%M:%S UTC")
                        
                except (ValueError, TypeError):
                    continue
        
        return None
    
    async def extract_last_updated_with_priority(self, page: Page, content: str, url: str | None = None) -> tuple[Optional[str], str]:
        """Extract last updated timestamp using priority-based approach"""
        
        # 1) schema.org
        ts = self._extract_schema_timestamp(content)
        if ts and self.is_reasonable_timestamp(ts): 
            return ts, "schema"

        # 2) Open Graph / article meta
        ts = self._extract_og_timestamp(content)
        if ts and self.is_reasonable_timestamp(ts): 
            return ts, "og"

        # 3) RSS/Atom (same-site)
        if page is not None and url:
            ts = await self._extract_rss_timestamp(page, url, content)
            if ts and self.is_reasonable_timestamp(ts): 
                return ts, "rss_or_atom"

        # 4) Visible timestamps
        ts = self._extract_visible_timestamp(content)
        if ts and self.is_reasonable_timestamp(ts): 
            return ts, "visible"

        # 5) Meta fallbacks
        ts = self.extract_last_updated_from_meta(content)
        if ts and self.is_reasonable_timestamp(ts): 
            return ts, "meta_tag"

        # 6) Content heuristics
        ts = self.extract_last_updated_from_content(content)
        if ts and self.is_reasonable_timestamp(ts): 
            return ts, "content_extraction"

        return None, "none"
    
    def _extract_schema_timestamp(self, content: str) -> Optional[str]:
        """Extract timestamp from schema.org structured data using proper JSON parsing"""
        import re
        import json
        
        # Look for JSON-LD schema.org data with better pattern matching
        json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        json_ld_matches = re.findall(json_ld_pattern, content, re.IGNORECASE | re.DOTALL)
        
        for match in json_ld_matches:
            try:
                # Clean the JSON content
                json_content = match.strip()
                # Remove any HTML comments or CDATA sections
                json_content = re.sub(r'<!--.*?-->', '', json_content, flags=re.DOTALL)
                json_content = re.sub(r'<!\[CDATA\[.*?\]\]>', '', json_content, flags=re.DOTALL)
                
                data = json.loads(json_content)
                if isinstance(data, dict):
                    # Look for dateModified in schema.org data
                    if 'dateModified' in data:
                        return self._normalize_timestamp(data['dateModified'])
                    # Also check for @graph structure
                    if '@graph' in data and isinstance(data['@graph'], list):
                        for item in data['@graph']:
                            if isinstance(item, dict) and 'dateModified' in item:
                                return self._normalize_timestamp(item['dateModified'])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'dateModified' in item:
                            return self._normalize_timestamp(item['dateModified'])
            except (json.JSONDecodeError, ValueError) as e:
                # Log parsing errors for debugging
                continue
        
        # Look for microdata with better pattern matching
        microdata_patterns = [
            r'itemprop=["\']dateModified["\'][^>]*content=["\']([^"\']+)["\']',
            r'itemprop=["\']dateModified["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*itemprop=["\']dateModified["\'][^>]*content=["\']([^"\']+)["\']',
        ]
        
        for pattern in microdata_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                timestamp = self._normalize_timestamp(match.group(1))
                if timestamp:
                    return timestamp
        
        return None
    
    def _extract_og_timestamp(self, content: str) -> Optional[str]:
        """Extract timestamp from Open Graph meta tags"""
        import re
        
        og_patterns = [
            r'<meta[^>]*property=["\']article:modified_time["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*property=["\']og:updated_time["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)["\']',
        ]
        
        for pattern in og_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                timestamp = self._normalize_timestamp(match.group(1))
                if timestamp:
                    return timestamp
        
        return None
    
    def _extract_visible_timestamp(self, content: str) -> Optional[str]:
        """Extract visible timestamp near headline/byline"""
        import re
        
        # Look for common patterns near content
        visible_patterns = [
            r'<time[^>]*datetime=["\']([^"\']+)["\'][^>]*>',
            r'<span[^>]*class=["\'][^"\']*date[^"\']*["\'][^>]*>([^<]+)</span>',
            r'<div[^>]*class=["\'][^"\']*date[^"\']*["\'][^>]*>([^<]+)</div>',
            r'Updated[^:]*:\s*([^\n\r<]+)',
            r'Last updated[^:]*:\s*([^\n\r<]+)',
            r'Modified[^:]*:\s*([^\n\r<]+)',
        ]
        
        for pattern in visible_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                timestamp = self._normalize_timestamp(match.group(1))
                if timestamp:
                    return timestamp
        
        return None
    
    def _normalize_timestamp(self, timestamp: str) -> Optional[str]:
        """Normalize timestamp to ISO format with strict UTC parsing and sanity checks"""
        try:
            from datetime import datetime, timezone
            import re
            
            # Clean the timestamp
            timestamp = timestamp.strip()
            
            # Handle relative time expressions
            if 'ago' in timestamp.lower():
                return self.parse_relative_time(timestamp)
            
            # Handle timezone abbreviations
            tz_abbrevs = {
                'EST': -5, 'EDT': -4, 'CST': -6, 'CDT': -5,
                'MST': -7, 'MDT': -6, 'PST': -8, 'PDT': -7,
                'GMT': 0, 'UTC': 0, 'Z': 0
            }
            
            # Replace timezone abbreviations with UTC offset
            for abbrev, offset in tz_abbrevs.items():
                timestamp = re.sub(rf'\b{abbrev}\b', f'+{offset:02d}:00', timestamp, flags=re.IGNORECASE)
            
            # Common timestamp formats with strict parsing
            formats = [
                # ISO formats
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                # RFC formats
                '%a, %d %b %Y %H:%M:%S %Z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%a, %d %b %Y %H:%M:%S %z',
                # Date only formats
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%B %d, %Y',
                '%B %d %Y',
                '%d %B %Y',
                # Additional formats
                '%Y-%m-%d %H:%M',
                '%m/%d/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    
                    # Always normalize to UTC
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    
                    # Sanity checks
                    now = datetime.now(timezone.utc)
                    
                    # Reject future dates (more than 1 day ahead)
                    if dt > now + timedelta(days=1):
                        continue
                    
                    # Reject very old dates (before 1990)
                    if dt < datetime(1990, 1, 1, tzinfo=timezone.utc):
                        continue
                    
                    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    def _generate_fuzzy_hash(self, content: str) -> str:
        """Generate a fuzzy hash for similarity comparison"""
        import re
        
        # Tokenize content into words
        words = re.findall(r'\b\w+\b', content.lower())
        
        # Create a simple fingerprint using word frequency
        word_freq = {}
        for word in words:
            if len(word) > 2:  # Skip very short words
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Sort by frequency and take top words
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:50]
        
        # Create hash from top words
        fingerprint = ' '.join([word for word, freq in sorted_words])
        return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    
    def calculate_similarity(self, old_content: str, new_content: str) -> float:
        """Calculate similarity between two content strings (0.0 to 1.0)"""
        import re
        
        # Tokenize both contents
        old_words = set(re.findall(r'\b\w+\b', old_content.lower()))
        new_words = set(re.findall(r'\b\w+\b', new_content.lower()))
        
        # Calculate Jaccard similarity
        intersection = len(old_words.intersection(new_words))
        union = len(old_words.union(new_words))
        
        if union == 0:
            return 1.0  # Both empty
        
        return intersection / union
    
    async def analyze_non_html_content(self, page: Page, url: str, content_type: str) -> Dict[str, Any]:
        """Analyze non-HTML content using raw download hashing"""
        try:
            response = await page.context.request.get(url, timeout=15000)
            if not response or response.status != 200:
                return {
                    "url": url,
                    "content_hash": None,
                    "fuzzy_hash": None,
                    "structured_hash": None,
                    "last_updated": None,
                    "timestamp_source": "none",
                    "content_type": content_type,
                    "is_not_modified": False,
                    "response_status": response.status if response else None,
                }
            
            # Get raw content
            content = await response.body()
            content_hash = hashlib.sha256(content).hexdigest()
            
            # For text-based content, also generate fuzzy hash
            fuzzy_hash = None
            if content_type.startswith(('text/', 'application/json', 'application/xml')):
                try:
                    text_content = content.decode('utf-8', errors='ignore')
                    fuzzy_hash = self._generate_fuzzy_hash(text_content)
                except Exception:
                    pass
            
            # Get headers
            headers = response.headers
            last_modified_header = headers.get("last-modified")
            etag_header = headers.get("etag")
            
            # Extract timestamp from headers
            last_updated = None
            timestamp_source = "none"
            
            if last_modified_header:
                norm = self._normalize_timestamp(last_modified_header)
                if norm and self.is_reasonable_timestamp(norm):
                    last_updated = norm
                    timestamp_source = "http_header"
            
            return {
                "url": url,
                "content_hash": content_hash,
                "fuzzy_hash": fuzzy_hash,
                "structured_hash": None,
                "last_updated": last_updated,
                "timestamp_source": timestamp_source,
                "last_modified_header": last_modified_header,
                "etag_header": etag_header,
                "content_type": content_type,
                "is_not_modified": False,
                "response_status": response.status,
            }
            
        except Exception as e:
            return {
                "url": url,
                "content_hash": None,
                "fuzzy_hash": None,
                "structured_hash": None,
                "last_updated": None,
                "timestamp_source": "none",
                "content_type": content_type,
                "is_not_modified": False,
                "response_status": None,
                "error": str(e)
            }

    async def extract_sitemap_data(self, base_url: str) -> Dict[str, Any]:
        """Extract last modification dates from XML sitemap"""
        
        sitemap_data = {}
        
        # Common sitemap locations
        sitemap_urls = [
            urljoin(base_url, "/sitemap.xml"),
            urljoin(base_url, "/sitemap_index.xml"),
            urljoin(base_url, "/sitemaps.xml"),
            urljoin(base_url, "/robots.txt"),  # Check robots.txt for sitemap location
        ]
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                for sitemap_url in sitemap_urls:
                    try:
                        response = await page.goto(sitemap_url, wait_until="networkidle", timeout=10000)
                        
                        if response.status == 200:
                            content = await page.content()
                            
                            # Check if it's robots.txt
                            if sitemap_url.endswith("robots.txt"):
                                # Extract sitemap URL from robots.txt
                                for line in content.split('\n'):
                                    if line.lower().startswith('sitemap:'):
                                        sitemap_url = line.split(':', 1)[1].strip()
                                        try:
                                            response = await page.goto(sitemap_url, wait_until="networkidle", timeout=10000)
                                            if response.status == 200:
                                                content = await page.content()
                                            else:
                                                continue
                                        except:
                                            continue
                            
                            # Parse XML sitemap
                            try:
                                root = ET.fromstring(content)
                                
                                # Handle sitemap index
                                if 'sitemapindex' in root.tag:
                                    for sitemap in root.findall('.//{*}sitemap'):
                                        loc = sitemap.find('{*}loc')
                                        lastmod = sitemap.find('{*}lastmod')
                                        if loc is not None:
                                            sitemap_url = loc.text
                                            try:
                                                response = await page.goto(sitemap_url, wait_until="networkidle", timeout=10000)
                                                if response.status == 200:
                                                    sub_content = await page.content()
                                                    sub_root = ET.fromstring(sub_content)
                                                    self._extract_urls_from_sitemap(sub_root, sitemap_data)
                                            except:
                                                continue
                                else:
                                    # Handle regular sitemap
                                    self._extract_urls_from_sitemap(root, sitemap_data)
                                    
                            except ET.ParseError:
                                continue
                                
                    except Exception as e:
                        continue
                
                await browser.close()
                
        except Exception as e:
            pass
        
        return sitemap_data
    
    def _extract_urls_from_sitemap(self, root: ET.Element, sitemap_data: Dict[str, Any]) -> None:
        """Extract URL and lastmod data from sitemap XML"""
        for url_elem in root.findall('.//{*}url'):
            loc = url_elem.find('{*}loc')
            lastmod = url_elem.find('{*}lastmod')
            
            if loc is not None:
                url = loc.text
                if lastmod is not None:
                    sitemap_data[url] = {
                        "lastmod": lastmod.text,
                        "source": "sitemap"
                    }
    
    def get_sitemap_lastmod(self, url: str, sitemap_data: Dict[str, Any]) -> Optional[str]:
        """Get last modification date from sitemap for a specific URL"""
        if url in sitemap_data:
            return sitemap_data[url]["lastmod"]
        
        # Try to match URL patterns (handle trailing slashes, etc.)
        for sitemap_url, data in sitemap_data.items():
            if (url == sitemap_url or 
                url.rstrip('/') == sitemap_url.rstrip('/') or
                url.replace('www.', '') == sitemap_url.replace('www.', '')):
                return data["lastmod"]
        
                return None
    
    async def _discover_feed_urls(self, page: Page, url: str, html: str) -> list[str]:
        """Find RSS/Atom feed URLs from link tags and common paths"""
        import re
        from urllib.parse import urljoin, urlparse
        
        # Find <link rel="alternate" type="...rss|atom|xml"...> and common paths (same site only)
        candidates = set()
        for m in re.findall(
            r'<link[^>]+rel=["\']alternate["\'][^>]*type=["\']([^"\']+)["\'][^>]*href=["\']([^"\']+)["\']',
            html, flags=re.IGNORECASE
        ):
            typ, href = m[0].lower(), m[1]
            if any(t in typ for t in ("rss", "atom", "xml")):
                candidates.add(urljoin(url, href))

        for path in ("/feed", "/rss.xml", "/atom.xml", "/feed.xml", "/index.xml"):
            candidates.add(urljoin(url, path))

        host = urlparse(url).netloc
        out = []
        seen = set()
        for u in candidates:
            if urlparse(u).netloc == host:
                k = u.rstrip("/")
                if k not in seen:
                    seen.add(k)
                    out.append(u)
        return out[:5]

    async def _fetch_text(self, page: Page, url: str) -> tuple[int | None, str]:
        """Fetch text content from URL with timeout"""
        try:
            resp = await page.context.request.get(url, timeout=15000)
            if not resp: 
                return None, ""
            if resp.status >= 400: 
                return resp.status, ""
            return resp.status, (await resp.text())
        except Exception:
            return None, ""

    def _parse_feed_times(self, xml_text: str) -> list[str]:
        """Parse timestamps from RSS/Atom XML"""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return []
        
        def L(tag: str) -> str: 
            return tag.split('}')[-1].lower()
        
        times = []
        if L(root.tag) == "feed":  # Atom
            for el in root.findall(".//*"):
                if L(el.tag) in ("updated", "published") and el.text:
                    times.append(el.text.strip())
        else:  # RSS
            ch = root.find(".//lastBuildDate")
            if ch is not None and ch.text: 
                times.append(ch.text.strip())
            for item in root.findall(".//item"):
                for child in list(item):
                    if L(child.tag) in ("pubdate", "date") and child.text:
                        times.append(child.text.strip())
        return times

    def _pick_latest_iso(self, raw_times: list[str]) -> Optional[str]:
        """Pick the latest timestamp from a list of raw timestamps"""
        best = None
        for raw in raw_times:
            norm = self._normalize_timestamp(raw)
            if norm and self.is_reasonable_timestamp(norm):
                if not best: 
                    best = norm
                    continue
                try:
                    a = datetime.fromisoformat(norm.replace('Z', '+00:00'))
                    b = datetime.fromisoformat(best.replace('Z', '+00:00'))
                    if a > b: 
                        best = norm
                except Exception:
                    pass
        return best

    async def _extract_rss_timestamp(self, page: Page, url: str, html: str) -> Optional[str]:
        """Extract timestamp from RSS/Atom feeds for the given URL"""
        feed_urls = await self._discover_feed_urls(page, url, html)
        for feed_url in feed_urls:
            status, text = await self._fetch_text(page, feed_url)
            if not text or ("<rss" not in text.lower() and "<feed" not in text.lower()):
                continue
            best = self._pick_latest_iso(self._parse_feed_times(text))
            if best:
                return best
        return None
    
    def get_canonical_url(self, content: str, current_url: str) -> str:
        """Get the canonical URL for a page, handling redirects and content moves"""
        import re
        
        # Check for canonical link
        canonical_match = re.search(r'<link[^>]*rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if canonical_match:
            return canonical_match.group(1)
        
        # Check for Open Graph URL
        og_url_match = re.search(r'<meta[^>]*property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if og_url_match:
            return og_url_match.group(1)
        
        # Return current URL if no canonical found
        return current_url
    
    def is_listing_page(self, content: str) -> bool:
        """Determine if this is a listing/hub page"""
        import re
        
        # Check for common listing page indicators
        listing_indicators = [
            r'<div[^>]*class=["\'][^"\']*list[^"\']*["\'][^>]*>',
            r'<div[^>]*class=["\'][^"\']*grid[^"\']*["\'][^>]*>',
            r'<div[^>]*class=["\'][^"\']*catalog[^"\']*["\'][^>]*>',
            r'<div[^>]*class=["\'][^"\']*archive[^"\']*["\'][^>]*>',
            r'<div[^>]*class=["\'][^"\']*index[^"\']*["\'][^>]*>',
            r'<ul[^>]*class=["\'][^"\']*posts[^"\']*["\'][^>]*>',
            r'<div[^>]*class=["\'][^"\']*articles[^"\']*["\'][^>]*>',
        ]
        
        for pattern in listing_indicators:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        
        return False
    
    def add_to_history(self, url: str, analysis: Dict[str, Any]) -> None:
        """Add analysis result to page history for de-bouncing"""
        if url not in self.page_history:
            self.page_history[url] = []
        
        history_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'content_hash': analysis.get('content_hash'),
            'fuzzy_hash': analysis.get('fuzzy_hash'),
            'structured_hash': analysis.get('structured_hash'),
            'last_updated': analysis.get('last_updated'),
        }
        
        self.page_history[url].append(history_entry)
        
        # Keep only the most recent entries
        if len(self.page_history[url]) > self.max_history_size:
            self.page_history[url] = self.page_history[url][-self.max_history_size:]
    
    def is_page_flapping(self, url: str, current_analysis: Dict[str, Any]) -> bool:
        """Detect if a page is flapping (frequently changing back and forth)"""
        if url not in self.page_history or len(self.page_history[url]) < 3:
            return False
        
        history = self.page_history[url]
        current_hash = current_analysis.get('content_hash')
        
        # Check if the current hash has appeared before in recent history
        recent_hashes = [entry['content_hash'] for entry in history[-3:]]
        
        # If current hash appears multiple times in recent history, it's flapping
        if current_hash in recent_hashes:
            hash_count = recent_hashes.count(current_hash)
            if hash_count >= 2:
                return True
        
        return False
    
    def get_site_specific_threshold(self, url: str) -> float:
        """Get site-specific similarity threshold based on history"""
        if url not in self.page_history or len(self.page_history[url]) < 2:
            return 0.8  # Default threshold
        
        # Analyze history to determine optimal threshold
        history = self.page_history[url]
        
        # Calculate average change frequency
        changes = 0
        for i in range(1, len(history)):
            if history[i]['content_hash'] != history[i-1]['content_hash']:
                changes += 1
        
        change_rate = changes / (len(history) - 1)
        
        # Adjust threshold based on change rate
        if change_rate > 0.8:  # Very dynamic site
            return 0.6  # Lower threshold
        elif change_rate > 0.5:  # Moderately dynamic
            return 0.7
        else:  # Stable site
            return 0.9  # Higher threshold
    
    def get_site_recrawl_frequency(self, url: str) -> float:
        """Get the recrawl frequency for a specific site"""
        domain = urlparse(url).netloc
        return self.site_recrawl_frequencies.get(domain, self.site_recrawl_frequencies.get("default", 12))
    
    async def get_domain_feed_cache(self, domain: str) -> Dict[str, Any]:
        """Get cached feed data for a domain to avoid repeated fetches"""
        if not hasattr(self, '_domain_feed_cache'):
            self._domain_feed_cache = {}
        
        # Check if cache is still valid (cache for 1 hour)
        if domain in self._domain_feed_cache:
            cache_entry = self._domain_feed_cache[domain]
            cache_time = cache_entry.get('timestamp')
            if cache_time:
                try:
                    cache_dt = datetime.fromisoformat(cache_time.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    if (now - cache_dt).total_seconds() < 3600:  # 1 hour
                        return cache_entry.get('data', {})
                except Exception:
                    pass
        
        return {}
    
    async def set_domain_feed_cache(self, domain: str, feed_data: Dict[str, Any]) -> None:
        """Cache feed data for a domain"""
        if not hasattr(self, '_domain_feed_cache'):
            self._domain_feed_cache = {}
        
        self._domain_feed_cache[domain] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': feed_data
        }
    
    async def analyze_page_efficient(self, page: Page, url: str, old_data: dict = None) -> Dict[str, Any]:
        """Main entry point using two-phase approach for efficient change detection"""
        
        # Phase 1: Lightweight checks
        lightweight_result = await self.check_page_changes_lightweight(page, url, old_data)
        
        if not lightweight_result.get("needs_deep_check", True):
            # No deep check needed - return lightweight result with cached data
            return {
                "url": url,
                "content_hash": old_data.get("content_hash") if old_data else None,
                "fuzzy_hash": old_data.get("fuzzy_hash") if old_data else None,
                "structured_hash": old_data.get("structured_hash") if old_data else None,
                "last_updated": old_data.get("last_updated") if old_data else None,
                "timestamp_source": old_data.get("timestamp_source") if old_data else "cached",
                "last_modified_header": old_data.get("last_modified_header") if old_data else None,
                "etag_header": old_data.get("etag_header") if old_data else None,
                "identifier": old_data.get("identifier") if old_data else None,
                "structured_content": old_data.get("structured_content") if old_data else {},
                "cleaned_content_length": old_data.get("cleaned_content_length") if old_data else 0,
                "is_not_modified": True,
                "response_status": None,
                "lightweight_check": lightweight_result,
                "phase": "lightweight"
            }
        
        # Check content type for non-HTML content
        try:
            head_response = await page.context.request.head(url, timeout=5000)
            if head_response:
                content_type = head_response.headers.get("content-type", "")
                if not content_type.startswith("text/html"):
                    # Use raw content analysis for non-HTML
                    return await self.analyze_non_html_content(page, url, content_type)
        except Exception:
            pass
        
        # Phase 2: Deep check with Playwright
        analysis_result = await self.analyze_page_content(page, url)
        analysis_result["lightweight_check"] = lightweight_result
        analysis_result["phase"] = "deep"
        
        return analysis_result

# Global instance
change_detector = AdvancedChangeDetector() 