import re
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LanguageDetectionResult:
    """Structured result from language detection"""
    detected_lang: str  # ISO-639-1 code
    confidence: float   # 0.0 to 1.0
    source: str        # 'metadata', 'content', 'fallback'
    is_rtl: bool       # Right-to-left language
    raw_detection: Dict[str, Any]  # Raw detection data for debugging
    script_hint: Optional[str] = None  # Script hint for Chinese variants

class LanguageDetector:
    """Centralized language detection with metadata hints and robust fallbacks"""
    
    def __init__(self):
        # RTL language codes
        self.rtl_languages = {
            'ar', 'he', 'fa', 'ur', 'ps', 'sd', 'yi', 'dv', 'ku', 'ckb'
        }
        
        # Chinese script variants
        self.chinese_scripts = {
            'zh-cn': 'Simplified Chinese',
            'zh-tw': 'Traditional Chinese', 
            'zh-hk': 'Traditional Chinese',
            'zh-sg': 'Simplified Chinese',
            'zh-mo': 'Traditional Chinese'
        }
        
        # Language code mappings for common variations
        self.lang_code_mappings = {
            'en-us': 'en', 'en-gb': 'en', 'en-ca': 'en', 'en-au': 'en',
            'es-es': 'es', 'es-mx': 'es', 'es-ar': 'es', 'es-cl': 'es',
            'fr-fr': 'fr', 'fr-ca': 'fr', 'fr-be': 'fr', 'fr-ch': 'fr',
            'de-de': 'de', 'de-at': 'de', 'de-ch': 'de', 'de-li': 'de',
            'pt-br': 'pt', 'pt-pt': 'pt',
            'zh-cn': 'zh', 'zh-tw': 'zh', 'zh-hk': 'zh', 'zh-sg': 'zh',
            'ja-jp': 'ja',
            'ko-kr': 'ko',
            'ru-ru': 'ru',
            'it-it': 'it', 'it-ch': 'it',
            'nl-nl': 'nl', 'nl-be': 'nl',
            'sv-se': 'sv', 'sv-fi': 'sv',
            'da-dk': 'da',
            'no-no': 'no',
            'fi-fi': 'fi',
            'pl-pl': 'pl',
            'cs-cz': 'cs',
            'sk-sk': 'sk',
            'hu-hu': 'hu',
            'ro-ro': 'ro',
            'bg-bg': 'bg',
            'hr-hr': 'hr',
            'sl-si': 'sl',
            'et-ee': 'et',
            'lv-lv': 'lv',
            'lt-lt': 'lt',
            'mt-mt': 'mt',
            'el-gr': 'el',
            'tr-tr': 'tr',
            'is-is': 'is',
            'ga-ie': 'ga',
            'cy-gb': 'cy',
            'eu-es': 'eu',
            'ca-es': 'ca',
            'gl-es': 'gl',
            'ast-es': 'ast',
            'oc-fr': 'oc',
            'br-fr': 'br',
            'co-fr': 'co',
            'rm-ch': 'rm',
            'fur-it': 'fur',
            'sc-it': 'sc',
            'vec-it': 'vec',
            'lmo-it': 'lmo',
            'pms-it': 'pms',
            'nap-it': 'nap',
            'scn-it': 'scn',
            'lij-it': 'lij',
            'rgn-it': 'rgn',
            'eml-it': 'eml',
        }
        
        # TLD to language mappings for URL-based hints
        self.tld_language_hints = {
            '.fr': 'fr', '.de': 'de', '.es': 'es', '.it': 'it', '.pt': 'pt',
            '.ru': 'ru', '.pl': 'pl', '.nl': 'nl', '.se': 'sv', '.no': 'no',
            '.dk': 'da', '.fi': 'fi', '.hu': 'hu', '.ro': 'ro', '.bg': 'bg',
            '.hr': 'hr', '.si': 'sl', '.sk': 'sk', '.cz': 'cs', '.ee': 'et',
            '.lv': 'lv', '.lt': 'lt', '.mt': 'mt', '.gr': 'el', '.tr': 'tr',
            '.is': 'is', '.ie': 'ga', '.uk': 'en', '.au': 'en',
            '.jp': 'ja', '.kr': 'ko', '.cn': 'zh', '.tw': 'zh', '.hk': 'zh',
            '.sg': 'zh', '.ar': 'ar', '.il': 'he', '.ir': 'fa', '.pk': 'ur',
            '.af': 'ps', '.sd': 'sd', '.yi': 'yi', '.mv': 'dv', '.iq': 'ku'
        }
        
        # Initialize language detection libraries
        self._init_detectors()
    
    def _init_detectors(self):
        """Initialize language detection libraries with fallbacks"""
        self.langdetect_available = False
        
        # Try langdetect
        try:
            import langdetect
            self.langdetect_available = True
            logger.info("langdetect language detector initialized")
        except ImportError:
            logger.warning("langdetect not available, language detection will be limited")
    
    def extract_metadata_hints(self, content: str, url: str = None) -> Dict[str, Any]:
        """Extract language hints from HTML metadata with improved parsing"""
        hints = {
            'html_lang': None,
            'og_locale': None,
            'content_language': None,
            'meta_language': None,
            'hreflang': [],
            'alternate_languages': [],
            'url_hint': None
        }
        
        # Extract <html lang> attribute
        html_lang_match = re.search(r'<html[^>]*lang=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if html_lang_match:
            hints['html_lang'] = html_lang_match.group(1).lower()
        
        # Extract Open Graph locale
        og_locale_match = re.search(r'<meta[^>]*property=["\']og:locale["\'][^>]*content=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if og_locale_match:
            hints['og_locale'] = og_locale_match.group(1).lower()
        
        # Extract content-language meta tag with improved parsing
        content_lang_match = re.search(r'<meta[^>]*http-equiv=["\']content-language["\'][^>]*content=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if content_lang_match:
            content_lang_value = content_lang_match.group(1).lower()
            # Split on commas/semicolons and take the first valid code
            for lang_code in re.split(r'[,;]', content_lang_value):
                lang_code = lang_code.strip()
                if lang_code and lang_code != 'x-default':
                    normalized = self.normalize_language_code(lang_code)
                    if normalized != 'und':
                        hints['content_language'] = lang_code
                        break
        
        # Extract language meta tag
        lang_meta_match = re.search(r'<meta[^>]*name=["\']language["\'][^>]*content=["\']([^"\']+)["\']', content, re.IGNORECASE)
        if lang_meta_match:
            hints['meta_language'] = lang_meta_match.group(1).lower()
        
        # Extract hreflang attributes with improved filtering
        hreflang_matches = re.findall(r'<link[^>]*hreflang=["\']([^"\']+)["\'][^>]*>', content, re.IGNORECASE)
        for lang_code in hreflang_matches:
            lang_code = lang_code.lower()
            if lang_code != 'x-default':
                hints['hreflang'].append(lang_code)
        
        # Extract alternate language links
        alternate_matches = re.findall(r'<link[^>]*rel=["\']alternate["\'][^>]*hreflang=["\']([^"\']+)["\'][^>]*>', content, re.IGNORECASE)
        for lang_code in alternate_matches:
            lang_code = lang_code.lower()
            if lang_code != 'x-default':
                hints['alternate_languages'].append(lang_code)
        
        # Add URL-based hint
        if url:
            hints['url_hint'] = self._get_url_language_hint(url)
        
        return hints
    
    def _get_url_language_hint(self, url: str) -> Optional[str]:
        """Get language hint from URL TLD"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Check for TLD hints
            for tld, lang_code in self.tld_language_hints.items():
                if domain.endswith(tld):
                    return lang_code
            
            # Check for two-letter subdomains first (e.g., fr.example.com)
            subdomain = domain.split('.')[0] if '.' in domain else None
            if subdomain and len(subdomain) == 2 and subdomain.isalpha():
                normalized = self.normalize_language_code(subdomain)
                if normalized != 'und':
                    return normalized
            
            # Check for subdomain hints in lang_code_mappings
            if subdomain and subdomain in self.lang_code_mappings:
                return self.lang_code_mappings[subdomain]
                
        except Exception:
            pass
        
        return None
    
    def normalize_language_code(self, lang_code: str) -> str:
        """Normalize language code to ISO-639-1"""
        if not lang_code:
            return 'und'
        
        # Convert to lowercase and clean
        lang_code = lang_code.lower().strip()
        
        # Handle common variations
        if lang_code in self.lang_code_mappings:
            return self.lang_code_mappings[lang_code]
        
        # Extract primary language code (before hyphen/underscore)
        primary_code = re.split(r'[-_]', lang_code)[0]
        
        # Validate it's a reasonable language code
        if len(primary_code) == 2 and primary_code.isalpha():
            return primary_code
        elif len(primary_code) == 3 and primary_code.isalpha():
            # Handle ISO-639-2 codes - map common ones
            iso639_2_to_1 = {
                'eng': 'en', 'spa': 'es', 'fra': 'fr', 'deu': 'de', 'por': 'pt',
                'zho': 'zh', 'jpn': 'ja', 'kor': 'ko', 'rus': 'ru', 'ita': 'it',
                'nld': 'nl', 'swe': 'sv', 'dan': 'da', 'nor': 'no', 'fin': 'fi',
                'pol': 'pl', 'ces': 'cs', 'slk': 'sk', 'hun': 'hu', 'ron': 'ro',
                'bul': 'bg', 'hrv': 'hr', 'slv': 'sl', 'est': 'et', 'lav': 'lv',
                'lit': 'lt', 'mlt': 'mt', 'ell': 'el', 'tur': 'tr', 'isl': 'is',
                'gle': 'ga', 'cym': 'cy', 'eus': 'eu', 'cat': 'ca', 'glg': 'gl',
                'ast': 'ast', 'oci': 'oc', 'bre': 'br', 'cos': 'co', 'roh': 'rm',
                'fur': 'fur', 'srd': 'sc', 'vec': 'vec', 'lmo': 'lmo', 'pms': 'pms',
                'nap': 'nap', 'scn': 'scn', 'lij': 'lij', 'rgn': 'rgn', 'eml': 'eml',
                'ara': 'ar', 'heb': 'he', 'fas': 'fa', 'urd': 'ur', 'pus': 'ps',
                'snd': 'sd', 'yid': 'yi', 'div': 'dv', 'kur': 'ku', 'ckb': 'ckb'
            }
            return iso639_2_to_1.get(primary_code, 'und')
        
        return 'und'
    
    def detect_language_from_content(self, text: str) -> Tuple[str, float, str]:
        """Detect language from text content using available detectors with improved logic"""
        if not text or len(text.strip()) < 100:  # Increased threshold
            return 'und', 0.0, 'insufficient_text'
        
        # Clean text for better detection
        cleaned_text = self._clean_text_for_detection(text)
        
        # Try langdetect
        if self.langdetect_available:
            try:
                import langdetect
                from langdetect import detect_langs, DetectorFactory
                # Set seed for consistent results
                DetectorFactory.seed = 0
                
                # Get multiple language probabilities
                lang_probs = detect_langs(cleaned_text)
                if lang_probs:
                    top_lang = lang_probs[0]
                    confidence = top_lang.prob
                    
                    # Reduce confidence if top two languages are close
                    if len(lang_probs) > 1:
                        margin = top_lang.prob - lang_probs[1].prob
                        if margin < 0.15:
                            confidence *= 0.8
                    
                    lang_code = self.normalize_language_code(top_lang.lang)
                    return lang_code, confidence, 'langdetect'
            except Exception as e:
                logger.warning(f"langdetect detection failed: {e}")
        
        return 'und', 0.0, 'detection_failed'
    
    def _clean_text_for_detection(self, text: str) -> str:
        """Clean text for better language detection while preserving CJK and RTL characters"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Remove URLs
        text = re.sub(r'https?://[^\s]+', ' ', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', ' ', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove numbers but keep CJK and RTL characters
        text = re.sub(r'\b\d+\b', ' ', text)
        
        # Keep letters, spaces, basic punctuation, CJK, and RTL characters
        # Simplified regex to avoid character class issues
        text = re.sub(r'[^\w\s.,!?;:()\'"\-]', ' ', text)
        
        return text.strip()
    
    def is_rtl_language(self, lang_code: str) -> bool:
        """Check if language is right-to-left"""
        return lang_code in self.rtl_languages
    
    def _get_script_hint(self, lang_code: str, original_code: str = None) -> Optional[str]:
        """Get script hint for Chinese variants"""
        if lang_code == 'zh' and original_code:
            return self.chinese_scripts.get(original_code.lower())
        return None
    
    def create_language_directive(self, lang_code: str, confidence: float, script_hint: str = None) -> str:
        """Create language directive for LLM prompts with improved logic"""
        if lang_code == 'und' or confidence < 0.5:
            return "REQUIREMENT: Write the FAQs in English. If the page content is in a different language, translate the FAQs to English."
        
        # Get language name for better instruction
        lang_names = {
            'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German',
            'pt': 'Portuguese', 'it': 'Italian', 'nl': 'Dutch', 'sv': 'Swedish',
            'da': 'Danish', 'no': 'Norwegian', 'fi': 'Finnish', 'pl': 'Polish',
            'cs': 'Czech', 'sk': 'Slovak', 'hu': 'Hungarian', 'ro': 'Romanian',
            'bg': 'Bulgarian', 'hr': 'Croatian', 'sl': 'Slovenian', 'et': 'Estonian',
            'lv': 'Latvian', 'lt': 'Lithuanian', 'mt': 'Maltese', 'el': 'Greek',
            'tr': 'Turkish', 'is': 'Icelandic', 'ga': 'Irish', 'cy': 'Welsh',
            'eu': 'Basque', 'ca': 'Catalan', 'gl': 'Galician', 'ast': 'Asturian',
            'oc': 'Occitan', 'br': 'Breton', 'cos': 'Corsican', 'rm': 'Romansh',
            'fur': 'Friulian', 'srd': 'Sardinian', 'vec': 'Venetian', 'lmo': 'Lombard',
            'pms': 'Piedmontese', 'nap': 'Neapolitan', 'scn': 'Sicilian', 'lij': 'Ligurian',
            'rgn': 'Romagnol', 'eml': 'Emilian', 'zh': 'Chinese', 'ja': 'Japanese',
            'ko': 'Korean', 'ru': 'Russian', 'ar': 'Arabic', 'he': 'Hebrew',
            'fa': 'Persian', 'ur': 'Urdu', 'ps': 'Pashto', 'sd': 'Sindhi',
            'yi': 'Yiddish', 'dv': 'Dhivehi', 'ku': 'Kurdish', 'ckb': 'Central Kurdish'
        }
        
        lang_name = lang_names.get(lang_code, lang_code.upper())
        
        directive = f"REQUIREMENT: Write the FAQs strictly in {lang_name} (language code: {lang_code}). "
        directive += f"Both questions and answers must be in {lang_name}. "
        
        if self.is_rtl_language(lang_code):
            directive += f"Note: {lang_name} is a right-to-left language. "
        
        if script_hint:
            directive += f"Use {script_hint} script. "
        
        directive += f"Generate FAQs in the same language as the page content. "
        directive += f"The detected language is {lang_code} with {confidence:.1%} confidence. "
        directive += f"You must use {lang_name} for both questions and answers."
        
        return directive
    
    def detect_language(self, content: str, url: str = None) -> LanguageDetectionResult:
        """Main language detection method with improved confidence blending"""
        
        # Step 1: Extract metadata hints
        metadata_hints = self.extract_metadata_hints(content, url)
        
        # Step 2: Get metadata-based detection with lower confidence
        metadata_lang = 'und'
        metadata_confidence = 0.0
        metadata_source = 'none'
        script_hint = None
        
        # Check metadata hints in order of reliability (lowered confidence)
        if metadata_hints['html_lang']:
            metadata_lang = self.normalize_language_code(metadata_hints['html_lang'])
            metadata_confidence = 0.7
            metadata_source = 'html_lang'
            script_hint = self._get_script_hint(metadata_lang, metadata_hints['html_lang'])
        elif metadata_hints['og_locale']:
            metadata_lang = self.normalize_language_code(metadata_hints['og_locale'])
            metadata_confidence = 0.65
            metadata_source = 'og_locale'
            script_hint = self._get_script_hint(metadata_lang, metadata_hints['og_locale'])
        elif metadata_hints['content_language']:
            metadata_lang = self.normalize_language_code(metadata_hints['content_language'])
            metadata_confidence = 0.6
            metadata_source = 'content_language'
            script_hint = self._get_script_hint(metadata_lang, metadata_hints['content_language'])
        elif metadata_hints['meta_language']:
            metadata_lang = self.normalize_language_code(metadata_hints['meta_language'])
            metadata_confidence = 0.55
            metadata_source = 'meta_language'
            script_hint = self._get_script_hint(metadata_lang, metadata_hints['meta_language'])
        elif metadata_hints['hreflang']:
            # Use the first hreflang as hint
            metadata_lang = self.normalize_language_code(metadata_hints['hreflang'][0])
            metadata_confidence = 0.5
            metadata_source = 'hreflang'
            script_hint = self._get_script_hint(metadata_lang, metadata_hints['hreflang'][0])
        elif metadata_hints['url_hint']:
            metadata_lang = metadata_hints['url_hint']
            metadata_confidence = 0.4
            metadata_source = 'url_hint'
        
        # Step 3: Get content-based detection
        text_content = self._extract_text_content(content)
        content_lang = 'und'
        content_confidence = 0.0
        content_source = 'none'
        
        if text_content:
            content_lang, content_confidence, content_source = self.detect_language_from_content(text_content)
            
            # Cap confidence for short text
            if len(text_content) < 200:
                content_confidence = min(content_confidence, 0.8)
        
        # Step 4: Blend metadata and content detection
        final_lang = 'en'  # Default to English instead of 'und'
        final_confidence = 0.5
        final_source = 'fallback_english'
        
        # If we have reliable content detection, prefer it
        if content_lang != 'und' and content_confidence > 0.6:
            final_lang = content_lang
            final_confidence = content_confidence
            final_source = f'content_{content_source}'
            
            # If metadata suggests a different language but content is reliable, stick with content
            if metadata_lang != 'und' and metadata_lang != content_lang:
                logger.info(f"Content detection ({content_lang}) differs from metadata ({metadata_lang}), preferring content")
        
        # If content detection failed or is unreliable, use metadata
        elif metadata_lang != 'und':
            final_lang = metadata_lang
            final_confidence = metadata_confidence
            final_source = metadata_source
            
            # Fix metadata script hint lookup with explicit mapping
            source_to_raw_map = {
                'html_lang': 'html_lang',
                'og_locale': 'og_locale', 
                'content_language': 'content_language',
                'meta_language': 'meta_language',
                'hreflang': 'hreflang'
            }
            raw_key = source_to_raw_map.get(metadata_source)
            raw_value = metadata_hints.get(raw_key) if raw_key else None
            script_hint = self._get_script_hint(metadata_lang, raw_value)
        
        # Step 5: Determine if RTL
        is_rtl = self.is_rtl_language(final_lang)
        
        # Step 6: Create raw detection data for debugging
        raw_detection = {
            'metadata_hints': metadata_hints,
            'metadata_lang': metadata_lang,
            'metadata_confidence': metadata_confidence,
            'metadata_source': metadata_source,
            'content_lang': content_lang,
            'content_confidence': content_confidence,
            'content_source': content_source,
            'text_length': len(text_content) if text_content else 0,
            'final_lang': final_lang,
            'final_confidence': final_confidence,
            'final_source': final_source,
            'is_rtl': is_rtl,
            'script_hint': script_hint,
            'url': url
        }
        
        return LanguageDetectionResult(
            detected_lang=final_lang,
            confidence=final_confidence,
            source=final_source,
            is_rtl=is_rtl,
            script_hint=script_hint,
            raw_detection=raw_detection
        )
    
    def get_language_info(self, content: str, url: str = None) -> Dict[str, str]:
        """Get language information in a simple format for LLM prompts"""
        result = self.detect_language(content, url)
        
        # Get human-readable language name
        lang_names = {
            'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German',
            'pt': 'Portuguese', 'it': 'Italian', 'nl': 'Dutch', 'sv': 'Swedish',
            'da': 'Danish', 'no': 'Norwegian', 'fi': 'Finnish', 'pl': 'Polish',
            'cs': 'Czech', 'sk': 'Slovak', 'hu': 'Hungarian', 'ro': 'Romanian',
            'bg': 'Bulgarian', 'hr': 'Croatian', 'sl': 'Slovenian', 'et': 'Estonian',
            'lv': 'Latvian', 'lt': 'Lithuanian', 'mt': 'Maltese', 'el': 'Greek',
            'tr': 'Turkish', 'is': 'Icelandic', 'ga': 'Irish', 'cy': 'Welsh',
            'eu': 'Basque', 'ca': 'Catalan', 'gl': 'Galician', 'ast': 'Asturian',
            'oc': 'Occitan', 'br': 'Breton', 'cos': 'Corsican', 'rm': 'Romansh',
            'fur': 'Friulian', 'srd': 'Sardinian', 'vec': 'Venetian', 'lmo': 'Lombard',
            'pms': 'Piedmontese', 'nap': 'Neapolitan', 'scn': 'Sicilian', 'lij': 'Ligurian',
            'rgn': 'Romagnol', 'eml': 'Emilian', 'zh': 'Chinese', 'ja': 'Japanese',
            'ko': 'Korean', 'ru': 'Russian', 'ar': 'Arabic', 'he': 'Hebrew',
            'fa': 'Persian', 'ur': 'Urdu', 'ps': 'Pashto', 'sd': 'Sindhi',
            'yi': 'Yiddish', 'dv': 'Dhivehi', 'ku': 'Kurdish', 'ckb': 'Central Kurdish'
        }
        
        return {
            'iso_code': result.detected_lang,
            'language_name': lang_names.get(result.detected_lang, result.detected_lang.upper()),
            'confidence': result.confidence,
            'source': result.source,
            'is_rtl': result.is_rtl,
            'script_hint': result.script_hint
        }
    
    def _extract_text_content(self, content: str) -> str:
        """Extract text content from HTML with improved selector handling"""
        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        
        # Extract text from content areas in priority order
        extracted_parts = []
        
        # 1. <main> tags
        main_matches = re.findall(r'<main[^>]*>(.*?)</main>', content, re.DOTALL | re.IGNORECASE)
        extracted_parts.extend(main_matches)
        
        # 2. <article> tags
        article_matches = re.findall(r'<article[^>]*>(.*?)</article>', content, re.DOTALL | re.IGNORECASE)
        extracted_parts.extend(article_matches)
        
        # 3. Elements with .content class
        content_class_matches = re.findall(r'<[^>]*class=["\'][^"\']*content[^"\']*["\'][^>]*>(.*?)</[^>]*>', content, re.DOTALL | re.IGNORECASE)
        extracted_parts.extend(content_class_matches)
        
        # 4. Elements with #content id
        content_id_matches = re.findall(r'<[^>]*id=["\']content["\'][^>]*>(.*?)</[^>]*>', content, re.DOTALL | re.IGNORECASE)
        extracted_parts.extend(content_id_matches)
        
        # 5. Additional content selectors
        additional_selectors = [
            '.main-content', '#main', '.post-content', '.entry-content', 
            '.article-content', '.story-content', '.page-content'
        ]
        
        for selector in additional_selectors:
            if selector.startswith('.'):
                # Class selector
                pattern = rf'<[^>]*class=["\'][^"\']*{selector[1:]}[^"\']*["\'][^>]*>(.*?)</[^>]*>'
            else:
                # ID selector
                pattern = rf'<[^>]*id=["\']{selector[1:]}["\'][^>]*>(.*?)</[^>]*>'
            
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            extracted_parts.extend(matches)
        
        # Combine all extracted parts
        if extracted_parts:
            extracted_text = ' '.join(extracted_parts)
        else:
            # Fallback to body content
            body_match = re.search(r'<body[^>]*>(.*?)</body>', content, re.DOTALL | re.IGNORECASE)
            if body_match:
                extracted_text = body_match.group(1)
            else:
                # Last resort: extract all text
                extracted_text = re.sub(r'<[^>]+>', ' ', content)
        
        # Clean up the text
        extracted_text = re.sub(r'\s+', ' ', extracted_text).strip()
        
        return extracted_text

# Global instance
language_detector = LanguageDetector() 