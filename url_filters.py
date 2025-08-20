from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import os

def _normalize_netloc(netloc: str) -> str:
    if not netloc:
        return netloc
    return netloc.lower().replace('www.', '')

def same_domain(url: str, base: str) -> bool:
    try:
        a = urlparse(url)
        b = urlparse(base)
        return _normalize_netloc(a.netloc) == _normalize_netloc(b.netloc)
    except Exception:
        return False

def strip_query(url: str, keep: list[str] | None = None) -> str:
    try:
        parsed = urlparse(url)
        if not keep:
            new_query = ''
        else:
            params = {k: v for k, v in parse_qsl(parsed.query) if k in keep}
            new_query = urlencode(params, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, ''))
    except Exception:
        return url

_MEDIA_EXTS = {
    # Images
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp', '.tiff', '.ico',
    # Video
    '.mp4', '.webm', '.mov', '.avi', '.mkv', '.m3u8', '.ts', '.flv', '.m4v',
    # Audio
    '.mp3', '.wav', '.ogg', '.m4a', '.flac',
    # Fonts
    '.woff', '.woff2', '.ttf', '.otf', '.eot',
    # Docs/archives
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.zip', '.rar', '.7z'
}

def is_media(url: str) -> bool:
    try:
        path = urlparse(url).path
        _, ext = os.path.splitext(path.lower())
        return ext in _MEDIA_EXTS
    except Exception:
        return False

_BLOCKED_HOST_SUBSTRINGS = (
    'facebook.com', 'fbcdn.net', 'twitter.com', 'x.com', 't.co', 'instagram.com', 'linkedin.com',
    'googletagmanager.com', 'google-analytics.com', 'analytics.google.com', 'doubleclick.net'
)

_BLOCKED_PATH_KEYWORDS = (
    '/login', '/privacy', '/help', '/careers', '/settings', '/allactivity'
)

def is_blocked(url: str, base_netloc: str | None = None) -> bool:
    try:
        parsed = urlparse(url)
        netloc = _normalize_netloc(parsed.netloc)
        # Off-domain
        if base_netloc and netloc and _normalize_netloc(base_netloc) != netloc:
            return True
        # Social/analytics hosts
        for bad in _BLOCKED_HOST_SUBSTRINGS:
            if bad in netloc:
                return True
        # Blocked paths
        path_lower = (parsed.path or '').lower()
        if any(kw in path_lower for kw in _BLOCKED_PATH_KEYWORDS):
            return True
        # Query params like share=
        if 'share=' in (parsed.query or '').lower():
            return True
        return False
    except Exception:
        return True 