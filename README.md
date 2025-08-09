# Website FAQ Scraper + API

This project combines a web crawler that generates FAQs from website content with a FastAPI server that provides access to the crawled data and generated FAQs.

## Features

### Crawler (`crawler.py`)
- Crawls websites using Playwright
- Converts HTML content to Markdown
- Generates FAQs using Google's Gemini AI
- Implements change detection to avoid re-processing unchanged pages
- Stores data in organized file structure

### API Server (`main.py`)
- **`/last-updated`** - Get last updated time for a specific URL (auto-crawls if not found)
- **`/page-faqs`** - Get FAQs for a specific page (auto-crawls and generates FAQs if not found)
- **`/site-faqs`** - Get all FAQs for an entire website (auto-crawls base URL if domain not found)

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up Environment Variables
Create a `.env` file with your Google AI API key:
```bash
GOOGLE_GENERATIVE_AI_API_KEY=your_api_key_here
```

### 3. Run the Crawler
First, crawl a website to generate data:
```bash
python crawler.py
```

The crawler will:
- Start with the default URL (https://www.inhotel.io/)
- Crawl the website and generate FAQs
- Store data in the `storage/` directory

### 4. Run the API Server
```bash
python run_server.py
```

Or directly:
```bash
python main.py
```

The server will start on `http://localhost:8000`

## API Usage

### 1. Get Last Updated Time
```bash
curl "http://localhost:8000/last-updated?url=https://www.inhotel.io/"
```

Response:
```json
{
  "url": "https://www.inhotel.io/",
  "last_updated": "Wed, 21 Oct 2024 10:30:00 GMT",
  "has_been_crawled": true,
  "just_crawled": false
}
```

**Note**: If the URL hasn't been crawled before, the API will automatically crawl it and return `"just_crawled": true`.

### 2. Get Page FAQs
```bash
curl "http://localhost:8000/page-faqs?url=https://www.inhotel.io/"
```

Response:
```json
{
  "url": "https://www.inhotel.io/",
  "last_updated": "Wed, 21 Oct 2024 10:30:00 GMT",
  "faqs": [
    {
      "question": "What is InHotel?",
      "answer": "InHotel is a platform that..."
    },
    {
      "question": "How does booking work?",
      "answer": "Booking with InHotel is simple..."
    }
  ],
  "faq_file": "storage/datasets/faqs/inhotel_index_faq.md",
  "just_crawled": false
}
```

**Note**: If the URL hasn't been crawled before, the API will automatically crawl it, generate FAQs, and return `"just_crawled": true`.

### 3. Get All Site FAQs
```bash
curl "http://localhost:8000/site-faqs?base_url=https://www.inhotel.io/"
```

Response:
```json
{
  "base_url": "https://www.inhotel.io/",
  "total_faqs": 25,
  "crawled_pages": 8,
  "faqs": [
    {
      "question": "What is InHotel?",
      "answer": "InHotel is a platform that...",
      "source_url": "https://www.inhotel.io/"
    },
    {
      "question": "How do I contact support?",
      "answer": "You can contact support by...",
      "source_url": "https://www.inhotel.io/contact"
    }
  ],
  "just_crawled": false,
  "crawled_urls": [
    "https://www.inhotel.io/",
    "https://www.inhotel.io/contact",
    "https://www.inhotel.io/about"
  ]
}
```

**Note**: If the domain hasn't been crawled before, the API will automatically crawl the entire website (up to 50 pages) and return `"just_crawled": true`.



## API Documentation

Once the server is running, visit `http://localhost:8000/docs` for interactive API documentation powered by Swagger UI.

## File Structure

```
.
├── main.py              # FastAPI server
├── crawler.py           # Web crawler
├── run_server.py        # Server startup script
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── .env                # Environment variables (create this)
└── storage/            # Crawler data storage
    ├── change_detection.json
    └── datasets/
        ├── page_content/    # Markdown versions of pages
        └── faqs/           # Generated FAQ files
```

## Customization

### Change Default Crawl URL
Edit `crawler.py` and modify the `start_urls` list in the `main()` function.

### Adjust FAQ Generation
Modify the prompt in the `generate_faq_from_markdown()` function in `crawler.py`.

### Change API Port
Edit the port in `main.py` or `run_server.py`.

## Error Handling

The API includes comprehensive error handling:
- Automatic crawling for URLs not found in crawled data
- Graceful handling of missing FAQ files
- Detailed error messages for debugging
- 500 errors for crawling failures (network issues, invalid URLs, etc.)

## Performance

- Change detection prevents unnecessary re-crawling
- FAQ generation is cached in files
- API responses are optimized for speed
- Health check endpoint for monitoring

## Troubleshooting

1. **Crawling failures**: Check your internet connection and ensure the Google AI API key is set correctly
2. **Missing FAQs**: The API will automatically generate FAQs for new URLs
3. **Server won't start**: Ensure all dependencies are installed
4. **Slow responses**: First-time requests may take longer as the API crawls and generates FAQs automatically 