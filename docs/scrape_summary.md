**Tools**

- Apify actor: `apify/cheerio-scraper` (invoked via `apify-client`)
- Python: `requests`, `beautifulsoup4`/`bs4`, and the standard `csv` module

**Approach**

- Listing scrape: use the Apify actor to fetch listing data on each pages including `detail_url` values.
- Detail extraction: a separate function visits each `detail_url` and extracts the website `website_url_primary` with targeted detection (e.g. `a[data-event="wecp_clk"]`)
