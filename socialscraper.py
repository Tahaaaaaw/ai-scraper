# social_scraper_app.py (with Streamlit UI)

import asyncio
import aiohttp
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd
import os
import streamlit as st

# Only look for Facebook and Instagram
SOCIAL_DOMAINS = ["facebook.com", "instagram.com"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}

# Async HTTP fetch
async def fetch(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=10) as response:
            if response.status == 200:
                return await response.text()
            return ""
    except:
        return ""

# Extract all <a> tag links from HTML
def extract_links_from_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        href = urljoin(base_url, href)
        links.add(href)
    return links

# Filter Facebook and Instagram links
def filter_social_links(links):
    return list(set(link for link in links if any(social in link for social in SOCIAL_DOMAINS)))

# Crawl a single website: homepage + a few subpages (about, contact, footer links)
async def crawl_site(session, base_url):
    visited = set()
    to_visit = {base_url}
    social_links_found = {}

    for _ in range(3):  # crawl up to 3 pages max
        if not to_visit:
            break

        next_url = to_visit.pop()
        visited.add(next_url)

        html = await fetch(session, next_url)
        if not html:
            continue

        links = extract_links_from_html(html, base_url)
        new_social_links = filter_social_links(links)

        for link in new_social_links:
            if link not in social_links_found:
                social_links_found[link] = next_url

        if social_links_found:
            break

        for link in links:
            if base_url in link and link not in visited:
                to_visit.add(link)

    result = {"Website": base_url}
    if social_links_found:
        for i, (social, source_page) in enumerate(social_links_found.items(), 1):
            result[f"Social Link {i}"] = social
            result[f"Found On Page {i}"] = source_page
    else:
        result["Social Link 1"] = ""
        result["Found On Page 1"] = ""
    return result

# Process list of websites with per-site progress
async def process_websites(websites):
    results = []
    progress_bar = st.progress(0)
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(websites):
            result = await crawl_site(session, url)
            results.append(result)
            progress_bar.progress((i + 1) / len(websites))
    return results

# Export results to CSV and Excel
def export_results(results, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(results)
    csv_path = os.path.join(output_dir, "social_links.csv")
    xlsx_path = os.path.join(output_dir, "social_links.xlsx")
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    return csv_path, xlsx_path

# Streamlit UI
st.set_page_config(page_title="Social Media Link Scraper")
st.title("🔍 Website Social Media Link Scraper")

st.markdown("""
### 📋 Instructions:
- Upload a CSV file with a **`Website`** column
- Example format:
```
Website
https://example.com
https://designsyncs.com
```
""")

sample_df = pd.DataFrame({"Website": ["https://example.com", "https://designsyncs.com"]})
st.download_button("⬇️ Download Sample CSV", sample_df.to_csv(index=False), file_name="sample_websites.csv")

uploaded_file = st.file_uploader("📁 Upload your CSV file here", type="csv")

if uploaded_file is None:
    st.info("👆 Please upload a CSV file to continue.")
else:
    df_input = pd.read_csv(uploaded_file)

    if "Website" not in df_input.columns:
        st.error("❌ CSV must contain a 'Website' column.")
    else:
        websites = df_input["Website"].dropna().unique().tolist()

        if st.button("Start Scraping"):
            with st.spinner("Scraping websites... this may take a moment."):
                results = asyncio.run(process_websites(websites))
                df_output = pd.DataFrame(results)

            st.success("✅ Scraping completed!")
            st.dataframe(df_output)

            csv = df_output.to_csv(index=False).encode("utf-8")
            xlsx_path = os.path.join("output", "social_links.xlsx")
            os.makedirs("output", exist_ok=True)
            df_output.to_excel(xlsx_path, index=False)

            st.download_button(
                label="📄 Download CSV",
                data=csv,
                file_name="social_links.csv",
                mime="text/csv",
            )

            with open(xlsx_path, "rb") as f:
                st.download_button(
                    label="📘 Download Excel",
                    data=f,
                    file_name="social_links.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
