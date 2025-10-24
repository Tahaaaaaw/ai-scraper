import asyncio
import aiohttp
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import pandas as pd
import streamlit as st
import nest_asyncio
from datetime import datetime

# Allow nested event loops (required for Streamlit)
nest_asyncio.apply()

# Social domains to look for
SOCIAL_DOMAINS = {
    "facebook.com": "Facebook",
    "instagram.com": "Instagram",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}

# Initialize session state
if 'results' not in st.session_state:
    st.session_state.results = None

# Async HTTP fetch
async def fetch(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=10, ssl=False) as response:
            status = response.status
            if status == 200:
                return await response.text(), status
            return "", status
    except Exception as e:
        return "", 0

# Extract all <a> tag links from HTML
def extract_links_from_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        links.add(href)
    return links

# Check if URL is a social media link
def is_social_link(url):
    parsed = urlparse(url.lower())
    domain = parsed.netloc.replace("www.", "")
    
    for social_domain, platform in SOCIAL_DOMAINS.items():
        if social_domain in domain:
            return platform
    return None

# Crawl a single website for Facebook and Instagram links
async def crawl_site(session, base_url, biz_name):
    visited = set()
    to_visit = {base_url}
    social_links = {"Facebook": "Not Found", "Instagram": "Not Found"}
    
    # Get base domain for comparison
    base_domain = urlparse(base_url).netloc
    
    start_time = datetime.now()
    pages_crawled = 0
    max_pages = 3
    status_code = 0

    while to_visit and pages_crawled < max_pages:
        # Stop early if we found both
        if social_links["Facebook"] != "Not Found" and social_links["Instagram"] != "Not Found":
            break

        next_url = to_visit.pop()
        if next_url in visited:
            continue
            
        visited.add(next_url)
        pages_crawled += 1

        html, status = await fetch(session, next_url)
        if pages_crawled == 1:
            status_code = status
        
        if not html:
            continue

        links = extract_links_from_html(html, base_url)
        
        for link in links:
            # Check if it's a social media link
            platform = is_social_link(link)
            if platform:
                # Only update if we haven't found this platform yet
                if social_links[platform] == "Not Found":
                    social_links[platform] = link
            
            # Add internal links to crawl queue
            try:
                link_domain = urlparse(link).netloc
                if base_domain in link_domain and link not in visited and len(to_visit) < 10:
                    to_visit.add(link)
            except:
                continue
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        "Business Name": biz_name,
        "Website": base_url,
        "Facebook": social_links["Facebook"],
        "Instagram": social_links["Instagram"],
        "Status Code": status_code,
        "Pages Crawled": pages_crawled,
        "Duration (s)": round(duration, 2),
        "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Process list of websites
async def process_websites(businesses, progress_bar, status_text):
    results = []
    async with aiohttp.ClientSession() as session:
        for i, (biz_name, url) in enumerate(businesses):
            status_text.text(f"Processing: {biz_name} ({i+1}/{len(businesses)})")
            result = await crawl_site(session, url, biz_name)
            results.append(result)
            progress_bar.progress((i + 1) / len(businesses))
    return results

# Main function to run async code
def run_scraper(businesses):
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Run async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(process_websites(businesses, progress_bar, status_text))
    loop.close()
    
    return results

# Analytics Dashboard
def show_analytics(results_df):
    st.subheader("📊 Analytics Dashboard")
    
    # Overall Stats
    col1, col2, col3, col4 = st.columns(4)
    
    fb_found = (results_df['Facebook'] != 'Not Found').sum()
    ig_found = (results_df['Instagram'] != 'Not Found').sum()
    both_found = ((results_df['Facebook'] != 'Not Found') & 
                 (results_df['Instagram'] != 'Not Found')).sum()
    neither_found = ((results_df['Facebook'] == 'Not Found') & 
                    (results_df['Instagram'] == 'Not Found')).sum()
    
    with col1:
        st.metric("✅ Total Scraped", len(results_df))
    with col2:
        fb_rate = (fb_found / len(results_df) * 100) if len(results_df) > 0 else 0
        st.metric("📘 Facebook Found", f"{fb_found} ({fb_rate:.1f}%)")
    with col3:
        ig_rate = (ig_found / len(results_df) * 100) if len(results_df) > 0 else 0
        st.metric("📸 Instagram Found", f"{ig_found} ({ig_rate:.1f}%)")
    with col4:
        both_rate = (both_found / len(results_df) * 100) if len(results_df) > 0 else 0
        st.metric("🎯 Both Found", f"{both_found} ({both_rate:.1f}%)")
    
    # Second row of metrics
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric("❌ Neither Found", neither_found)
    with col6:
        avg_duration = results_df['Duration (s)'].mean()
        st.metric("⏱️ Avg Time", f"{avg_duration:.2f}s")
    with col7:
        successful = (results_df['Status Code'] == 200).sum()
        success_rate = (successful / len(results_df) * 100) if len(results_df) > 0 else 0
        st.metric("🌐 Success Rate", f"{success_rate:.1f}%")
    with col8:
        avg_pages = results_df['Pages Crawled'].mean()
        st.metric("📄 Avg Pages", f"{avg_pages:.1f}")
    
    # Charts
    st.divider()
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("##### Social Media Coverage")
        coverage_data = pd.DataFrame({
            'Category': ['Facebook Only', 'Instagram Only', 'Both', 'Neither'],
            'Count': [
                fb_found - both_found,
                ig_found - both_found,
                both_found,
                neither_found
            ]
        })
        st.bar_chart(coverage_data.set_index('Category'))
    
    with chart_col2:
        st.markdown("##### Status Code Distribution")
        status_counts = results_df['Status Code'].value_counts().reset_index()
        status_counts.columns = ['Status Code', 'Count']
        st.bar_chart(status_counts.set_index('Status Code'))
    
    # Detailed breakdown
    st.divider()
    st.markdown("##### 🔍 Detailed Breakdown")
    
    breakdown_col1, breakdown_col2 = st.columns(2)
    
    with breakdown_col1:
        st.markdown("**⚡ Fastest Scrapes**")
        fastest = results_df.nsmallest(5, 'Duration (s)')[['Business Name', 'Duration (s)']]
        st.dataframe(fastest, hide_index=True, use_container_width=True)
    
    with breakdown_col2:
        st.markdown("**🐌 Slowest Scrapes**")
        slowest = results_df.nlargest(5, 'Duration (s)')[['Business Name', 'Duration (s)']]
        st.dataframe(slowest, hide_index=True, use_container_width=True)

# Streamlit UI
st.set_page_config(page_title="Website Social Media Link Scraper", layout="wide")
st.title("🔍 Website Social Media Link Scraper")

# ========== OUTPUT SECTION (TOP) ==========
if st.session_state.results is not None:
    results_df = pd.DataFrame(st.session_state.results)
    
    st.success("✅ Scraping complete!")
    
    # Clear button at the top
    if st.button("🗑️ Clear Results", type="secondary"):
        st.session_state.results = None
        st.rerun()
    
    # Show Analytics Dashboard
    show_analytics(results_df)
    
    st.divider()
    st.subheader("📋 Full Results")
    
    # Display columns for results
    display_df = results_df[['Business Name', 'Website', 'Facebook', 'Instagram']]
    st.dataframe(display_df, use_container_width=True)
    
    # Expander for technical details
    with st.expander("🔧 View Technical Details"):
        st.dataframe(results_df, use_container_width=True)
    
    # Prepare CSV data
    csv = results_df[['Business Name', 'Website', 'Facebook', 'Instagram']].to_csv(index=False)
    
    # Download and Copy buttons side by side
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        st.download_button(
            label="📥 Download Results as CSV",
            data=csv,
            file_name=f"social_media_links_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True
        )
    with col_btn2:
        # Use Streamlit's built-in copy functionality
        if st.button("📋 Copy to Clipboard", type="secondary", use_container_width=True):
            st.code(csv, language=None)
            st.success("✅ CSV data displayed above - select and copy (Ctrl+C / Cmd+C)")
    
    st.divider()
    st.markdown("---")

# ========== INPUT SECTION (BOTTOM) ==========
st.markdown("""
This tool crawls websites to find their **Facebook** and **Instagram** links.
Upload a CSV with business names and website URLs, or enter URLs manually.
""")

# Tabs for different input methods
tab1, tab2 = st.tabs(["📁 Upload CSV", "✍️ Manual Input"])

businesses_to_process = []

with tab1:
    st.info("💡 Supported column names: 'Business Name', 'Company', 'Name' | 'Website URL', 'Website', 'URL', 'Link'")
    
    uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Auto-detect business name column
            name_col = None
            name_patterns = ['business name', 'business', 'name', 'company', 'company name', 'client']
            for col in df.columns:
                if col.lower().strip() in name_patterns:
                    name_col = col
                    break
            
            # Auto-detect website URL column
            url_col = None
            url_patterns = ['website url', 'website', 'url', 'web', 'site', 'link', 'webpage']
            for col in df.columns:
                if col.lower().strip() in url_patterns:
                    url_col = col
                    break
            
            if not name_col or not url_col:
                st.error(f"❌ Could not detect required columns. Found columns: {', '.join(df.columns)}")
                st.info("Please ensure your CSV has columns for business names and website URLs")
            else:
                st.success(f"✅ Loaded {len(df)} businesses")
                st.info(f"📋 Detected: **{name_col}** (names) and **{url_col}** (URLs)")
                st.dataframe(df.head())
                
                # Prepare business list using detected columns
                businesses_to_process = list(zip(df[name_col], df[url_col]))
                
        except Exception as e:
            st.error(f"Error: {str(e)}")

with tab2:
    st.markdown("Enter business information manually (one per line)")
    
    col_manual1, col_manual2 = st.columns(2)
    
    with col_manual1:
        business_names = st.text_area(
            "Business Names",
            placeholder="Example Co\nAnother Business\nThird Company",
            height=200
        )
    
    with col_manual2:
        website_urls = st.text_area(
            "Website URLs",
            placeholder="https://example.com\nhttps://another.com\nhttps://third.com",
            height=200
        )
    
    if business_names and website_urls:
        names_list = [name.strip() for name in business_names.split('\n') if name.strip()]
        urls_list = [url.strip() for url in website_urls.split('\n') if url.strip()]
        
        if len(names_list) != len(urls_list):
            st.warning(f"⚠️ Mismatch: {len(names_list)} names but {len(urls_list)} URLs. Please ensure they match.")
        else:
            st.success(f"✅ Ready to process {len(names_list)} businesses")
            businesses_to_process = list(zip(names_list, urls_list))
            
            # Preview
            preview_df = pd.DataFrame(businesses_to_process, columns=['Business Name', 'Website URL'])
            st.dataframe(preview_df, use_container_width=True)

# Start Scraping Button (works for both tabs)
if businesses_to_process:
    st.divider()
    
    if st.button("🚀 Start Scraping", type="primary", use_container_width=True):
        with st.spinner("Scraping websites..."):
            results = run_scraper(businesses_to_process)
            st.session_state.results = results
        st.rerun()

else:
    if st.session_state.results is None:
        st.info("👆 Please upload a CSV file or enter URLs manually to begin")
        
        # Sample CSV format
        with st.expander("📋 Sample CSV Format"):
            sample_df = pd.DataFrame({
                'Business Name': ['Example Co', 'Another Business'],
                'Website URL': ['https://example.com', 'https://another.com']
            })
            st.dataframe(sample_df)
            st.download_button(
                "Download Sample CSV",
                sample_df.to_csv(index=False),
                "sample_format.csv",
                "text/csv"
            )
