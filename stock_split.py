import requests
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent
import sys

def scrape_investing_splits(symbol: str) -> pd.DataFrame:
    """
    Scrapes the stock split history table from Investing.com.
    """
    base_url = "https://www.investing.com/equities/"
    url = f"{base_url}{symbol}-historical-data-splits"
    
    ua = UserAgent()
    # 🎯 IMPROVEMENT: Added more headers to mimic a real browser session
    headers = {
        'User-Agent': ua.random,
        'Accept-Language': 'en-US,en;q=0.9', # Changed q-value slightly
        'Accept-Encoding': 'gzip, deflate, br', # Tells the server what compression we accept
        'Referer': f'{base_url}{symbol}-historical-data', # Mimic navigation from the main historical page
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        # Often required to confirm user consent or bot check
        'Cookie': 'consent=agree; market_cookies_checked=true;', 
    }

    print(f"Fetching data from: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # Check status code immediately after request
        if response.status_code == 403:
             print(f"❌ Error: Received 403 Forbidden. The site has blocked the request.")
             print("   Try running the script again, as the User-Agent might be the issue.")
             return pd.DataFrame()
             
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching the URL: {e}")
        return pd.DataFrame()

    # --- Rest of the parsing logic remains the same ---
    soup = BeautifulSoup(response.content, 'html.parser')
    
    splits_container = soup.find('div', id='splitsHistory')
    
    if not splits_container:
        print("❌ Error: Could not find the main split history container ('splitsHistory').")
        return pd.DataFrame()

    split_table = splits_container.find('table', class_='datatable')

    if not split_table:
        print("❌ Error: Found container, but could not find the split history table ('datatable') inside.")
        return pd.DataFrame()

    # 1. Extract headers (Date, Ratio, etc.)
    headers_list = [th.text.strip() for th in split_table.find('thead').find_all('th')]
    
    # 2. Extract data rows
    data_rows = []
    for row in split_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if cells:
            data_rows.append([cell.text.strip() for cell in cells])

    # 3. Create and clean the DataFrame
    df = pd.DataFrame(data_rows, columns=headers_list)
    
    # Clean up column names and data types
    df = df.rename(columns={'Split Ratio': 'Ratio', 'Ex-Date': 'Date'})
    
    if 'Date' in df.columns:
        try:
            # Use explicit format for reliability
            df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce') 
            df = df.set_index('Date').sort_index(ascending=False)
            df = df.dropna(subset=['Ratio'])
        except ValueError as e:
            print(f"❌ Date Parsing Error: {e}. Returning raw table.")
            return pd.DataFrame()
    
    return df

# --- Execution ---

if __name__ == "__main__":
    NVDA_SYMBOL = 'nvidia-corp'

    df_splits = scrape_investing_splits(NVDA_SYMBOL)

    if not df_splits.empty:
        print("\n✅ Successfully scraped NVIDIA Stock Split Data:")
        print(df_splits)
    else:
        print("\n❌ Final DataFrame is empty.")