import asyncio
import nest_asyncio
import re, os
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd
from path import *

nest_asyncio.apply()

def extract_ptcg_rarity_and_card_name(content):
    pattern = r'(?:S-TD|UR|AR|SR|SAR)\s+[^\s\n]+'
    match = re.search(pattern, content)
    if match:
        return match.group().split(' ')[0], match.group().split(' ')[1]
    return None

def extract_opcg_rarity_and_card_name(content):
    pattern = r'(?:P-SEC|SEC|P-SR|P-R|P-L|SP|-)\s+[^\s\n]+'
    matches = re.search(pattern, content)
    if matches:
        matches = matches.group()
        if matches.split(' ')[0] == '-':
            return 'DON', matches.split(' ')[1]
        return matches.split(' ')[0], matches.split(' ')[1]
    return None

def extract_ptcg_card_index(content):
    pattern = r'\d{3}/\d{3}'
    match = re.search(pattern, content)
    if match:
        return match.group()
    return None

def extract_opcg_card_index(content):
    pattern = r'(?:OP|EB|ST)\d{2}-\d{3}'
    match = re.search(pattern, content)
    if match:
        return match.group()
    return None

def extract_card_price(content):
    pattern = r'\d{1,3}(?:,\d{3})* 円'
    match = re.search(pattern, content)
    if match:
        price_str = match.group()
        return int(price_str.replace(',', '').replace(' 円', ''))
    return None

async def extract_content(tcg_type, card_set, i):
    print(f"Extracting content for {tcg_type}/{card_set}/{i}")
    for attempt in range(3):
        print(f"Attempt {attempt+1}/3")
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage']
                )
                page = await browser.new_page()
                await page.goto(f'https://yuyu-tei.jp/sell/{tcg_type}/card/{card_set}/{i}', timeout=60000)
                await page.wait_for_selector('.fw-bold', timeout=60000)
                print(f"Page loaded: https://yuyu-tei.jp/sell/{tcg_type}/card/{card_set}/{i}")
                fw_bold_texts = await page.evaluate('''() => {
                    const boldElements = document.querySelectorAll('.fw-bold');
                    return Array.from(boldElements).map(element => element.innerText).join('\\n');
                }''')
                return fw_bold_texts
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(5)
            continue
    print("All attempts to extract content failed")
    return None

async def get_ptcg_links(vers, rarity):
    print(f"Launching browser for PTCG links: {vers}, {rarity}")
    for attempt in range(3):
        print(f"Attempt {attempt+1}/3")
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage']
                )
                page = await browser.new_page()
                await page.goto(f'https://yuyu-tei.jp/sell/poc/s/search?search_word={vers}&rare={rarity}&type=&kizu=0', timeout=60000)
                hyperlinks = await page.evaluate('''() => {
                    const links = document.querySelectorAll('a');
                    return Array.from(links).map(link => link.href);
                }''')
                return hyperlinks
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(5)
            continue
    print("All attempts to launch browser failed")
    return []

async def get_opcg_links(search_word, rarity):
    print(f"Launching browser for OPCG links: {search_word}, {rarity}")
    for attempt in range(3):
        print(f"Attempt {attempt+1}/3")
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage']
                )
                page = await browser.new_page()
                await page.goto(f'https://yuyu-tei.jp/sell/opc/s/search?search_word={search_word}&rare={rarity}&type=&kizu=0', timeout=60000)
                hyperlinks = await page.evaluate('''() => {
                    const links = document.querySelectorAll('a');
                    return Array.from(links).map(link => link.href);
                }''')
                return hyperlinks
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(5)
            continue
    print("All attempts to launch browser failed")
    return []

async def scrape_ptcg():
    ptcg_rarity_table = {
        'UR': ['sv10']
    }

    links = []
    for rarity in ptcg_rarity_table.keys():
        vers = ''.join('&vers%5B%5D=' + i for i in ptcg_rarity_table[rarity])
        all_links = await get_ptcg_links(vers, rarity)
        if all_links:
            cleaned_links = [url for url in all_links if any(val in url for val in ptcg_rarity_table[rarity]) and 'card' in url]
            links += cleaned_links
        else:
            print(f"No links found for rarity {rarity}")

    links = list(set(links))
    sorted_links = sorted(links, key=lambda x: (x.split('/card/')[1].split('/')[0], int(x.split('/')[-1])))
    print(f"Collected {len(links)} PTCG links")

    pkm_df = pd.DataFrame(columns=['card_set', 'card_rarity', 'card_name', 'card_index', 'card_price', 'created_time'])
    
    for idx, link in enumerate(sorted_links, 1):
        tcg_type = link.split('/')[-4]
        card_set = link.split('/')[-2]
        i = link.split('/')[-1]
        print(f'Processing {idx}/{len(sorted_links)}')
        content = await extract_content(tcg_type, card_set, i)
        if content:
            try:
                card_rarity, card_name = extract_ptcg_rarity_and_card_name(content)
            except:
                print(f"Failed to extract rarity/name for {link}")
                continue
            card_index = extract_ptcg_card_index(content)
            card_price = extract_card_price(content)
            created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pkm_df.loc[len(pkm_df)] = [card_set, card_rarity, card_name, card_index, card_price, created_time]

    os.makedirs(ptcg_result_path, exist_ok=True)
    
    if pkm_df.empty:
        print("No PTCG data collected, skipping CSV save")
        return pkm_df
    
    csv_path = f'{ptcg_result_path}/{datetime.now().strftime("%Y%m%d")}.csv'
    print(f"Saving PTCG data to {csv_path}")
    pkm_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Saved {len(pkm_df)} rows to {csv_path}")
    return pkm_df

async def scrape_opcg():
    rarities = ['P-SEC']

    links = []
    for rarity in rarities:
        search_word = 'スーパーパラレル' if rarity == '-' else ''
        all_links = await get_opcg_links(search_word, rarity)
        if all_links:
            links += all_links
        else:
            print(f"No links found for rarity {rarity}")
    
    links = list(set(sorted([l for l in links if 'card' in l])))
    sorted_links = sorted(links, key=lambda x: (x.split('/card/')[1].split('/')[0], int(x.split('/')[-1])))
    print(f"Collected {len(links)} OPCG links")

    op_df = pd.DataFrame(columns=['card_set', 'card_rarity', 'card_name', 'card_index', 'card_price', 'created_time'])

    for idx, link in enumerate(sorted_links, 1):
        tcg_type = link.split('/')[-4]
        card_set = link.split('/')[-2]
        i = link.split('/')[-1]
        print(f'Processing {idx}/{len(sorted_links)}')
        content = await extract_content(tcg_type, card_set, i)
        if content:
            try:
                card_rarity, card_name = extract_opcg_rarity_and_card_name(content)
            except:
                print(f"Failed to extract rarity/name for {link}")
                continue
            card_index = extract_opcg_card_index(content)
            card_price = extract_card_price(content)
            created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            op_df.loc[len(op_df)] = [card_set, card_rarity, card_name, card_index, card_price, created_time]

    os.makedirs(opcg_result_path, exist_ok=True)
    
    if op_df.empty:
        print("No OPCG data collected, skipping CSV save")
        return op_df
    
    csv_path = f'{opcg_result_path}/{datetime.now().strftime("%Y%m%d")}.csv'
    print(f"Saving OPCG data to {csv_path}")
    op_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Saved {len(op_df)} rows to {csv_path}")
    return op_df

async def main():
    print(f"Starting TCG scraper at {datetime.now()}")
    print("Scraping PTCG data...")
    ptcg_df = await scrape_ptcg()
    print(f"PTCG scraping completed: {len(ptcg_df)} rows")
    print("Scraping OPCG data...")
    opcg_df = await scrape_opcg()
    print(f"OPCG scraping completed: {len(opcg_df)} rows")

if __name__ == "__main__":
    asyncio.run(main())