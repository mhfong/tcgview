import asyncio
import nest_asyncio
import re
from datetime import datetime
from pyppeteer import launch
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
    try:
        browser = await launch(
            headless=True,
            executablePath='/usr/bin/chromium-browser',
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False
        )
        # Rest of the function
    except Exception as e:
        print(f"Failed to launch browser: {e}")
        return None
    finally:
        if 'browser' in locals():
            await browser.close()
    try:
        page = await browser.newPage()
        await page.goto(f'https://yuyu-tei.jp/sell/{tcg_type}/card/{card_set}/{i}', timeout=60000)
        await page.waitForSelector('.fw-bold', timeout=60000)
        print(f'https://yuyu-tei.jp/sell/{tcg_type}/card/{card_set}/{i}')
        fw_bold_texts = await page.evaluate('''() => {
            const boldElements = document.querySelectorAll('.fw-bold');
            return Array.from(boldElements).map(element => element.innerText).join('\\n');
        }''')
        return fw_bold_texts
    except Exception as e:
        print(f"Error for {card_set}/{i}: {e}")
        return None
    finally:
        await browser.close()

async def get_ptcg_links(vers, rarity):
    try:
        browser = await launch(
            headless=True,
            executablePath='/usr/bin/chromium-browser',
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False
        )
        # Rest of the function
    except Exception as e:
        print(f"Failed to launch browser: {e}")
        return None
    finally:
        if 'browser' in locals():
            await browser.close()
    try:
        page = await browser.newPage()
        await page.goto(f'https://yuyu-tei.jp/sell/poc/s/search?search_word={vers}&rare={rarity}&type=&kizu=0', timeout=60000)
        hyperlinks = await page.evaluate('''() => {
            const links = document.querySelectorAll('a');
            return Array.from(links).map(link => link.href);
        }''')
        return hyperlinks
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        await browser.close()

async def get_opcg_links(search_word, rarity):
    try:
        browser = await launch(
            headless=True,
            executablePath='/usr/bin/chromium-browser',
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False
        )
        # Rest of the function
    except Exception as e:
        print(f"Failed to launch browser: {e}")
        return None
    finally:
        if 'browser' in locals():
            await browser.close()
    try:
        page = await browser.newPage()
        await page.goto(f'https://yuyu-tei.jp/sell/opc/s/search?search_word={search_word}&rare={rarity}&type=&kizu=0', timeout=60000)
        hyperlinks = await page.evaluate('''() => {
            const links = document.querySelectorAll('a');
            return Array.from(links).map(link => link.href);
        }''')
        return hyperlinks
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        await browser.close()

async def scrape_ptcg():
    # ptcg_rarity_table = {
    #     'UR': ['sv10','sv09a','sv09','sv08a','sv08','sv07a','sv07','sv06a','sv06','sv05a','sv05k','sv05m','sv04a','sv04k','sv04m','sv03a','sv03','sv02a','sv02p','sv02d','sv01a','sv01s','sv01v','s12a'],
    #     'SAR': ['sv10','sv09a','sv09','sv08a','sv08','sv07a','sv07','sv06a','sv06','sv05a','sv05k','sv05m','sv04a','sv04k','sv04m','sv03a','sv03','sv02a','sv02p','sv02d','sv01a','sv01s','sv01v','s12a'],
    #     'SR': ['sv10','sv09a','sv09','sv08a','sv08','sv07a','sv07','sv06a','sv06','sv05a','sv05k','sv05m','sv04a','sv04k','sv04m','sv03a','sv03','sv02a','sv02p','sv02d','sv01a','sv01s','sv01v','s12a'],
    #     'AR': ['sv10','sv09a','sv09','sv08a','sv08','sv07a','sv07','sv06a','sv06','sv05a','sv05k','sv05m','sv04a','sv04k','sv04m','sv03a','sv03','sv02a','sv02p','sv02d','sv01a','sv01s','sv01v','s12a'],
    #     'S-TD': ['svg']
    # }

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

    links = list(set(links))
    sorted_links = sorted(links, key=lambda x: (x.split('/card/')[1].split('/')[0], int(x.split('/')[-1])))

    pkm_df = pd.DataFrame(columns=['card_set', 'card_rarity', 'card_name', 'card_index', 'card_price', 'created_time'])
    
    for idx, link in enumerate(sorted_links, 1):
        tcg_type = link.split('/')[-4]
        card_set = link.split('/')[-2]
        i = link.split('/')[-1]
        print(f'{idx}/{len(sorted_links)}')
        content = await extract_content(tcg_type, card_set, i)
        if content:
            try:
                card_rarity, card_name = extract_ptcg_rarity_and_card_name(content)
            except:
                continue
            card_index = extract_ptcg_card_index(content)
            card_price = extract_card_price(content)
            created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pkm_df.loc[len(pkm_df)] = [card_set, card_rarity, card_name, card_index, card_price, created_time]

    pkm_df.to_csv(f'{ptcg_result_path}/{datetime.now().strftime("%Y%m%d")}.csv', index=False, encoding='utf-8-sig')
    return pkm_df

async def scrape_opcg():
    # rarities = ['P-SEC', 'SEC', 'P-SR', 'P-R', 'P-L', 'SP', '-']
    
    rarities = ['P-SEC']

    links = []
    for rarity in rarities:
        search_word = 'スーパーパラレル' if rarity == '-' else ''
        links += await get_opcg_links(search_word, rarity)
    
    links = list(set(sorted([l for l in links if 'card' in l])))
    sorted_links = sorted(links, key=lambda x: (x.split('/card/')[1].split('/')[0], int(x.split('/')[-1])))

    op_df = pd.DataFrame(columns=['card_set', 'card_rarity', 'card_name', 'card_index', 'card_price', 'created_time'])

    for idx, link in enumerate(sorted_links, 1):
        tcg_type = link.split('/')[-4]
        card_set = link.split('/')[-2]
        i = link.split('/')[-1]
        print(f'{idx}/{len(sorted_links)}')
        content = await extract_content(tcg_type, card_set, i)
        if content:
            try:
                card_rarity, card_name = extract_opcg_rarity_and_card_name(content)
            except:
                continue
            card_index = extract_opcg_card_index(content)
            card_price = extract_card_price(content)
            created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            op_df.loc[len(op_df)] = [card_set, card_rarity, card_name, card_index, card_price, created_time]

    op_df.to_csv(f'{opcg_result_path}/{datetime.now().strftime("%Y%m%d")}.csv', index=False, encoding='utf-8-sig')
    return op_df

async def main():
    print("Scraping PTCG data...")
    await scrape_ptcg()
    print("Scraping OPCG data...")
    await scrape_opcg()

if __name__ == "__main__":
    asyncio.run(main())