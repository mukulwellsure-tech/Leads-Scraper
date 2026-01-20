import time
import random
import re
import os
from datetime import datetime
from multiprocessing import Process, Queue, current_process

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
OUTPUT_FILE = "Trademark_Sellers_All.xlsx"
SAVE_EVERY = 20
NUM_BROWSERS = 3

WAIT_MIN = 0.8
WAIT_MAX = 1.6
HEADLESS = False   # ❌ keep False for safety in parallel

# =====================================================
# RAJASTHAN DISTRICTS (CLEANED)
# =====================================================
RAJASTHAN_DISTRICTS = [
    "Ajmer","Alwar","Anupgarh","Balotra","Banswara","Baran","Barmer",
    "Beawar","Bharatpur","Bhilwara","Bikaner","Bundi","Chittorgarh",
    "Churu","Dausa","Deeg","Didwana Kuchaman","Dholpur","Dungarpur",
    "Dudu","Gangapur City","Hanumangarh","Jaipur","Jaisalmer","Jalore",
    "Jhalawar","Jhunjhunu","Jodhpur","Karauli","Kekri","Khairthal Tijara",
    "Kota","Kotputli Behror","Nagaur","Neem Ka Thana","Pali","Phalodi",
    "Pratapgarh","Rajsamand","Salumbar","Sanchore","Sawai Madhopur",
    "Shahpura","Sikar","Sirohi","Sri Ganganagar","Tonk","Udaipur"
]

KEYWORDS = [
    "MRF dealer",
    "MRF tyre dealer",
    "MRF authorized dealer"
]

# =====================================================
# DRIVER SETUP (CRASH SAFE)
# =====================================================
def setup_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

# =====================================================
# SCROLL UNTIL EXHAUSTED
# =====================================================
def scroll_feed(driver):
    last_height = 0
    retries = 0
    while retries < 3:
        driver.execute_script("""
            const feed = document.querySelector('div[role="feed"]');
            if (feed) feed.scrollTop = feed.scrollHeight;
        """)
        time.sleep(1.2)
        height = driver.execute_script("""
            const feed = document.querySelector('div[role="feed"]');
            return feed ? feed.scrollHeight : 0;
        """)
        if height == last_height:
            retries += 1
        else:
            retries = 0
        last_height = height

# =====================================================
# SAVE PROGRESS (GLOBAL SAFE)
# =====================================================
def save_progress(data):
    if not data:
        return

    df_new = pd.DataFrame(data)

    if os.path.exists(OUTPUT_FILE):
        df_old = pd.read_excel(OUTPUT_FILE)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df["Phone"] = df["Phone"].fillna("").str.strip()
    df["Brand_Name"] = df["Brand_Name"].fillna("").str.strip()

    df["dedupe_key"] = df["Phone"]
    df.loc[df["dedupe_key"] == "", "dedupe_key"] = df["Brand_Name"]

    df.drop_duplicates(subset=["dedupe_key"], inplace=True)
    df.drop(columns=["dedupe_key"], inplace=True)

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"💾 Saved total: {len(df)} unique records")


# =====================================================
# SCRAPER CORE
# =====================================================
def scrape_query(driver, query, buffer):
    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
    )

    seen = set()

    while True:
        listings = driver.find_elements(By.XPATH, '//div[@role="article"]')
        for item in listings:
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", item)
                time.sleep(0.5)
                item.click()

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
                )

                name = driver.find_element(By.CLASS_NAME, "DUwDvf").text
                if name in seen:
                    continue
                seen.add(name)

                def safe(x):
                    try:
                        return driver.find_element(By.XPATH, x).text
                    except:
                        return ""

                phone = re.sub(r"[^\d+]", "", safe("//button[contains(@aria-label,'Phone')]"))
                website = ""
                try:
                    website = driver.find_element(By.XPATH, "//a[contains(@aria-label,'Website')]").get_attribute("href")
                except:
                    pass

                buffer.append({
                    "Brand_Name": name,
                    "Phone": phone,
                    "Website": website,
                    "Query": query,
                    "Source": "Google Maps",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                if len(buffer) % SAVE_EVERY == 0:
                    save_progress(buffer)
                    buffer.clear()

                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except StaleElementReferenceException:
                continue
            except:
                continue

        old_len = len(listings)
        scroll_feed(driver)
        if len(driver.find_elements(By.XPATH, '//div[@role="article"]')) == old_len:
            break

# =====================================================
# WORKER (AUTO RESTART)
# =====================================================
def worker(queries):
    buffer = []
    while True:
        try:
            driver = setup_driver()
            for q in queries:
                print(f"[{current_process().name}] 🔍 {q}")
                scrape_query(driver, q, buffer)
            driver.quit()
            break
        except WebDriverException:
            print(f"[{current_process().name}] 🔁 Chrome crashed, restarting...")
            time.sleep(5)

    if buffer:
        save_progress(buffer)

# =====================================================
# MAIN (PARALLEL EXECUTION)
# =====================================================
def main():
    all_queries = []
    for d in RAJASTHAN_DISTRICTS:
        for k in KEYWORDS:
            all_queries.append(f"{k} {d}")

    chunk_size = len(all_queries) // NUM_BROWSERS + 1
    chunks = [all_queries[i:i + chunk_size] for i in range(0, len(all_queries), chunk_size)]

    processes = []
    for chunk in chunks[:NUM_BROWSERS]:
        p = Process(target=worker, args=(chunk,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("🏁 ALL DISTRICTS SCRAPED SUCCESSFULLY")

if __name__ == "__main__":
    main()
