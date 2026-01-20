import time
import random
import re
import os
from datetime import datetime
from multiprocessing import Process

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
NUM_BROWSERS = 3
HEADLESS = False
SAVE_EVERY = 20

WAIT_MIN = 1.0
WAIT_MAX = 2.0

QUERY_COOLDOWN = (5, 10)
CRASH_COOLDOWN = (15, 25)

FINAL_OUTPUT = "Trademark_Sellers_All.xlsx"

# =====================================================
# RAJASTHAN DISTRICTS
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
# DRIVER SETUP (WINDOWS SAFE)
# =====================================================
def setup_driver(worker_id):
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=0")

    if HEADLESS:
        options.add_argument("--headless=new")

    for _ in range(3):
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            return driver
        except Exception:
            time.sleep(5)

    raise RuntimeError("❌ Chrome could not start")

# =====================================================
# END OF LIST DETECTION (CRITICAL FIX)
# =====================================================
def reached_end_of_list(driver):
    try:
        return "You've reached the end of the list" in driver.page_source
    except:
        return False

# =====================================================
# SCRAPE ONE QUERY SAFELY
# =====================================================
def scrape_query(driver, query, results):
    print(f"🔍 Searching: {query}")
    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
        )
    except TimeoutException:
        return

    seen = set()
    stall = 0

    while True:
        if reached_end_of_list(driver):
            print("🛑 End of list reached")
            break

        cards = driver.find_elements(By.XPATH, '//div[@role="article"]')
        if not cards:
            stall += 1
            if stall >= 3:
                break
        else:
            stall = 0

        for card in cards:
            try:
                driver.execute_script("arguments[0].click();", card)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
                )

                name = driver.find_element(By.CLASS_NAME, "DUwDvf").text.strip()
                if not name or name in seen:
                    continue
                seen.add(name)

                phone = ""
                try:
                    phone = re.sub(
                        r"[^\d+]",
                        "",
                        driver.find_element(
                            By.XPATH,
                            "//button[contains(@aria-label,'Call') or contains(@aria-label,'Phone')]"
                        ).text
                    )
                except:
                    pass

                if len(phone) < 10:
                    continue

                results.append({
                    "Brand_Name": name,
                    "Phone": phone,
                    "Query": query,
                    "Source": "Google Maps",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                if len(results) % SAVE_EVERY == 0:
                    save_partial(results)

                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except StaleElementReferenceException:
                continue
            except TimeoutException:
                continue

        driver.execute_script("""
            const feed = document.querySelector('div[role="feed"]');
            if (feed) feed.scrollTop = feed.scrollHeight;
        """)
        time.sleep(1.5)

# =====================================================
# SAVE PARTIAL (DEDUP SAFE)
# =====================================================
def save_partial(data):
    df_new = pd.DataFrame(data)

    if os.path.exists(FINAL_OUTPUT):
        df_old = pd.read_excel(FINAL_OUTPUT)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df["dedupe"] = df["Phone"] + "|" + df["Brand_Name"]
    df.drop_duplicates("dedupe", inplace=True)
    df.drop(columns=["dedupe"], inplace=True)

    df.to_excel(FINAL_OUTPUT, index=False)
    print(f"💾 Saved {len(df)} total records")

# =====================================================
# WORKER PROCESS
# =====================================================
def worker(worker_id, queries):
    time.sleep(worker_id * 5)
    driver = setup_driver(worker_id)
    results = []

    for q in queries:
        try:
            scrape_query(driver, q, results)
            time.sleep(random.uniform(*QUERY_COOLDOWN))
        except WebDriverException:
            print(f"⚠️ Chrome crash detected. Restarting...")
            try:
                driver.quit()
            except:
                pass
            time.sleep(random.uniform(*CRASH_COOLDOWN))
            driver = setup_driver(worker_id)

    driver.quit()
    save_partial(results)

# =====================================================
# MAIN
# =====================================================
def main():
    all_queries = [f"{k} {d}" for d in RAJASTHAN_DISTRICTS for k in KEYWORDS]
    chunk = len(all_queries) // NUM_BROWSERS + 1
    chunks = [all_queries[i:i + chunk] for i in range(0, len(all_queries), chunk)]

    procs = []
    for i, c in enumerate(chunks[:NUM_BROWSERS], 1):
        p = Process(target=worker, args=(i, c))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print("🏁 SCRAPING COMPLETE")

# =====================================================
# WINDOWS ENTRY POINT
# =====================================================
if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
