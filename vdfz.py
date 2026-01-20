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
from selenium.common.exceptions import WebDriverException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
NUM_BROWSERS = 3
HEADLESS = False

WAIT_MIN = 1.0
WAIT_MAX = 2.0
QUERY_COOLDOWN = (6, 12)
CRASH_COOLDOWN = (20, 30)

FINAL_OUTPUT = "Trademark_Sellers_All.xlsx"

# =====================================================
# INSTALL DRIVER ONCE (CRITICAL)
# =====================================================
CHROMEDRIVER_PATH = ChromeDriverManager().install()

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
# DRIVER SETUP (PROCESS SAFE)
# =====================================================
def setup_driver(worker_id):
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument(f"--user-data-dir=chrome_profile_{worker_id}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    if HEADLESS:
        options.add_argument("--headless=new")

    service = Service(CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)

# =====================================================
# SCRAPE ONE QUERY
# =====================================================
def scrape_query(driver, query, results):
    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")

    WebDriverWait(driver, 25).until(
        EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
    )

    seen = set()
    stall = 0

    while stall < 3:
        cards = driver.find_elements(By.XPATH, '//div[@role="article"]')
        found = 0

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

                if not phone or len(phone) < 10:
                    continue

                results.append({
                    "Brand_Name": name,
                    "Phone": phone,
                    "Query": query,
                    "Source": "Google Maps",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                found += 1
                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except StaleElementReferenceException:
                continue

        if found == 0:
            stall += 1
        else:
            stall = 0

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.2)

# =====================================================
# WORKER
# =====================================================
def worker(worker_id, queries):
    time.sleep(worker_id * 5)  # 🔑 stagger startup

    output_file = f"output_part_{worker_id}.xlsx"
    results = []

    driver = setup_driver(worker_id)

    for q in queries:
        print(f"[Process-{worker_id}] 🔍 {q}")
        while True:
            try:
                scrape_query(driver, q, results)
                time.sleep(random.uniform(*QUERY_COOLDOWN))
                break
            except WebDriverException:
                print(f"[Process-{worker_id}] 🔁 Chrome crashed, restarting...")
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(random.uniform(*CRASH_COOLDOWN))
                driver = setup_driver(worker_id)

    driver.quit()

    if results:
        pd.DataFrame(results).to_excel(output_file, index=False)
        print(f"[Process-{worker_id}] 💾 Saved {len(results)} leads")

# =====================================================
# MERGE
# =====================================================
def merge_outputs():
    dfs = []
    for i in range(1, NUM_BROWSERS + 1):
        f = f"output_part_{i}.xlsx"
        if os.path.exists(f):
            dfs.append(pd.read_excel(f))

    if not dfs:
        return

    df = pd.concat(dfs, ignore_index=True)
    df["dedupe"] = df["Phone"] + "|" + df["Brand_Name"]
    df.drop_duplicates("dedupe", inplace=True)
    df.drop(columns=["dedupe"], inplace=True)
    df.to_excel(FINAL_OUTPUT, index=False)

    print(f"🏁 FINAL SAVED: {len(df)} CALLABLE LEADS")

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

    merge_outputs()

if __name__ == "__main__":
    main()
