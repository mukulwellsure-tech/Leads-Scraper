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
# DRIVER SETUP
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
# SAFE SCROLL
# =====================================================
def scroll_results(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1.2)

# =====================================================
# SCRAPE ONE QUERY
# =====================================================
def scrape_query(driver, query, results):
    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
    )

    seen_names = set()
    no_new_rounds = 0

    while no_new_rounds < 3:
        listings = driver.find_elements(By.XPATH, '//div[@role="article"]')
        new_found = 0

        for item in listings:
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", item)
                driver.execute_script("arguments[0].click();", item)

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
                )

                name = driver.find_element(By.CLASS_NAME, "DUwDvf").text.strip()
                if not name or name in seen_names:
                    continue

                seen_names.add(name)

                def safe(xpath):
                    try:
                        return driver.find_element(By.XPATH, xpath).text
                    except:
                        return ""

                phone = re.sub(
                    r"[^\d+]",
                    "",
                    safe("//button[contains(@aria-label,'Call') or contains(@aria-label,'Phone')]")
                )

                # 🚫 HARD FILTER: NO PHONE = SKIP
                if not phone or len(phone) < 10:
                    continue

                website = ""
                try:
                    website = driver.find_element(
                        By.XPATH, "//a[contains(@aria-label,'Website')]"
                    ).get_attribute("href")
                except:
                    pass

                results.append({
                    "Brand_Name": name,
                    "Phone": phone,
                    "Website": website,
                    "Query": query,
                    "Source": "Google Maps",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                new_found += 1
                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except StaleElementReferenceException:
                continue

        if new_found == 0:
            no_new_rounds += 1
        else:
            no_new_rounds = 0

        scroll_results(driver)

# =====================================================
# WORKER PROCESS
# =====================================================
def worker(worker_id, queries):
    output_file = f"output_part_{worker_id}.xlsx"
    results = []

    driver = setup_driver()

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
                driver = setup_driver()

    driver.quit()

    if results:
        df = pd.DataFrame(results)
        df.to_excel(output_file, index=False)
        print(f"[Process-{worker_id}] 💾 Saved {len(df)} VALID leads")

# =====================================================
# MERGE & DEDUPE
# =====================================================
def merge_outputs():
    dfs = []
    for i in range(1, NUM_BROWSERS + 1):
        f = f"output_part_{i}.xlsx"
        if os.path.exists(f):
            dfs.append(pd.read_excel(f))

    if not dfs:
        print("❌ No data to merge")
        return

    df = pd.concat(dfs, ignore_index=True)

    df["Phone"] = df["Phone"].astype(str)
    df["Brand_Name"] = df["Brand_Name"].fillna("")

    df["dedupe_key"] = df["Phone"] + "|" + df["Brand_Name"]
    df.drop_duplicates(subset=["dedupe_key"], inplace=True)
    df.drop(columns=["dedupe_key"], inplace=True)

    df.to_excel(FINAL_OUTPUT, index=False)
    print(f"🏁 FINAL FILE SAVED: {len(df)} CALLABLE LEADS")

# =====================================================
# MAIN
# =====================================================
def main():
    all_queries = [f"{k} {d}" for d in RAJASTHAN_DISTRICTS for k in KEYWORDS]

    chunk_size = len(all_queries) // NUM_BROWSERS + 1
    chunks = [all_queries[i:i + chunk_size] for i in range(0, len(all_queries), chunk_size)]

    processes = []
    for i, chunk in enumerate(chunks[:NUM_BROWSERS], start=1):
        p = Process(target=worker, args=(i, chunk))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    merge_outputs()

if __name__ == "__main__":
    main()
