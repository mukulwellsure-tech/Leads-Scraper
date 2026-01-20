import os, time, random, re, sqlite3
from datetime import datetime
from multiprocessing import Process, Lock

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# =========================
# CONFIG
# =========================
DB_FILE = "leads.db"
NUM_BROWSERS = max(2, os.cpu_count() - 1)

WAIT_MIN, WAIT_MAX = 0.3, 0.7
QUERY_COOLDOWN = (2, 4)
CRASH_COOLDOWN = (10, 15)

# =========================
# DATA
# =========================
RAJASTHAN_DISTRICTS = [
    "Ajmer","Alwar","Bharatpur","Bhilwara","Bikaner","Jaipur",
    "Jodhpur","Kota","Sikar","Udaipur"
]

KEYWORDS = [
    "MRF dealer",
    "MRF tyre dealer",
    "MRF authorized dealer"
]

# =========================
# DATABASE
# =========================
db_lock = Lock()

def init_db():
    with sqlite3.connect(DB_FILE) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                phone TEXT,
                name TEXT,
                query TEXT,
                scraped_at TEXT,
                UNIQUE(phone, name)
            )
        """)

def save_lead(name, phone, query):
    with db_lock:
        with sqlite3.connect(DB_FILE) as con:
            con.execute("""
                INSERT OR IGNORE INTO leads VALUES (?,?,?,?)
            """, (phone, name, query, datetime.now().isoformat()))

# =========================
# DRIVER
# =========================
def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service("/opt/homebrew/bin/chromedriver")
    return webdriver.Chrome(service=service, options=options)

# =========================
# PHONE EXTRACTION
# =========================
def extract_phone(driver):
    candidates = []

    # button text
    for b in driver.find_elements(By.XPATH, "//button"):
        t = b.text.strip()
        if re.search(r"\d{10}", t):
            candidates.append(t)

        aria = b.get_attribute("aria-label") or ""
        if re.search(r"\d{10}", aria):
            candidates.append(aria)

    # tel links
    for a in driver.find_elements(By.XPATH, "//a[contains(@href,'tel:')]"):
        candidates.append(a.get_attribute("href"))

    # page source regex
    candidates += re.findall(r"\+?\d[\d\s\-]{9,14}", driver.page_source)

    for c in candidates:
        phone = re.sub(r"[^\d+]", "", c)
        if len(phone) >= 10:
            return phone
    return ""

# =========================
# SCRAPER
# =========================
def scrape_query(driver, query):
    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
        )
    except TimeoutException:
        return

    seen = set()
    last_count = 0
    stall = 0

    while True:
        cards = driver.find_elements(By.XPATH, '//div[@role="article"]')

        if len(cards) == last_count:
            stall += 1
        else:
            stall = 0

        if stall >= 3:
            break

        last_count = len(cards)

        for card in cards:
            try:
                driver.execute_script("arguments[0].click();", card)
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
                )

                name = driver.find_element(By.CLASS_NAME, "DUwDvf").text.strip()
                if not name or name in seen:
                    continue

                seen.add(name)
                phone = extract_phone(driver)

                if phone:
                    save_lead(name, phone, query)

                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except Exception:
                continue

        driver.execute_script("""
            const feed = document.querySelector('div[role="feed"]');
            if (feed) feed.scrollTop = feed.scrollHeight;
        """)
        time.sleep(1)

# =========================
# WORKER
# =========================
def worker(queries):
    driver = setup_driver()

    for q in queries:
        try:
            scrape_query(driver, q)
            time.sleep(random.uniform(*QUERY_COOLDOWN))
        except WebDriverException:
            try: driver.quit()
            except: pass
            time.sleep(random.uniform(*CRASH_COOLDOWN))
            driver = setup_driver()

    driver.quit()

# =========================
# MAIN
# =========================
def main():
    init_db()
    all_queries = [f"{k} {d}" for d in RAJASTHAN_DISTRICTS for k in KEYWORDS]
    chunk = len(all_queries) // NUM_BROWSERS + 1
    chunks = [all_queries[i:i+chunk] for i in range(0, len(all_queries), chunk)]

    procs = []
    for c in chunks[:NUM_BROWSERS]:
        p = Process(target=worker, args=(c,))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print("🔥 SCRAPING COMPLETE")

if __name__ == "__main__":
    main()
