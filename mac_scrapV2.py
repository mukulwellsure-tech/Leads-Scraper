import os, re, time, random, sqlite3
from datetime import datetime
from multiprocessing import Process, Lock, cpu_count

from playwright.sync_api import sync_playwright, TimeoutError

# =========================
# CONFIG
# =========================
DB_FILE = "leads.db"
NUM_WORKERS = max(2, cpu_count() - 1)

WAIT_MIN, WAIT_MAX = 0.2, 0.5
QUERY_COOLDOWN = (1.5, 3)
CRASH_COOLDOWN = (8, 12)

# =========================
# DATA
# =========================
RAJASTHAN_DISTRICTS = [
    "Ajmer","Alwar","Bharatpur","Bhilwara","Bikaner",
    "Jaipur","Jodhpur","Kota","Sikar","Udaipur"
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
            con.execute(
                "INSERT OR IGNORE INTO leads VALUES (?,?,?,?)",
                (phone, name, query, datetime.now().isoformat())
            )

# =========================
# PHONE EXTRACTION (STRONG)
# =========================
def extract_phone(page):
    candidates = []

    # buttons
    for btn in page.locator("button").all():
        t = btn.inner_text(timeout=100) or ""
        aria = btn.get_attribute("aria-label") or ""
        candidates += [t, aria]

    # tel links
    for a in page.locator("a[href^='tel:']").all():
        candidates.append(a.get_attribute("href"))

    # regex from page source
    candidates += re.findall(r"\+?\d[\d\s\-]{9,14}", page.content())

    for c in candidates:
        phone = re.sub(r"[^\d+]", "", c or "")
        if len(phone) >= 10:
            return phone
    return ""

# =========================
# SCRAPE QUERY
# =========================
def scrape_query(page, query):
    url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    page.goto(url, timeout=60000)

    try:
        page.wait_for_selector("div[role='article']", timeout=15000)
    except TimeoutError:
        return

    seen = set()
    stall = 0
    last_count = 0

    while True:
        cards = page.locator("div[role='article']")
        count = cards.count()

        if count == last_count:
            stall += 1
        else:
            stall = 0

        if stall >= 3:
            break

        last_count = count

        for i in range(count):
            try:
                card = cards.nth(i)
                card.click(timeout=3000)

                page.wait_for_selector(".DUwDvf", timeout=8000)
                name = page.locator(".DUwDvf").inner_text().strip()

                if not name or name in seen:
                    continue
                seen.add(name)

                phone = extract_phone(page)
                if phone:
                    save_lead(name, phone, query)

                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except Exception:
                continue

        # scroll results feed
        page.evaluate("""
            const feed = document.querySelector("div[role='feed']");
            if (feed) feed.scrollTop = feed.scrollHeight;
        """)
        time.sleep(0.8)

# =========================
# WORKER
# =========================
def worker(queries):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-extensions",
                "--disable-blink-features=AutomationControlled",
                "--blink-settings=imagesEnabled=false"
            ]
        )

        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6)"
        )

        page = context.new_page()

        for q in queries:
            try:
                scrape_query(page, q)
                time.sleep(random.uniform(*QUERY_COOLDOWN))
            except Exception:
                time.sleep(random.uniform(*CRASH_COOLDOWN))

        browser.close()

# =========================
# MAIN
# =========================
def main():
    init_db()

    all_queries = [f"{k} {d}" for d in RAJASTHAN_DISTRICTS for k in KEYWORDS]
    chunk = len(all_queries) // NUM_WORKERS + 1
    chunks = [all_queries[i:i+chunk] for i in range(0, len(all_queries), chunk)]

    procs = []
    for c in chunks[:NUM_WORKERS]:
        p = Process(target=worker, args=(c,))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print("🔥 PLAYWRIGHT SCRAPING COMPLETE")

if __name__ == "__main__":
    main()
