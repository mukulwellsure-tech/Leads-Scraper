import time
import random
import re
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# CONFIG
# =========================
OUTPUT_FILE = "Master_Leads.xlsx"
SAVE_EVERY = 50
MAX_RESULTS = 30
DELAY_RANGE = (3, 6)

# =========================
# INPUT ARRAYS (ONLY THESE)
# =========================
BUSINESS_TYPES = [
    "Electronics Shop",
    "Toy Store"
]

CITIES = [
    "Jaipur",
    "Manali"
]

# =========================
# DRIVER SETUP
# =========================
def get_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver

# =========================
# HELPERS
# =========================
def clean_phone(text):
    if not text:
        return None
    text = re.sub(r"[^\d+]", "", text)
    return text if len(text) >= 10 else None

def ensure_excel():
    if not os.path.exists(OUTPUT_FILE):
        df = pd.DataFrame(columns=[
            "Business Name", "Phone", "Keyword", "City", "Source"
        ])
        df.to_excel(OUTPUT_FILE, index=False)

def save_rows(rows):
    if not rows:
        return 0

    new_df = pd.DataFrame(rows)

    if os.path.exists(OUTPUT_FILE):
        old_df = pd.read_excel(OUTPUT_FILE)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    before = len(combined)
    combined.drop_duplicates(subset=["Phone"], inplace=True)
    after = len(combined)

    combined.to_excel(OUTPUT_FILE, index=False)
    return after - before

# =========================
# GOOGLE MAPS SCRAPER
# =========================
def scrape_google_maps(driver, keyword, city):
    leads = []
    url = f"https://www.google.com/maps/search/{keyword.replace(' ', '+')}+{city}"
    driver.get(url)
    time.sleep(random.randint(*DELAY_RANGE))

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
        )
    except:
        return leads

    listings = driver.find_elements(By.XPATH, '//div[@role="article"]')[:MAX_RESULTS]

    for listing in listings:
        try:
            name = listing.text.split("\n")[0]
            phone = None

            match = re.search(r'(\+91[\s\-]?)?\d{5}[\s\-]?\d{5}', listing.text)
            if match:
                phone = clean_phone(match.group())

            if not phone:
                driver.execute_script("arguments[0].click();", listing)
                time.sleep(3)

                try:
                    btn = driver.find_element(By.XPATH, '//button[contains(@aria-label,"Call")]')
                    phone = clean_phone(btn.get_attribute("aria-label"))
                except:
                    pass

            if phone:
                leads.append({
                    "Business Name": name,
                    "Phone": phone,
                    "Keyword": keyword,
                    "City": city,
                    "Source": "Google Maps"
                })

        except:
            continue

    return leads

# =========================
# JUSTDIAL SCRAPER (FALLBACK)
# =========================
def scrape_justdial(driver, keyword, city):
    leads = []
    city_slug = city.replace(" ", "-")
    keyword_slug = keyword.replace(" ", "-")
    url = f"https://www.justdial.com/{city_slug}/{keyword_slug}"
    driver.get(url)
    time.sleep(random.randint(5, 8))

    try:
        cards = driver.find_elements(By.CLASS_NAME, "resultbox_info")
    except:
        return leads

    for card in cards[:MAX_RESULTS]:
        try:
            name = card.find_element(By.CLASS_NAME, "resultbox_title_anchor").text
            phone = card.find_element(By.CLASS_NAME, "callcontent").text
            phone = clean_phone(phone)

            if phone:
                leads.append({
                    "Business Name": name,
                    "Phone": phone,
                    "Keyword": keyword,
                    "City": city,
                    "Source": "Justdial"
                })

        except:
            continue

    return leads

# =========================
# MASTER PIPELINE
# =========================
def main():
    ensure_excel()
    driver = get_driver()
    buffer = []
    total_added = 0

    for city in CITIES:
        for business in BUSINESS_TYPES:
            print(f"🔍 {business} | {city}")

            # 1️⃣ GOOGLE MAPS
            gm_leads = scrape_google_maps(driver, business, city)
            buffer.extend(gm_leads)

            time.sleep(random.randint(2, 4))

            # 2️⃣ JUSTDIAL
            jd_leads = scrape_justdial(driver, business, city)
            buffer.extend(jd_leads)

            # 3️⃣ SAVE IN BATCHES
            if len(buffer) >= SAVE_EVERY:
                added = save_rows(buffer)
                total_added += added
                print(f"💾 Saved | New: {added} | Total: {total_added}")
                buffer.clear()

            time.sleep(random.randint(*DELAY_RANGE))

    # Final save
    if buffer:
        added = save_rows(buffer)
        total_added += added

    driver.quit()
    print(f"✅ DONE | Total Unique Leads: {total_added}")

# =========================
if __name__ == "__main__":
    main()
