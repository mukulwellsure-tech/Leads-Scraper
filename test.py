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
OUTPUT_FILE = "Auto_leads.xlsx"
SAVE_EVERY = 50              # 🔥 SAVE AFTER EVERY 50 UNIQUE NUMBERS
MAX_RESULTS_PER_KEYWORD = 30
DELAY_RANGE = (3, 6)

# =========================
# BUSINESS TYPES
# =========================
BUSINESS_TYPES = [
    # CORE B2B ROLES (Sales Focused)
    "Electronics Shop",
    "Toy Store",
]

# =========================
# CITIES
# =========================
CITIES = [
    # Tier-1 Metros
    "Manali", "Jaipur"
]


# =========================
# KEYWORD GENERATOR
# =========================
def generate_keywords():
    return [f"{b} in {c}" for b in BUSINESS_TYPES for c in CITIES]

# =========================
# DRIVER SETUP
# =========================
def get_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

def ensure_excel_exists():
    if not os.path.exists(OUTPUT_FILE):
        df = pd.DataFrame(columns=[
            "Business Name",
            "Phone",
            "Keyword",
            "Source"
        ])
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"📄 Created empty Excel file: {OUTPUT_FILE}")


# =========================
# PHONE CLEANER
# =========================
def clean_phone(phone):
    if not phone:
        return None
    phone = re.sub(r"[^\d+]", "", phone)
    return phone if len(phone) >= 10 else None

# =========================
# SAVE TO EXCEL (APPEND + DEDUPE)
# =========================
def save_to_excel(new_rows):
    if not new_rows:
        return 0

    # Keep ONLY required fields
    new_df = pd.DataFrame(new_rows)[
        ["Business Name", "Phone", "Keyword", "Source"]
    ]

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_excel(OUTPUT_FILE)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df

    before = len(combined)
    combined.drop_duplicates(subset=["Phone"], inplace=True)
    after = len(combined)

    combined.to_excel(OUTPUT_FILE, index=False)
    return after - before


# =========================
# SCRAPE GOOGLE MAPS
# =========================
def scrape_keyword(driver, keyword):
    leads = []

    search_url = f"https://www.google.com/maps/search/{keyword.replace(' ', '+')}"
    driver.get(search_url)
    time.sleep(random.randint(*DELAY_RANGE))

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="article"]'))
        )
    except:
        return leads

    listings = driver.find_elements(By.XPATH, '//div[@role="article"]')

    for listing in listings[:MAX_RESULTS_PER_KEYWORD]:
        try:
            # =========================
            # 1️⃣ BUSINESS NAME (FROM CARD)
            # =========================
            try:
                name = listing.find_element(
                    By.XPATH, './/a[contains(@class,"hfpxzc")]'
                ).get_attribute("aria-label")
            except:
                name = ""

            if not name or name.lower() in ["results", "sponsored"]:
                continue

            # =========================
            # 2️⃣ PHONE FROM SEARCH CARD (CRITICAL FIX)
            # =========================
            phone = None
            try:
                card_text = listing.text
                match = re.search(r'(\+91[\s\-]?)?\d{5}[\s\-]?\d{5}', card_text)
                if match:
                    phone = clean_phone(match.group())
            except:
                pass

            # =========================
            # 3️⃣ CLICK LISTING (ONLY IF NEEDED)
            # =========================
            driver.execute_script("arguments[0].scrollIntoView(true);", listing)
            time.sleep(random.uniform(0.5, 1.2))
            driver.execute_script("arguments[0].click();", listing)
            time.sleep(random.randint(3, 5))

            # =========================
            # 4️⃣ PHONE FROM DETAILS PANEL (FALLBACK)
            # =========================
            if not phone:
                phone_xpaths = [
                    '//button[contains(@aria-label,"Call")]',
                    '//div[contains(@data-tooltip,"Call")]',
                    '//span[contains(text(),"+91")]'
                ]

                for xp in phone_xpaths:
                    try:
                        elem = driver.find_element(By.XPATH, xp)
                        raw = elem.get_attribute("aria-label") or elem.text
                        phone = clean_phone(raw)
                        if phone:
                            break
                    except:
                        continue

            if not phone:
                continue

            # =========================
            # 5️⃣ CATEGORY
            # =========================
            try:
                category = driver.find_element(
                    By.XPATH, '//button[contains(@aria-label,"Category")]'
                ).text
            except:
                category = ""

            # =========================
            # 6️⃣ ADDRESS
            # =========================
            try:
                address = driver.find_element(
                    By.XPATH, '//button[contains(@aria-label,"Address")]'
                ).text
            except:
                address = ""

            leads.append({
                "Business Name": name,
                "Phone": phone,
                "Category": category,
                "Address": address,
                "Keyword": keyword,
                "Source": "Google Maps"
            })

        except:
            continue

    return leads



# =========================
# MAIN PIPELINE
# =========================
def main():
    driver = get_driver()
    keywords = generate_keywords()
    ensure_excel_exists()
    buffer = []
    total_saved = 0

    for i, keyword in enumerate(keywords, 1):
        print(f"[{i}/{len(keywords)}] Scraping: {keyword}")
        leads = scrape_keyword(driver, keyword)

        for lead in leads:
            buffer.append(lead)

            if len(buffer) >= SAVE_EVERY:
                added = save_to_excel(buffer)
                total_saved += added
                print(f"💾 Saved batch | New leads added: {added} | Total: {total_saved}")
                buffer.clear()

        time.sleep(random.randint(*DELAY_RANGE))

    # Save remaining leads
    if buffer:
        added = save_to_excel(buffer)
        total_saved += added
        print(f"💾 Final save | New leads added: {added} | Total: {total_saved}")

    driver.quit()
    print(f"✅ DONE. Total unique leads saved: {total_saved}")

if __name__ == "__main__":
    main()
