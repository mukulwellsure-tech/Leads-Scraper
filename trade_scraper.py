import time
import random
import re
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
SEARCH_QUERIES = [
    "packaged food manufacturer",
    "electronics manufacturing company",
    "industrial machinery manufacturer",
    "cosmetics manufacturing company",
    "home appliances manufacturing company",
]

MAX_RESULTS_PER_QUERY = 40
OUTPUT_FILE = "Trademark_Owned_Sellers.xlsx"
REJECTED_FILE = "Rejected_Sellers.xlsx"

WAIT_MIN = 2
WAIT_MAX = 4

# =====================================================
# KEYWORD INTELLIGENCE
# =====================================================
BLACKLIST = {
    "trader", "dealer", "distributor", "supplier",
    "wholesaler", "retailer", "store", "shop"
}

WHITELIST = {
    "manufacturer", "manufacturing", "brand",
    "industries", "factory", "private limited",
    "pvt ltd", "limited"
}

STRONG_SIGNALS = {
    "iso", "oem", "brand owner",
    "registered brand", "private label"
}

# =====================================================
# SCORING ENGINE
# =====================================================
def evaluate_brand(name, category, website):
    text = f"{name} {category}".lower()
    score = 0
    signals = []

    for word in BLACKLIST:
        if word in text:
            return -100, ["Trader keyword detected"]

    for word in WHITELIST:
        if word in text:
            score += 20
            signals.append(word)

    for word in STRONG_SIGNALS:
        if word in text:
            score += 30
            signals.append(word)

    if website:
        score += 20
        signals.append("Website present")

    if re.search(r"\b(pvt|private|ltd|limited)\b", text):
        score += 15
        signals.append("Legal entity keyword")

    return score, list(set(signals))

# =====================================================
# DRIVER SETUP
# =====================================================
def setup_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

# =====================================================
# GOOGLE MAPS SCRAPER
# =====================================================
def scrape_google_maps(driver, query):
    approved = []
    rejected = []

    driver.get("https://www.google.com/maps")
    time.sleep(5)

    driver.find_element(By.ID, "searchboxinput").clear()
    driver.find_element(By.ID, "searchboxinput").send_keys(query)
    driver.find_element(By.ID, "searchbox-searchbutton").click()
    time.sleep(5)

    for _ in range(6):
        driver.execute_script(
            "document.querySelector('.m6QErb').scrollTop=100000"
        )
        time.sleep(2)

    listings = driver.find_elements(By.CLASS_NAME, "hfpxzc")[:MAX_RESULTS_PER_QUERY]

    for item in listings:
        try:
            item.click()
            time.sleep(3)

            name = driver.find_element(By.CLASS_NAME, "DUwDvf").text

            category = ""
            try:
                category = driver.find_element(
                    By.XPATH, "//button[contains(@aria-label,'Category')]"
                ).text
            except:
                pass

            website = ""
            try:
                website = driver.find_element(
                    By.XPATH, "//a[contains(@aria-label,'Website')]"
                ).get_attribute("href")
            except:
                pass

            phone = ""
            try:
                phone = driver.find_element(
                    By.XPATH, "//button[contains(@aria-label,'Phone')]"
                ).text
            except:
                pass

            address = ""
            try:
                address = driver.find_element(
                    By.XPATH, "//button[contains(@aria-label,'Address')]"
                ).text
            except:
                pass

            city, state = "", ""
            if address:
                parts = address.split(",")
                if len(parts) >= 2:
                    city = parts[-2].strip()
                    state = parts[-1].strip()

            score, signals = evaluate_brand(name, category, website)

            record = {
                "Brand_Name": name,
                "Phone": phone,
                "Website": website,
                "Category": category,
                "City": city,
                "State": state,
                "Source": "Google Maps",
                "Ownership_Signals": ", ".join(signals),
                "Confidence_Score": score,
                "Status": "APPROVED" if score >= 60 else "REJECTED",
                "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            if score >= 60:
                approved.append(record)
            else:
                rejected.append(record)

            time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

        except Exception:
            continue

    return approved, rejected

# =====================================================
# MAIN
# =====================================================
def main():
    driver = setup_driver()
    approved_all = []
    rejected_all = []

    try:
        for query in SEARCH_QUERIES:
            print(f"🔍 {query}")
            ok, bad = scrape_google_maps(driver, query)
            approved_all.extend(ok)
            rejected_all.extend(bad)
    finally:
        driver.quit()

    pd.DataFrame(approved_all).to_excel(OUTPUT_FILE, index=False)
    pd.DataFrame(rejected_all).to_excel(REJECTED_FILE, index=False)

    print("\n✅ SCRAPING COMPLETE")
    print(f"Approved trademark owners: {len(approved_all)}")
    print(f"Rejected listings: {len(rejected_all)}")

if __name__ == "__main__":
    main()
