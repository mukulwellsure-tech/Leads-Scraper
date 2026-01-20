import time
import random
import re
import os
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException

# =====================================================
# CONFIG
# =====================================================
SEARCH_QUERIES = ["MRF RAJASTHAN"]
MAX_RESULTS_PER_QUERY = 1001
OUTPUT_FILE = "Trademark_Sellers_All.xlsx"

SAVE_EVERY = 20  # 🔥 SAVE AFTER EVERY 30 RECORDS
WAIT_MIN = 1.5
WAIT_MAX = 3.0
HEADLESS = False  # Set True for bulk runs

# =====================================================
# BRAND INTELLIGENCE
# =====================================================
BLACKLIST = {
    "retailer", "shop", "store", "wholesaler", "trading",
    "general trading", "import export", "broker", "resale"
}

WHITELIST = {
    "authorized distributor", "official distributor",
    "exclusive distributor", "sole distributor",
    "channel partner", "distribution partner"
}

STRONG_SIGNALS = {
    "iso", "iso 9001", "certified distributor",
    "authorized partner", "registered company",
    "since", "established"
}

AGGREGATOR_DOMAINS = ["justdial", "indiamart", "facebook", "tradeindia"]

# =====================================================
# SCORING ENGINE
# =====================================================
def evaluate_brand(name, category, website, rating, reviews):
    text = f"{name} {category}".lower()
    score = 0
    signals = []

    for word in BLACKLIST:
        if word in text:
            score -= 40
            signals.append("Reseller keyword")

    for word in WHITELIST:
        if word in text:
            score += 30
            signals.append(word)

    for word in STRONG_SIGNALS:
        if word in text:
            score += 20
            signals.append(word)

    if website:
        if not any(x in website for x in AGGREGATOR_DOMAINS):
            score += 25
            signals.append("Official Website")
        else:
            score -= 10
            signals.append("Aggregator Website")

    if re.search(r"\b(pvt|private|ltd|limited)\b", text):
        score += 20
        signals.append("Legal Entity")

    if rating:
        if rating >= 4.0:
            score += 20
            signals.append("High Rating")
        elif rating < 3.5:
            score -= 10

    if reviews and reviews >= 50:
        score += 15
        signals.append("High Reviews")

    if score >= 50:
        tier = "HIGH"
    elif score >= 20:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    status = (
        "APPROVED" if score >= 20 else
        "REVIEW_REQUIRED" if score >= 0 else
        "REJECTED"
    )

    return score, tier, status, list(set(signals))

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
# UTILITIES
# =====================================================
def save_progress(data):
    df_new = pd.DataFrame(data)

    if os.path.exists(OUTPUT_FILE):
        df_old = pd.read_excel(OUTPUT_FILE)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df.drop_duplicates(subset=["Phone", "Website"], inplace=True)
    else:
        df = df_new

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"💾 Saved {len(df)} total records")

# =====================================================
# GOOGLE MAPS SCRAPER
# =====================================================
def scrape_google_maps(driver, query):
    collected = []
    seen_names = set()

    driver.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
    )

    feed_xpath = '//div[@role="feed"]'
    item_xpath = '//div[@role="article"]'

    last_count = 0
    same_count_retries = 0

    while True:
        listings = driver.find_elements(By.XPATH, item_xpath)

        # STOP if Google Maps stops loading new results
        if len(listings) == last_count:
            same_count_retries += 1
            if same_count_retries >= 3:
                break
        else:
            same_count_retries = 0

        last_count = len(listings)

        for idx in range(len(listings)):
            try:
                listings = driver.find_elements(By.XPATH, item_xpath)
                item = listings[idx]

                driver.execute_script("arguments[0].scrollIntoView(true);", item)
                time.sleep(0.7)
                item.click()

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
                )

                name = driver.find_element(By.CLASS_NAME, "DUwDvf").text
                if name in seen_names:
                    continue
                seen_names.add(name)

                def safe(xpath):
                    try:
                        return driver.find_element(By.XPATH, xpath).text
                    except:
                        return ""

                category = safe("//button[contains(@aria-label,'Category')]")
                address = safe("//button[contains(@aria-label,'Address')]")
                phone_raw = safe("//button[contains(@aria-label,'Phone')]")
                phone = re.sub(r"[^\d+\-\s]", "", phone_raw).strip()

                website = ""
                try:
                    website = driver.find_element(
                        By.XPATH, "//a[contains(@aria-label,'Website')]"
                    ).get_attribute("href")
                except:
                    pass

                rating = None
                reviews = None
                try:
                    rating = float(
                        driver.find_element(
                            By.XPATH,
                            "//span[@aria-label[contains(.,'stars')]]"
                        ).get_attribute("aria-label").split()[0]
                    )
                except:
                    pass

                try:
                    reviews = int(
                        re.sub(r"\D", "",
                            driver.find_element(
                                By.XPATH,
                                "//button[contains(@aria-label,'reviews')]"
                            ).text
                        )
                    )
                except:
                    pass

                city, state = "", ""
                if address:
                    parts = address.split(",")
                    if len(parts) >= 2:
                        city, state = parts[-2].strip(), parts[-1].strip()

                score, tier, status, signals = evaluate_brand(
                    name, category, website, rating, reviews
                )

                collected.append({
                    "Brand_Name": name,
                    "Phone": phone,
                    "Website": website,
                    "Category": category,
                    "Rating": rating,
                    "Reviews": reviews,
                    "City": city,
                    "State": state,
                    "Confidence_Score": score,
                    "Confidence_Tier": tier,
                    "Status": status,
                    "Ownership_Signals": ", ".join(signals),
                    "Source": "Google Maps",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                if len(collected) % SAVE_EVERY == 0:
                    save_progress(collected)
                    collected.clear()

                time.sleep(random.uniform(WAIT_MIN, WAIT_MAX))

            except StaleElementReferenceException:
                continue
            except Exception:
                continue

        # SCROLL TO LOAD MORE
        driver.execute_script("""
            const feed = document.querySelector('div[role="feed"]');
            if (feed) feed.scrollTop = feed.scrollHeight;
        """)
        time.sleep(2)

    return collected


# =====================================================
# MAIN
# =====================================================
def main():
    driver = setup_driver()
    buffer = []

    try:
        for query in SEARCH_QUERIES:
            print("🔍 Searching:", query)
            buffer.extend(scrape_google_maps(driver, query))
    finally:
        driver.quit()

    if buffer:
        save_progress(buffer)

    print("✅ Scraping complete")

if __name__ == "__main__":
    main()
