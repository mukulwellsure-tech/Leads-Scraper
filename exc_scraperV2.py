import time
import random
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ----------------
INPUT_FILE = "sample.xlsx"
OUTPUT_FILE = "out.xlsx"
BLOCKED_NUMBER = "9999999776"
RESTART_AFTER = 50


# ---------------- DRIVER ----------------
def setup_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )


# ---------------- UTILS ----------------
def random_sleep(a=4, b=8):
    time.sleep(random.uniform(a, b))


def clean_phone(text):
    if not text:
        return None
    phone = re.sub(r"[^\d]", "", text)
    if BLOCKED_NUMBER in phone:
        return None
    if len(phone) < 10:
        return None
    return phone

def get_phone_justdial(driver, url):
    if not isinstance(url, str) or not url.startswith("http"):
        return None

    try:
        driver.get(url)
        random_sleep(5, 8)

        # Scroll to load content
        driver.execute_script("window.scrollBy(0,800)")
        random_sleep(2, 3)

        # Common Justdial phone containers
        elements = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'tel:')] | //span[contains(@class,'callcontent')]"
        )

        for el in elements:
            phone = clean_phone(el.text or el.get_attribute("href"))
            if phone:
                return phone

    except Exception:
        pass

    return None

# ---------------- GOOGLE MAPS SCRAPER ----------------
def get_phone_google_maps(driver, url):
    if not isinstance(url, str) or not url.startswith("http"):
        return None

    try:
        driver.get(url)
        random_sleep(4, 6)

        driver.execute_script("window.scrollBy(0,600)")
        random_sleep(2, 3)

        elements = driver.find_elements(
            By.XPATH,
            "//button[contains(@aria-label,'Phone')] | //a[starts-with(@href,'tel:')]"
        )

        for el in elements:
            phone = clean_phone(el.text or el.get_attribute("href"))
            if phone:
                return phone

    except Exception:
        pass

    return None


# ---------------- SAVE SAFE ----------------
def save_progress(rows):
    if rows:
        pd.DataFrame(rows).to_excel(OUTPUT_FILE, index=False)


# ---------------- MAIN ----------------
def main():
    df = pd.read_excel(INPUT_FILE)
    results = []

    driver = setup_driver()

    try:
        for idx, row in df.iterrows():
            name = row["Seller name"]
            city = row["City"]
            state = row["State"]
            gmap = row["Google"]
            justdial = row["Just Dial"]

            print(f"\n[{idx+1}] {name} - {city}")

            # ---------- TRY GOOGLE MAPS ----------
            phone = get_phone_google_maps(driver, gmap)
            source = None

            if phone:
                source = "Google Maps"
            else:
                # ---------- FALLBACK TO JUSTDIAL ----------
                print("  → Trying Justdial...")
                phone = get_phone_justdial(driver, justdial)
                if phone:
                    source = "Justdial"

            if phone:
                print(f"  ✔ Found: {phone} ({source})")
                results.append({
                    "Seller name": name,
                    "City": city,
                    "State": state,
                    "Phone": phone,
                    "Source": source
                })
                save_progress(results)
            else:
                print("  ✖ No phone found on Maps or Justdial")

            random_sleep(5, 9)

            # Restart browser to reduce detection
            if (idx + 1) % RESTART_AFTER == 0:
                driver.quit()
                random_sleep(10, 15)
                driver = setup_driver()

    finally:
        save_progress(results)
        driver.quit()


if __name__ == "__main__":
    main()






