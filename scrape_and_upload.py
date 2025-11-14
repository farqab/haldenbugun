import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore


# -------------------------------------------------------
#  PRICE PARSER
# -------------------------------------------------------
def parse_price(text: str):
    if not text:
        return None
    cleaned = (
        text.replace("₺", "")
        .replace("TL", "")
        .replace(",", ".")
        .replace(".", "")
        .strip()
    )
    try:
        return float(cleaned.split()[0])
    except:
        return None


# -------------------------------------------------------
#  ISTANBUL SCRAPER
# -------------------------------------------------------
def scrape_istanbul():
    url = "https://hal.ibb.gov.tr/dashboard"
    json_url = "https://hal.ibb.gov.tr/api/v1/market-prices/wholesale"
    try:
        r = requests.get(json_url, timeout=15)
        data = r.json()

        items = []
        for row in data["result"]:
            items.append({
                "product": row["productName"],
                "unit": row["unit"],
                "price_min": float(row["lowerPrice"]),
                "price_max": float(row["upperPrice"]),
            })
        return items
    except:
        return []


# -------------------------------------------------------
#  ANKARA SCRAPER
# -------------------------------------------------------
def scrape_ankara():
    url = "https://www.ankara.bel.tr/hal-fiyatlari"
    try:
        html = requests.get(url, timeout=15).text
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table")
        items = []

        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 4:
                continue

            items.append({
                "product": tds[0],
                "unit": tds[1],
                "price_min": parse_price(tds[2]),
                "price_max": parse_price(tds[3]),
            })
        return items
    except:
        return []


# -------------------------------------------------------
#  IZMIR SCRAPER
# -------------------------------------------------------
def scrape_izmir():
    url = "https://www.izmir.bel.tr/tr/HalFiyatlari/181"
    try:
        html = requests.get(url, timeout=15).text
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table")
        items = []

        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 4:
                continue

            items.append({
                "product": tds[0],
                "unit": tds[1],
                "price_min": parse_price(tds[2]),
                "price_max": parse_price(tds[3]),
            })
        return items
    except:
        return []


# -------------------------------------------------------
#  BURSA SCRAPER
# -------------------------------------------------------
def scrape_bursa():
    url = "https://www.bursa.bel.tr/hal-fiyatlari"
    try:
        html = requests.get(url, timeout=15).text
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table")
        items = []

        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 4:
                continue

            items.append({
                "product": tds[0],
                "unit": tds[1],
                "price_min": parse_price(tds[2]),
                "price_max": parse_price(tds[3]),
            })
        return items
    except:
        return []


# -------------------------------------------------------
#  ANTALYA SCRAPER
# -------------------------------------------------------
def scrape_antalya():
    url = "https://antalya.bel.tr/hal-fiyatlari"
    try:
        html = requests.get(url, timeout=15).text
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table")
        items = []

        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 4:
                continue

            items.append({
                "product": tds[0],
                "unit": tds[1],
                "price_min": parse_price(tds[2]),
                "price_max": parse_price(tds[3]),
            })
        return items
    except:
        return []


# -------------------------------------------------------
#  SCRAPE ALL
# -------------------------------------------------------
def scrape_all():
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "cities": {
            "istanbul": scrape_istanbul(),
            "ankara": scrape_ankara(),
            "izmir": scrape_izmir(),
            "bursa": scrape_bursa(),
            "antalya": scrape_antalya(),
        }
    }


# -------------------------------------------------------
#  FIREBASE UPLOADER
# -------------------------------------------------------
def upload_to_firebase(data):
    cred = credentials.Certificate("firebase-key.json")  # kendi anahtarın!!
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    # Tüm data -> 1 belgeye yazılacak
    db.collection("hal_fiyatlari").document("guncel").set(data)

    print("🔥 Firebase'e yüklendi!")


# -------------------------------------------------------
#  MAIN
# -------------------------------------------------------
if __name__ == "__main__":
    data = scrape_all()

    # Local JSON
    with open("hal_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✔ hal_data.json oluşturuldu.")
    upload_to_firebase(data)
