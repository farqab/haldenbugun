import os
import json
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore


# ---------------- FIREBASE ---------------- #

def init_firebase():
    """
    GitHub Actions'tan gelen FIREBASE_SERVICE_ACCOUNT_JSON
    env değişkeni ile Firebase Admin başlatır.
    """
    raw = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON env değişkeni yok!")

    cred_dict = json.loads(raw)

    # Burada servis hesabı JSON'u kullanıyoruz, içinde "type": "service_account" olmalı
    cred = credentials.Certificate(cred_dict)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    return firestore.client()


# --------------- ORTAK YARDIMCI FONKSİYONLAR --------------- #

def to_float(val: str):
    """
    "40 ₺", "4.6 TL", "5,12 TL" gibi değerleri float'a çevirir.
    Çeviremezse None döner.
    """
    if not val:
        return None

    # Boşluk ve para birimi atanır
    cleaned = (
        val.replace("₺", "")
           .replace("TL", "")
           .replace("tl", "")
           .replace("\xa0", " ")
           .strip()
    )

    # Türkçe virgül -> nokta
    cleaned = cleaned.replace(".", "").replace(",", ".") if cleaned.count(",") == 1 else cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return None


# --------------- SCRAPER: GUNCELFIYATLARI.COM --------------- #

def scrape_antalya():
    """
    Antalya hal fiyatlarını guncelfiyatlari.com'dan çeker.
    URL: https://www.guncelfiyatlari.com/antalya-hal-fiyatlari/
    Çıktı: [{"product": ..., "unit": ..., "min": ..., "max": ...}, ...]
    """
    url = "https://www.guncelfiyatlari.com/antalya-hal-fiyatlari/"
    print(f"[SCRAPER] Antalya -> {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [l.replace("\xa0", " ").strip() for l in text.splitlines()]

    items = []

    for line in lines:
        # Satırda ₺ yoksa geç
        if "₺" not in line:
            continue

        # 80 ₺ 190 ₺ gibi iki fiyat yakala
        m = re.search(r"(\d+[.,]?\d*)\s*₺\s+(\d+[.,]?\d*)\s*₺", line)
        if not m:
            continue

        min_raw, max_raw = m.group(1), m.group(2)
        min_price = to_float(min_raw)
        max_price = to_float(max_raw)

        # Fiyatlardan önceki kısım: "Ahududu Pk/125 G"
        prefix = line[:m.start()].strip()
        parts = prefix.split()
        if len(parts) < 2:
            continue

        unit = parts[-1]
        product = " ".join(parts[:-1])

        items.append({
            "product": product,
            "unit": unit,
            "min": min_price,
            "max": max_price,
        })

    print(f"[SCRAPER] Antalya: {len(items)} satır bulundu.")
    return items


def scrape_mersin():
    """
    Mersin hal fiyatlarını guncelfiyatlari.com'dan çeker.
    URL: https://www.guncelfiyatlari.com/mersin-hal-fiyatlari/
    Çıktı: [{"product": ..., "unit": ..., "min": ..., "max": ...}, ...]
    """
    url = "https://www.guncelfiyatlari.com/mersin-hal-fiyatlari/"
    print(f"[SCRAPER] Mersin -> {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [l.replace("\xa0", " ").strip() for l in text.splitlines()]

    items = []

    for line in lines:
        # Satırda "TL" yoksa geç
        if "TL" not in line:
            continue

        # 4.6 TL 6 TL gibi iki fiyat yakala
        m = re.search(r"(\d+[.,]?\d*)\s*TL\s+(\d+[.,]?\d*)\s*TL", line)
        if not m:
            continue

        min_raw, max_raw = m.group(1), m.group(2)
        min_price = to_float(min_raw)
        max_price = to_float(max_raw)

        # Fiyatlardan önceki kısımda ürün adı / tür vs var
        prefix = line[:m.start()].strip()
        tokens = prefix.split()

        # Mersin satırı: ŞUBE ÜRÜN CİNSİ TÜRÜ ...
        # Çok kasmadan 2. ve 3. kelimeyi ürün ismi olarak alalım
        if len(tokens) >= 3:
            product = f"{tokens[1]} {tokens[2]}"
        elif len(tokens) >= 2:
            product = tokens[1]
        else:
            product = tokens[0]

        # Satırın sonunda birim: KİLOGRAM, ADET, BAĞ vs
        suffix = line[m.end():].strip()
        unit = suffix.split()[-1] if suffix else ""

        items.append({
            "product": product,
            "unit": unit,
            "min": min_price,
            "max": max_price,
        })

    print(f"[SCRAPER] Mersin: {len(items)} satır bulundu.")
    return items


# ----------- TÜM ŞEHİRLERİ TOPLAYAN FONKSİYON ----------- #

def collect_all_cities():
    data = {}
    try:
        data["Antalya"] = scrape_antalya()
    except Exception as e:
        print(f"[HATA] Antalya verisi çekilirken hata: {e}")

    try:
        data["Mersin"] = scrape_mersin()
    except Exception as e:
        print(f"[HATA] Mersin verisi çekilirken hata: {e}")
    return data


# --------------- FIRESTORE'A YAZMA --------------- #

def save_to_firestore(db, all_city_data: dict):
    """
    Firestore yapısı:

    halPrices (collection)
      ├─ Antalya (document)
      │    ├─ date: "2025-11-14"
      │    └─ items: [ {product, unit, min, max}, ... ]
      └─ Mersin  (document)
           ├─ date: "2025-11-14"
           └─ items: [ ... ]
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    col = db.collection("halPrices")

    for city_name, items in all_city_data.items():
        if not items:
            print(f"[FIRESTORE] {city_name}: veri yok, atlanıyor.")
            continue

        doc_ref = col.document(city_name)
        payload = {
            "date": today,
            "items": items
        }
        doc_ref.set(payload)
        print(f"[FIRESTORE] {city_name}: {len(items)} satır yazıldı.")


# --------------- MAIN --------------- #

def main():
    print("=== Hal Updater Çalıştı ===")
    db = init_firebase()
    all_data = collect_all_cities()
    save_to_firestore(db, all_data)
    print("=== Bitti ===")


if __name__ == "__main__":
    main()
