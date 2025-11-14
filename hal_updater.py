import csv
import json
import os
from datetime import datetime, timedelta

import firebase_admin
import requests
from firebase_admin import credentials, firestore


# ----------------------------------------------------
# 1) Firebase Admin başlatma
# ----------------------------------------------------
def init_firebase():
    service_account_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if not service_account_json:
        raise RuntimeError(
            "FIREBASE_SERVICE_ACCOUNT_JSON env değişkeni tanımlı değil."
        )

    cred_dict = json.loads(service_account_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()


# ----------------------------------------------------
# 2) Ortak yardımcı fonksiyonlar
# ----------------------------------------------------
def parse_price(value):
    """Fiyat değerlerini güvenli biçimde float'a çevirir."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if "," in s and s.count(",") == 1 and "." not in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------
# 3) İZMİR – Güncel Sebze/Meyve Hal Fiyatları API
# ----------------------------------------------------
IZMIR_HAL_API_URL = (
    "https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{date}"
)


def fetch_izmir_for_date(date_str: str):
    url = IZMIR_HAL_API_URL.format(date=date_str)
    print(f"[INFO] İzmir API isteği: {url}")

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] İzmir API isteği başarısız: {e}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        print(f"[WARN] İzmir API JSON parse hatası: {e}")
        return []

    if isinstance(data, dict):
        items = (
            data.get("data")
            or data.get("Data")
            or data.get("result")
            or data.get("Result")
            or []
        )
    else:
        items = data

    normalized = []
    for row in items:
        product = (
            row.get("MAL_ADI")
            or row.get("mal_adi")
            or row.get("malAdi")
            or row.get("URUN_ADI")
            or row.get("urun_adi")
        )
        unit = row.get("BIRIM") or row.get("birim") or "KG"
        avg_price = (
            row.get("ORTALAMA_UCRET")
            or row.get("ortalama_ucret")
            or row.get("ORTALAMA_FIYAT")
            or row.get("ortalama_fiyat")
        )
        price = parse_price(avg_price)

        if not product or price is None:
            continue

        normalized.append(
            {
                "product": product,
                "unit": unit,
                "price": price,
            }
        )

    print(f"[INFO] İzmir için {len(normalized)} kayıt bulundu ({date_str})")
    return normalized


def fetch_izmir():
    today = datetime.now()
    for delta in range(0, 3):  # bugün, dün, evvelsi gün
        date = today - timedelta(days=delta)
        date_str = date.strftime("%Y-%m-%d")
        items = fetch_izmir_for_date(date_str)
        if items:
            return items, date_str

    print("[WARN] İzmir için son 3 günde veri bulunamadı.")
    return [], today_str()


# ----------------------------------------------------
# 4) KONYA – Açık Veri CSV
# ----------------------------------------------------
KONYA_CSV_URL = (
    "https://acikveri.konya.bel.tr/dataset/"
    "0a341ce8-4369-4d91-93d7-a302298275ad/resource/"
    "532c336b-b3b4-42f9-ae46-44d0597e3ff9/download/hal_fiyatlari.csv"
)


def fetch_konya_for_date(date_str: str):
    print(f"[INFO] Konya CSV isteği: {KONYA_CSV_URL}")
    try:
        resp = requests.get(KONYA_CSV_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Konya CSV isteği başarısız: {e}")
        return []

    text = resp.text
    reader = csv.DictReader(text.splitlines())

    rows_for_
