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
    """
    GitHub Actions ortamında, FIREBASE_SERVICE_ACCOUNT_JSON
    adlı secret'tan gelen JSON string'i kullanarak Firebase Admin'i başlatır.
    """
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
    """
    Fiyat değerlerini güvenli biçimde float'a çevirir.
    Örnek: "4,0" veya "4.0" -> 4.0
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    # "4,50" gibi değerler gelebilir
    if "," in s and s.count(",") == 1 and "." not in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def today_str():
    """Bugünün tarihini YYYY-MM-DD formatında döndürür."""
    return datetime.now().strftime("%Y-%m-%d")


# ----------------------------------------------------
# 3) İZMİR – Güncel Sebze/Meyve Hal Fiyatları API
#    https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{yyyy-MM-dd}
# ----------------------------------------------------
IZMIR_HAL_API_URL = (
    "https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{date}"
)


def fetch_izmir_for_date(date_str: str):
    """
    Verilen tarih için İzmir hal fiyatlarını çeker.
    Başarılı olursa normalize edilmiş ürün listesi döner.
    """
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

    # Bazen dict içinde "data"/"Data"/"result" vs. olabilir
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
        # Ürün adı farklı anahtarlarla gelebilir
        product = (
            row.get("MAL_ADI")
            or row.get("mal_adi")
            or row.get("malAdi")
            or row.get("URUN_ADI")
            or row.get("urun_adi")
        )
        unit = row.get("BIRIM") or row.get("birim") or "KG"

        # Ortalama fiyat alanını bul
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
    """
    İzmir için bugün / dünü / evvelki günü deneyerek veri döndürür.
    """
    today = datetime.now()
    for delta in range(0, 3):  # bugün, -1, -2
        date = today - timedelta(days=delta)
        date_str = date.strftime("%Y-%m-%d")
        items = fetch_izmir_for_date(date_str)
        if items:
            return items, date_str

    print("[WARN] İzmir için son 3 günde veri bulunamadı.")
    return [], today_str()


# ----------------------------------------------------
# 4) KONYA – Açık Veri CSV
#    https://acikveri.konya.bel.tr/.../download/hal_fiyatlari.csv
# ----------------------------------------------------
KONYA_CSV_URL = (
    "https://acikveri.konya.bel.tr/dataset/"
    "0a341ce8-4369-4d91-93d7-a302298275ad/resource/"
    "532c336b-b3b4-42f9-ae46-44d0597e3ff9/download/hal_fiyatlari.csv"
)


def fetch_konya_for_date(date_str: str):
    """
    Konya CSV veri setinden sadece verilen tarihe ait kayıtları çeker.
    Kolonlar: tarih, urun_ad, birim, en_dusuk_fiyat, en_yuksek_fiyat ...
    """
    print(f"[INFO] Konya CSV isteği: {KONYA_CSV_URL}")
    try:
        resp = requests.get(KONYA_CSV_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Konya CSV isteği başarısız: {e}")
        return []

    text = resp.text
    reader = csv.DictReader(text.splitlines())

    rows_for_day = []
    for row in reader:
        raw_date = row.get("tarih") or row.get("TARIH")
        if not raw_date:
            continue

        # timestamp gibi "2023-12-26 00:00:00" gelirse ilk 10 karakteri al
        day = str(raw_date).strip()[:10]
        if day != date_str:
            continue

        product = row.get("urun_ad") or row.get("URUN_AD")
        unit = row.get("birim") or row.get("BIRIM") or "KG"

        min_price = parse_price(row.get("en_dusuk_fiyat"))
        max_price = parse_price(row.get("en_yuksek_fiyat"))

        if not product or (min_price is None and max_price is None):
            continue

        # Ortalama fiyat: min/max varsa ortalamasını al
        if min_price is not None and max_price is not None:
            price = (min_price + max_price) / 2.0
        else:
            price = min_price or max_price

        rows_for_day.append(
            {
                "product": product,
                "unit": unit,
                "price": price,
            }
        )

    print(f"[INFO] Konya için {len(rows_for_day)} kayıt bulundu ({date_str})")
    return rows_for_day


def fetch_konya():
    """
    Konya için bugün / dünü / evvelki günü deneyerek veri döndürür.
    """
    today = datetime.now()
    for delta in range(0, 3):
        date = today - timedelta(days=delta)
        date_str = date.strftime("%Y-%m-%d")
        items = fetch_konya_for_date(date_str)
        if items:
            return items, date_str

    print("[WARN] Konya için son 3 günde veri bulunamadı.")
    return [], today_str()


# ----------------------------------------------------
# 5) Şehir bazlı router
# ----------------------------------------------------
def fetch_hal_data_for_city(city: str):
    """
    Şehir bazlı veri çekme router'ı.
    Buraya yeni şehir eklemek için yeni bir fetch_* fonksiyonu yazıp
    if/elif içine eklemen yeterli.
    Dönen değer: (items, date_str)
      - items: [{"product":..., "unit":..., "price":...}, ...]
      - date_str: "YYYY-MM-DD"
    """
    if city == "İzmir":
        return fetch_izmir()

    if city == "Konya":
        return fetch_konya()

    # Diğer şehirler için şimdilik veri kaynağı yok
    print(f"[WARN] {city} için henüz gerçek veri kaynağı tanımlı değil.")
    return [], today_str()


# ----------------------------------------------------
# 6) Firestore'a yazan fonksiyon
# ----------------------------------------------------
def normalize_doc_id(name: str) -> str:
    """Ürün adını Firestore doküman ID'si için normalize eder."""
    return (
        name.lower()
        .replace(" ", "_")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ş", "s")
        .replace("ı", "i")
        .replace("ö", "o")
        .replace("ç", "c")
        .replace("/", "_")
        .replace("\\", "_")
    )


def push_city_prices(db, city: str):
    """
    Verilen şehir için verileri Firestore'a yazar.
    Koleksiyon yapısı:
      halPrices / {city} / {yyyy-MM-dd} / {productId}
    """
    items, date_str = fetch_hal_data_for_city(city)

    if not items:
        print(f"[WARN] {city} için yazılacak veri bulunamadı.")
        return

    batch = db.batch()

    for item in items:
        product_name = item["product"]
        unit = item.get("unit", "KG")
        price = float(item["price"])

        doc_id = normalize_doc_id(product_name)
        doc_ref = (
            db.collection("halPrices")
            .document(city)
            .collection(date_str)
            .document(doc_id)
        )

        data = {
            "product": product_name,
            "unit": unit,
            "price": price,
            "city": city,
            "date": date_str,
        }

        batch.set(doc_ref, data)
        print(f"[OK] {city} - {product_name} kaydedildi (doc_id={doc_id})")

    batch.commit()
    print(f"[INFO] {city} için toplam {len(items)} kayıt Firestore'a yazıldı ({date_str}).")


# ----------------------------------------------------
# 7) main
# ----------------------------------------------------
def main():
    db = init_firebase()

    # Buraya istediğin kadar şehir ekleyebilirsin.
    # Şu an gerçek kaynak bağlı olanlar: İzmir (API), Konya (CSV)
    cities = ["İzmir", "Konya"]

    for city in cities:
        push_city_prices(db, city)


if __name__ == "__main__":
    main()
