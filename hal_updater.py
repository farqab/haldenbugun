import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import json
import os
import requests


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
# 2) İzmir Açık Veri / Hal Fiyatları API'den veri çekme
#    URL: https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{yyyy-MM-dd}
# ----------------------------------------------------
IZMIR_HAL_API_URL = (
    "https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{date}"
)


def parse_price(value):
    """
    API'den gelen fiyat değerleri string/virgüllü olabilir.
    Örnek: "4,0" veya "4.0" -> float(4.0)
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    s = s.replace(".", "").replace(",", ".") if "," in s else s
    try:
        return float(s)
    except ValueError:
        return None


def fetch_izmir_data_for_date(date_str: str):
    """
    Verilen tarih için İzmir hal fiyatlarını çeker.
    Başarılı olursa normalleştirilmiş product-list döner.
    Başarısız olursa [] döner.
    """
    url = IZMIR_HAL_API_URL.format(date=date_str)
    print(f"[INFO] İzmir API isteği: {url}")

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] İzmir API isteği başarısız: {e}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        print(f"[WARN] İzmir API JSON parse hatası: {e}")
        return []

    # API bazen şöyle gelebilir:
    # 1) Direkt liste: [ { "MAL_ADI": "...", ... }, ... ]
    # 2) Dict içinde "data" / "Data" / "result" alanı: { "data": [ ... ] }
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
        # Alan isimleri uppercase / lowercase olabilir, hepsini deneyelim
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
            # Ürün adı veya fiyat yoksa bu kaydı atla
            continue

        normalized.append(
            {
                "product": product,
                "unit": unit,
                "price": price,
                "raw": row,  # gerekirse debug için
            }
        )

    print(f"[INFO] İzmir için {len(normalized)} kayıt bulundu ({date_str})")
    return normalized


def fetch_hal_data_for_city(city: str):
    """
    Şehir bazlı veri çekme fonksiyonu.
    Şimdilik İzmir için gerçek API, diğer şehirler için (Antalya/Mersin)
    istersen ileride ayrı kaynaklar ekleyebilirsin.
    """
    today = datetime.now()

    if city == "İzmir":
        # Bugün için veri yoksa dünü / evveli günü denemek için küçük bir fallback
        for delta in range(0, 3):  # bugün, -1, -2
            date = today - timedelta(days=delta)
            date_str = date.strftime("%Y-%m-%d")
            items = fetch_izmir_data_for_date(date_str)
            if items:
                return items
        return []

    # Diğer şehirler için şimdilik boş dönüyoruz (veya dummy):
    # İleride CollectAPI vs. eklersen burada kullanırsın.
    print(f"[WARN] {city} için henüz gerçek veri kaynağı tanımlı değil.")
    return []


# ----------------------------------------------------
# 3) Firestore'a yazan fonksiyon
# ----------------------------------------------------
def push_city_prices(db, city: str):
    """
    Verilen şehir için verileri Firestore'a yazar.
    Koleksiyon yapısı:
    halPrices / {city} / {yyyy-MM-dd} / {productId}
    """
    today = datetime.now().strftime("%Y-%m-%d")
    items = fetch_hal_data_for_city(city)

    if not items:
        print(f"[WARN] {city} için yazılacak veri bulunamadı.")
        return

    for item in items:
        product_name = item["product"]
        price = float(item["price"])
        unit = item.get("unit", "KG")

        # doc_id: ürün adını küçük harf ve boşlukları tire yapıyoruz
        doc_id = (
            product_name.lower()
            .replace(" ", "_")
            .replace("ğ", "g")
            .replace("ü", "u")
            .replace("ş", "s")
            .replace("ı", "i")
            .replace("ö", "o")
            .replace("ç", "c")
        )

        doc_ref = (
            db.collection("halPrices")
            .document(city)
            .collection(today)
            .document(doc_id)
        )

        data = {
            "product": product_name,
            "unit": unit,
            "price": price,
            "city": city,
            "date": today,
        }

        doc_ref.set(data)
        print(f"[OK] {city} - {product_name} kaydedildi (doc_id={doc_id})")


# ----------------------------------------------------
# 4) main
# ----------------------------------------------------
def main():
    db = init_firebase()

    # Şimdilik sadece İzmir'i gerçek API'den dolduruyoruz.
    # İstersen Antalya, Mersin vs. de eklenebilir.
    cities = ["İzmir"]

    for city in cities:
        push_city_prices(db, city)


if __name__ == "__main__":
    main()
