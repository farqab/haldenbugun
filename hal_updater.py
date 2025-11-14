import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json
import os


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


def fetch_hal_data_for_city(city: str):
    """
    Şimdilik dummy veriler. Sonraki adımda CollectAPI / belediye API'si ile değiştireceğiz.
    """
    if city == "Antalya":
        return [
            {"product": "Domates", "unit": "KG", "price": 12.5},
            {"product": "Salatalık", "unit": "KG", "price": 15.0},
            {"product": "Biber", "unit": "KG", "price": 20.0},
        ]
    elif city == "Mersin":
        return [
            {"product": "Domates", "unit": "KG", "price": 11.0},
            {"product": "Patlıcan", "unit": "KG", "price": 18.0},
        ]
    else:
        return []


def push_city_prices(db, city: str):
    """
    Verilen şehir için verileri Firestore'a yazar.
    Koleksiyon yapısı:
    halPrices / {city} / {yyyy-MM-dd} / {productId}
    """
    today = datetime.now().strftime("%Y-%m-%d")
    items = fetch_hal_data_for_city(city)

    for item in items:
        doc_id = item["product"].lower()  # domates, salatalık...

        doc_ref = (
            db.collection("halPrices")
            .document(city)
            .collection(today)
            .document(doc_id)
        )

        data = {
            "product": item["product"],
            "unit": item["unit"],
            "price": float(item["price"]),
            "city": city,
            "date": today,
        }

        doc_ref.set(data)
        print(f"{city} - {doc_id} kaydedildi")


def main():
    db = init_firebase()
    for city in ["Antalya", "Mersin"]:
        push_city_prices(db, city)


if __name__ == "__main__":
    main()
