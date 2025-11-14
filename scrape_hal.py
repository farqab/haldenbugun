import json
from datetime import datetime

import requests
from bs4 import BeautifulSoup


def parse_price(text: str):
    """
    '120 ₺' , '20,00 TL' gibi değerleri float'a çevirir.
    Hata olursa None döner.
    """
    if not text:
        return None
    t = (
        text.replace("₺", "")
        .replace("TL", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    # Örn: "120", "120.00"
    parts = t.split()
    if not parts:
        return None
    try:
        return float(parts[0])
    except ValueError:
        return None


def scrape_kayseri():
    """
    Kayseri Büyükşehir Belediyesi 'Hal Fiyat Listesi' sayfasından verileri çeker.
    URL: https://www.kayseri.bel.tr/hal-fiyatlari
    """
    url = "https://www.kayseri.bel.tr/hal-fiyatlari"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Sayfadaki tabloları tara, başlığında 'CİNSİ' geçen tabloyu bul
    target_table = None
    for table in soup.find_all("table"):
        header_text = " ".join(
            th.get_text(strip=True).upper() for th in table.find_all("th")
        )
        if "CİNSİ" in header_text or "CINSI" in header_text:
            target_table = table
            break

    if target_table is None:
        raise RuntimeError("Kayseri için fiyat tablosu bulunamadı.")

    items = []
    # İlk satır genelde başlık, o yüzden [1:] ile geçiyoruz
    for tr in target_table.find_all("tr")[1:]:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        # Beklenen sıra: CİNSİ | BİRİMİ | EN YÜKSEK FİYAT | EN DÜŞÜK FİYAT
        if len(tds) < 4:
            continue

        cinsi = tds[0]
        birimi = tds[1]
        en_yuksek = tds[2]
        en_dusuk = tds[3]

        item = {
            "product": cinsi,
            "unit": birimi,
            "price_max": parse_price(en_yuksek),
            "price_min": parse_price(en_dusuk),
        }
        items.append(item)

    return items


def main():
    data = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "cities": {
            "kayseri": scrape_kayseri(),
            # Sonra buraya "konya": scrape_konya(), "trabzon": scrape_trabzon() gibi ekleyebiliriz
        },
    }

    # Şu an için sadece JSON dosyası olarak kaydediyoruz
    with open("hal_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("hal_data.json oluşturuldu, kayıtlı şehirler:", ", ".join(data["cities"].keys()))


if __name__ == "__main__":
    main()
