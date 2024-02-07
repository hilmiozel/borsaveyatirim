from datetime import datetime
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests

USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://192.168.1.35:9200', basic_auth=(USER, PASS), verify_certs=False)


# Define constants
URL = "https://www.borsaveyatirim.com/bist-hisse-onerileri-ve-hedef-fiyatlari"
DATABASE_NAME = "borsaveyatirim.db"
TABLE_NAME = "hedef_fiyat"

# URL'den sayfa içeriğini getirin
response = requests.get(URL)
html_content = response.text

# BeautifulSoup kullanarak HTML içeriğini parse edin
soup = BeautifulSoup(html_content, "html.parser")

# Tüm tabloları bulun
tables = soup.find_all("table")

# Hedef tabloyu bulun
target_table = None
for table in tables:
    for content in table.contents:
        if 'Güncelleme' in content:
            tarih_str = content.split(': ')[1]
            tarih = datetime.strptime(tarih_str, '%d/%m/%Y').strftime('%d%m%Y')
            target_table = table
            break

# Değişim verilerini tutacak bir liste oluşturun
table_data = []
onceki_kurum_sayisi = ''
onceki_ort_hedef = ''
# Tablodaki her bir satırı işleyin ve veritabanına ekleyin
if target_table:
    rows = target_table.find_all('tr')
    for row in rows[1:]:  # İlk satır başlıkları içerdiğinden atlanır
        columns = row.find_all('td')
        kod = columns[1].text.strip()  # Kod sütunu
        hisse_adi = columns[2].text.strip()  # Hisse adı sütunu
        oneri_kurum_sayisi = int(columns[3].text.strip()) # Öneri kurum sayısı sütunu
        son_kapanis = float(columns[4].text.strip())  # Son kapanış sütunu
        ort_hedef = float(columns[5].text.strip())  # Ortalama Hedef sütunu
        ort_getiri = float(columns[6].text.strip())  # Ortalama Hedef sütunu
        
        
        doc = {
            'tarih': tarih,
            'kod': kod,
            'hisse_adi': hisse_adi,
            'oneri_kurum_sayisi': oneri_kurum_sayisi,
            'son_kapanis': son_kapanis,
            'ort_hedef': ort_hedef,
            'ort_getiri': ort_getiri
        }
        
        
        resp = es.index(index="borsa", document=doc)
        print(resp)