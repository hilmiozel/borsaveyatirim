from bs4 import BeautifulSoup
import requests
from datetime import datetime
from elasticsearch import Elasticsearch

USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)

# Define constants
URL = "https://www.borsaveyatirim.com/araci-kurum-hisse-onerileri"
SEARCH_TABLE_ID = "myTable2"

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
    attrs = table.attrs
    if 'id' in attrs and 'class' in attrs:
        if attrs['id'] == SEARCH_TABLE_ID and 'tablesorter' in attrs['class']:
            target_table = table
            for content in table.contents:
                if 'Güncelleme' in content:
                    tarih_str = content.split(': ')[1]
                    tarih = datetime.strptime(tarih_str, '%d/%m/%Y')
            break

# Tablodaki her bir satırı işleyin ve veritabanına ekleyin
if target_table:
    rows = target_table.find_all('tr')
    for row in rows[1:]:  # İlk satır başlıkları içerdiğinden atlanır
        columns = row.find_all('td')
        kod = columns[1].text.strip()  # Kod sütunu
        hisse_adi = columns[2].text.strip()  # Hisse adı sütunu
        modele_ekleyen_kurum_sayisi = int(columns[3].text.strip()) # Öneri kurum sayısı sütunu
        son_kapanis = float(columns[4].text.strip())  # Son kapanış sütunu
        
        doc = {
            "timestamp": datetime.now(),
            'guncelleme_tarihi': tarih,
            'kod': kod,
            'hisse_adi': hisse_adi,
            'modele_ekleyen_kurum_sayisi': modele_ekleyen_kurum_sayisi,
            'son_kapanis': son_kapanis,
        }
        
        
        resp = es.index(index="borsa_araci_kurum_modelportfoy_eklenme", document=doc)
        print(resp)