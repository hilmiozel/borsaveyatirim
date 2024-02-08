from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# Define constants
URL = 'https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/default.aspx'
URL1_TEMP = 'https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/sirket-karti.aspx?hisse={hisse}'
URL2_TEMP = 'https://www.yapikredi.com.tr/yatirimci-kosesi/hisse-senedi/{hisse}'
USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)

# URL'den sayfa içeriğini getirin
response = requests.get(URL)
html_content = response.text

soup = BeautifulSoup(html_content, "html.parser")

# Tüm tabloları bulun
tables = soup.find_all("table")

tum_hisseler = []

for table in tables:
    attrs = table.attrs
    if 'data-csvname' in attrs and 'class' in attrs:
        if attrs['data-csvname'] == 'tumhisse' and 'excelexport' in attrs['class']:
            tum_hisse_table = table
            target_table = table
            rows = target_table.find_all('tr')
            for row in rows[1:]:
                columns = row.find_all('td')
                if columns:
                    tum_hisseler.append(columns[0].text.strip().split()[0])

for hisse_kodu in tum_hisseler:
    print("Processing:", hisse_kodu)
    URL1 = URL1_TEMP.format(hisse=hisse_kodu)
    URL2 = URL2_TEMP.format(hisse=hisse_kodu)
    # URL'den sayfa içeriğini getirin
    response = requests.get(URL2)
    html_content = response.text

    soup = BeautifulSoup(html_content, "html.parser")

    # Tüm tabloları bulun
    tables = soup.find_all("table")
    toplam_islem='' 
    toplam_hacim=''
    a_ortalama=''
    # Hedef tabloyu bulun
    for table in tables:
        attrs = table.attrs
        if 'class' in attrs:
            if 'table-striped' in attrs['class'] and 'table-text-right' in attrs['class']:
                target_table = table
                rows = target_table.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        if columns[0].text.strip() == 'Toplam İşlem Miktarı':
                            toplam_islem=int(columns[1].text.strip().split(',')[0].replace('.', ''))
                        if columns[0].text.strip() == 'Toplam İşlem Hacmi':
                            toplam_hacim=int(columns[1].text.strip().split(',')[0].replace('.', ''))
                        if columns[0].text.strip() == 'A. Ortalama':
                            a_ortalama=float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip() else 0.0
                if toplam_islem and toplam_hacim and a_ortalama:
                    doc1 = {
                        "timestamp": datetime.now(),
                        'kod': hisse_kodu,
                        'gunluk_toplam_islem': toplam_islem,
                        'gunluk_toplam_hacim': toplam_hacim,
                        'a_ortalama': a_ortalama
                    }
                    
                    resp = es.index(index="borsa_yapikredi_daily", document=doc1)
                    break
        
    # URL'den sayfa içeriğini getirin
    response = requests.get(URL1)
    html_content = response.text

    # BeautifulSoup kullanarak HTML içeriğini parse edin
    soup = BeautifulSoup(html_content, "html.parser")

    # Tüm tabloları bulun
    tables = soup.find_all("table")


    # Hedef tabloyu bulun
    finansaloranlartahmin_table = None
    yurtdisiiskonto_table = None
    tarihselortalamalar_table = None
    
    for table in tables:
        attrs = table.attrs
        if 'data-csvname' in attrs and 'class' in attrs:
            if attrs['data-csvname'] == 'finansaloranlartahmin' and 'excelexport' in attrs['class']:
                finansaloranlartahmin_table = table
            if attrs['data-csvname'] == 'yurtdisiiskonto' and 'excelexport' in attrs['class']:
                yurtdisiiskonto_table = table
            if attrs['data-csvname'] == 'tarihselortalamalar' and 'excelexport' in attrs['class']:
                tarihselortalamalar_table = table
            
    # Değişim verilerini tutacak bir liste oluşturun
    finansaloranlartahmin_data = []
    onceki_kurum_sayisi = ''
    # Tablodaki her bir satırı işleyin ve veritabanına ekleyin
    if finansaloranlartahmin_table:
        rows = finansaloranlartahmin_table.find_all('tr')
        for row in rows[1:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            kod = columns[0].text.strip()  # Kod sütunu
            year = 2022 + int(row.attrs['class'][1])
            fk = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            fdfavök = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            fdsatis = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            pddd = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            result = columns[5].text.strip()
            
            doc2 = {
                    "timestamp": datetime.now(),
                    'kod': hisse_kodu,
                    'yil': year,
                    'fk': fk,
                    'fdfavök': fdfavök,
                    'fdsatis': fdsatis,
                    'pddd': pddd,
                    'raportipi': result
                }
                
            resp = es.index(index="borsa_is_finansaloranlartahmin", document=doc2)
            
    doc3=''
    if yurtdisiiskonto_table:
        rows = yurtdisiiskonto_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            kod = columns[0].text.strip()  # Kod sütunu
            son_fiyat = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            fk = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            yurtdisi_fk_prim_oran = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            # fdfavök = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip() else 0.0
            # fdfavök_prim_oran = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip() else 0.0
            # pddd = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip() else 0.0
            # pddd_prim_oran = float(columns[7].text.strip().replace('.', '').replace(',', '.')) if columns[7].text.strip() else 0.0
    
    if tarihselortalamalar_table:
        rows = tarihselortalamalar_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            kod = columns[0].text.strip()  # Kod sütunu
            fk = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            tarihsel_fk_prim_oran = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            # fdfavök = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip() else 0.0
            # fdfavök_prim_oran = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip() else 0.0
            # pddd = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip() else 0.0
            # pddd_prim_oran = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip() else 0.0
    
    if tarihselortalamalar_table and yurtdisiiskonto_table:
    
        doc3 = {
                "timestamp": datetime.now(),
                'kod': kod,
                'son_fiyat': son_fiyat,
                'fk': fk,
                'yurtdisi_fk_prim_oran': yurtdisi_fk_prim_oran,
                'tarihsel_fk_prim_oran': tarihsel_fk_prim_oran
            }
            
    
    elif tarihselortalamalar_table:
        
        doc3 = {
                "timestamp": datetime.now(),
                'kod': kod,
                'fk': fk,
                'tarihsel_fk_prim_oran': tarihsel_fk_prim_oran
            }
            
        
    elif yurtdisiiskonto_table:
        
        doc3 = {
                "timestamp": datetime.now(),
                'kod': kod,
                'son_fiyat': son_fiyat,
                'fk': fk,
                'yurtdisi_fk_prim_oran': yurtdisi_fk_prim_oran
            }
            
    
    if doc3:
        resp = es.index(index="borsa_is_iskontolar", document=doc3)
    
