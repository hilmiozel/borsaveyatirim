from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# Define constants
URL = 'https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/default.aspx'
URL1_TEMP = 'https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/sirket-karti.aspx?hisse={hisse}'
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
                    'stock': hisse_kodu,
                    'year': year,
                    'pe': fk,
                    'EBITDA': fdfavök,
                    'EBITDA_Sale_Ratio': fdsatis,
                    'pb_ratio': pddd,
                    'reporttype': result
                }
                
            resp = es.index(index="borsa_is_finansaloranlartahmin", document=doc2)
            print("Doc2:",doc2)
            
    doc3=''
    yurtdisi_fk_prim_oran=''
    yurtdisi_pddd_prim_oran=''
    yurtdisi_fdfavök_prim_oran=''
    tarihsel_fk_prim_oran=''
    tarihsel_pddd_prim_oran=''
    tarihsel_fdfavök_prim_oran=''
    if yurtdisiiskonto_table:
        rows = yurtdisiiskonto_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            if len(columns)>3:
                kod = columns[0].text.strip()  # Kod sütunu
                son_fiyat = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                fk = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_fk_prim_oran = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            if len(columns)==6:
                pddd = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_pddd_prim_oran = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            elif len(columns)==8:
                fdfavök = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_fdfavök_prim_oran = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                pddd = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_pddd_prim_oran = float(columns[7].text.strip().replace('.', '').replace(',', '.')) if columns[7].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
    
    if tarihselortalamalar_table:
        rows = tarihselortalamalar_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            if len(columns)>2:
                kod = columns[0].text.strip()  # Kod sütunu
                fk = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_fk_prim_oran = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            if len(columns)==5:
                pddd = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_pddd_prim_oran = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            elif len(columns)==7:
                fdfavök = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_fdfavök_prim_oran = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                pddd = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_pddd_prim_oran = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
    
    if tarihselortalamalar_table or yurtdisiiskonto_table:
    
        doc3 = {
                "timestamp": datetime.now(),
                'stock': kod,
                'closing_price': son_fiyat,
                'estimated_pe_ratio': fk,
                'estimated_pe_discount_foreign': yurtdisi_fk_prim_oran,
                'estimated_pe_discount_historaical': tarihsel_fk_prim_oran,
                'estimated_pb_ratio': pddd,
                'estimated_pb_discount_foreign': yurtdisi_pddd_prim_oran,
                'estimated_pb_discount_historaical': tarihsel_pddd_prim_oran,
                'estimated_ebitda_ratio': fdfavök,
                'estimated_ebitda_discount_foreign': yurtdisi_fdfavök_prim_oran,
                'estimated_ebitda_discount_historaical': tarihsel_fdfavök_prim_oran,
            }
    
    if doc3:
        resp = es.index(index="borsa_is_finansaloranlartahmin", document=doc3)
