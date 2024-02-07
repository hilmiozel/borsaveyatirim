from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

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


# Veritabanı bağlantısı oluşturun
conn = sqlite3.connect(DATABASE_NAME)
cursor = conn.cursor()

# Tabloyu oluşturun (varsa, yoksa)
cursor.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    kod TEXT,
                    hisse_adi TEXT,
                    oneri_kurum_sayisi INTEGER,
                    son_kapanis REAL,
                    ort_hedef REAL,
                    ort_getiri REAL,
                    tarih TEXT,
                    PRIMARY KEY (kod, tarih) 
                )''')

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
        
        # Veritabanında mevcut kayıtları sorgula
        cursor.execute(f'''SELECT oneri_kurum_sayisi, ort_hedef
                  FROM {TABLE_NAME} 
                  WHERE kod = ? 
                    AND tarih != ?  
                  ORDER BY tarih DESC 
                  LIMIT 1''', (kod, tarih))
        if cursor.fetchone():
            onceki_kurum_sayisi, onceki_ort_hedef = cursor.fetchone()
        if onceki_kurum_sayisi:
            onceki_kurum_sayisi = int(onceki_kurum_sayisi[0])
            onceki_ort_hedef = float(onceki_ort_hedef[0])
        else:
            onceki_kurum_sayisi = 0
            onceki_ort_hedef = 0
        
        # Değişim miktarını hesapla
        degisim_kurum_sayisi = oneri_kurum_sayisi - onceki_kurum_sayisi
        degisim_ort_hedef = ort_hedef - onceki_ort_hedef
        
        # Değişim verisini table_data listesine ekleyin
        if degisim_kurum_sayisi != 0 or degisim_ort_hedef !=0:
            table_data.append([kod, degisim_kurum_sayisi, degisim_ort_hedef])
        
        cursor.execute(f'''SELECT * FROM {TABLE_NAME} WHERE kod = ? AND tarih = ?''', (kod, tarih))
        existing_record = cursor.fetchone()
        
        if existing_record:
            # Eğer mevcut kayıt varsa, yeni kayıt eklemek yerine güncelleme yapmayalım
            cursor.execute(f'''REPLACE INTO {TABLE_NAME} 
                            (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih, ort_hedef , ort_getiri) 
                            VALUES (?, ?, ?, ?, ?, ?, ?)''', (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih, ort_hedef, ort_getiri))
        else:
            # Eğer mevcut kayıt yoksa, yeni kaydı ekleyelim
            cursor.execute(f'''INSERT INTO {TABLE_NAME}  
                            (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih, ort_hedef , ort_getiri)
                            VALUES (?, ?, ?, ?, ?, ?, ?)''', (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih, ort_hedef, ort_getiri))
        

# Veritabanı bağlantısını kapat
conn.commit()
conn.close()

# colors listesini oluşturun
colors = []

# table_data listesindeki her bir öğeyi kontrol edin ve renkleri atayın
# Define color mapping for changes

color_mapping = { (1, 1): 'green', (1, 0): 'yellow', (1, -1): 'red',
                  (0, 1): 'green', (0, 0): 'yellow', (0, -1): 'red',
                  (-1, 1): 'green', (-1, 0): 'yellow', (-1, -1): 'red' }

# Tablo oluşturma
fig, ax = plt.subplots()
ax.axis('on')  # Eksenleri kapatma

# Tablo başlığı
ax.set_title('Hedef Fiyat Değişimler', fontsize=10)

table = ax.table(cellText=table_data, colLabels=['Kod', 'Kurum Sayısındaki Değişim', 'Hedef Fiyattaki Değişim'], cellLoc='center', loc='center', colColours=['lightgray']*3)

# Hücre renklendirme
for i, (kod, deg1, deg2) in enumerate(table_data, start=1):
    color1 = color_mapping.get((1 if deg1 > 0 else 0 if deg1 == 0 else -1, 1 if deg2 > 0 else 0 if deg2 == 0 else -1))
    color2 = color_mapping.get((1 if deg1 > 0 else 0 if deg1 == 0 else -1, 1 if deg2 > 0 else 0 if deg2 == 0 else -1))
    table[(i, 1)].set_facecolor(color1)
    table[(i, 2)].set_facecolor(color2)

# Create a legend for the colors
legend_elements = [Line2D([0], [0], marker='s', color='w', label='Positive Change', markerfacecolor='green'),
                   Line2D([0], [0], marker='s', color='w', label='Zero Change', markerfacecolor='yellow'),
                   Line2D([0], [0], marker='s', color='w', label='Negative Change', markerfacecolor='red')]

# Add the legend to the plot
plt.legend(handles=legend_elements, loc='upper left')

# Show the plot
plt.show()

