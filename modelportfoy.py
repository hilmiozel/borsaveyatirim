from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Define constants
URL = "https://www.borsaveyatirim.com/araci-kurum-hisse-onerileri"
DATABASE_NAME = "borsaveyatirim.db"
SEARCH_TABLE_ID = "myTable2"
TABLE_NAME = "model_portfoy"

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
                    tarih TEXT,
                    PRIMARY KEY (kod, tarih)
                )''')

# Değişim verilerini tutacak bir liste oluşturun
table_data = []
onceki_kurum_sayisi = ''
# Tablodaki her bir satırı işleyin ve veritabanına ekleyin
if target_table:
    rows = target_table.find_all('tr')
    for row in rows[1:]:  # İlk satır başlıkları içerdiğinden atlanır
        columns = row.find_all('td')
        kod = columns[1].text.strip()  # Kod sütunu
        hisse_adi = columns[2].text.strip()  # Hisse adı sütunu
        oneri_kurum_sayisi = int(columns[3].text.strip()) # Öneri kurum sayısı sütunu
        son_kapanis = float(columns[4].text.strip())  # Son kapanış sütunu
        tarih = datetime.now().strftime("%d%m%Y")  # GGAAYYYY formatında tarih
        
        # Veritabanında mevcut kayıtları sorgula
        cursor.execute(f'''SELECT oneri_kurum_sayisi 
                  FROM {TABLE_NAME} 
                  WHERE kod = ? 
                    AND tarih != ?  
                  ORDER BY tarih DESC 
                  LIMIT 1''', (kod, tarih))
        if cursor.fetchone():
            onceki_kurum_sayisi = cursor.fetchone()
        if onceki_kurum_sayisi:
            onceki_kurum_sayisi = int(onceki_kurum_sayisi[0])
        else:
            onceki_kurum_sayisi = 0
        
        # Değişim miktarını hesapla
        degisim = oneri_kurum_sayisi - onceki_kurum_sayisi
        
        # Değişim verisini table_data listesine ekleyin
        if degisim != 0:
            table_data.append([kod, degisim])
        
        cursor.execute(f'''SELECT * FROM {TABLE_NAME} WHERE kod = ? AND tarih = ?''', (kod, tarih))
        existing_record = cursor.fetchone()
        
        if existing_record:
            # Eğer mevcut kayıt varsa, yeni kayıt eklemek yerine güncelleme yapmayalım
            cursor.execute(f'''REPLACE INTO {TABLE_NAME} 
                            (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih) 
                            VALUES (?, ?, ?, ?, ?)''', (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih))
        else:
            # Eğer mevcut kayıt yoksa, yeni kaydı ekleyelim
            cursor.execute(f'''INSERT INTO {TABLE_NAME} 
                            (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih) 
                            VALUES (?, ?, ?, ?, ?)''', (kod, hisse_adi, oneri_kurum_sayisi, son_kapanis, tarih))
        

# Veritabanı bağlantısını kapat
conn.commit()
conn.close()

# colors listesini oluşturun
colors = []

# table_data listesindeki her bir öğeyi kontrol edin ve renkleri atayın
for kod, deg in table_data:
    if deg > 0:
        colors.append('green')
    elif deg == 0:
        colors.append('yellow')
    else:
        colors.append('red')

# Tablo oluşturma
fig, ax = plt.subplots()
ax.axis('on')  # Eksenleri kapatma

# Tablo başlığı
ax.set_title('Kodlar ve Değişimler', fontsize=10)

table = ax.table(cellText=table_data, colLabels=['Kod', 'Değişim'], cellLoc='center', loc='center', colColours=['lightgray']*2)

# Hücre renklendirme
for i, color in enumerate(colors, start=1):
    table[(i, 1)].set_facecolor(color)

# Tabloyu göster
plt.show() 

