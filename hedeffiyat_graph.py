import sqlite3
import matplotlib.pyplot as plt
from tkinter import Tk, StringVar, OptionMenu, Button
from datetime import datetime

DATABASE_NAME = "borsaveyatirim.db"
TABLE_NAME = "hedef_fiyat"
# Veritabanı bağlantısı oluşturun
conn = sqlite3.connect(DATABASE_NAME)
cursor = conn.cursor()

# Veritabanındaki tüm hisse kodlarını alın
cursor.execute(f'''SELECT DISTINCT kod FROM {TABLE_NAME}''')
hisse_kodlari = cursor.fetchall()
hisse_kodlari = [kod[0] for kod in hisse_kodlari]

# Tkinter penceresi oluşturun
root = Tk()
root.title("Hisse Kodu Seçin")

# Kullanıcının seçebileceği bir hisse kodu seçme alanı oluşturun
selected_hisse_kodu = StringVar(root)
selected_hisse_kodu.set(hisse_kodlari[0])  # Varsayılan olarak ilk hisse kodunu seçin
hisse_kodu_menu = OptionMenu(root, selected_hisse_kodu, *hisse_kodlari)
hisse_kodu_menu.pack()

# Grafiği çizdirmek için bir fonksiyon tanımlayın
def plot_graph():
    # Hisse senedi verilerini sorgulayın
    cursor.execute(f'''SELECT tarih, oneri_kurum_sayisi, ort_hedef, son_kapanis FROM {TABLE_NAME} WHERE kod = ?''', (selected_hisse_kodu.get(),))
    results = cursor.fetchall()

    # Tarih, öneri kurum sayısı ve ortalama hedef fiyat verilerini ayırın
    tarihler = [row[0] for row in results]
    oneri_kurum_sayilari = [row[1] for row in results]
    ortalama_hedef_fiyat = [row[2] for row in results]
    kapanis_fiyat = [row[3] for row in results]

    # Grafiği çizdirin
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Sol y eksenine öneri kurum sayısı verilerini çizdirin (sütun grafiği)
    ax1.bar(tarihler, oneri_kurum_sayilari, color='skyblue', label='Öneri Kurum Sayısı', width=0.1)
    ax1.set_xlabel('Tarih')
    ax1.set_ylabel('Öneri Kurum Sayısı', color='skyblue')
    ax1.tick_params(axis='y', labelcolor='skyblue')
    ax1.grid(True)

    # Sağdaki y eksenine ortalama hedef fiyat verilerini çizdirin
    ax2 = ax1.twinx()
    ax2.plot(tarihler, ortalama_hedef_fiyat, color='orange', marker='s', linestyle='-', label='Ortalama Hedef Fiyat')
    ax2.plot(tarihler, kapanis_fiyat, color='red', marker='s', linestyle='-', label='Kapanış Fiyat')
    ax2.set_ylabel('Ortalama Hedef Fiyat', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')

    # Grafik başlığı ve eksen etiketlerini belirtin
    plt.title(f"{selected_hisse_kodu.get()} Hisse Senedi Öneri Kurum Sayısı ve Ortalama Hedef Fiyatı")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Eksenler arasındaki çakışmayı önlemek için legend() fonksiyonunu çağırın
    fig.legend(loc='upper left')

    # Grafiği gösterin
    plt.show()

# Grafiği çizdirmek için bir düğme ekleyin
plot_button = Button(root, text="Grafiği Çiz", command=plot_graph)
plot_button.pack()

# Tkinter penceresini çalıştırın
root.mainloop()

# Veritabanı bağlantısını kapatın
conn.close()
