
import wbgapi as wb
from datetime import datetime
from elasticsearch import Elasticsearch
import time

USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)

# Güncellenmiş veriyi tutmak için yeni bir liste oluştur
updated_data_list = []

for report in ['NY.GDP.MKTP.CD', 'NY.GDP.PCAP.CD', 'NY.GDP.MKTP.KD.ZG', 'SL.UEM.TOTL.ZS', 'FP.CPI.TOTL.ZG', 'NE.TRD.GNFS.ZS', 'BN.CAB.XOKA.GD.ZS', 'NE.CON.PRVT.KD.ZG','NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS', 'BN.CAB.XOKA.GD.ZS']:
    data_list = list(wb.data.fetch(report, 'TUR', mrv=5))
    series_info = wb.series.info(report)

    # Her bir sözlükte 'TUR' anahtarını 'Türkiye' ile değiştir ve güncellenmiş sözlüğü yeni listeye ekle
    for row in data_list:
        row['economy'] = 'Türkiye'
        row['series'] = series_info.items[0]['value']
        now=datetime.now()
        row["timestamp"]= now.isoformat()
        resp = es.index(index="turkey_major_economics_info", document=row)
        print(resp.body)
        
time.sleep(5)

