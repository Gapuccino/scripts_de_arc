import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()
ARC_ACCESS_TOKEN = os.getenv('ARC_ACCESS_TOKEN')
ORG_ID = os.getenv('ORG_ID')
if not (ARC_ACCESS_TOKEN and ORG_ID):
    print('Faltan ARC_ACCESS_TOKEN u ORG_ID en .env')
    raise SystemExit(1)

API_BASE_URL = f"https://api.{ORG_ID}.arcpublishing.com/content/v4/search/published"

csv_path = 'todos_los_videos_para_eliminar_fayerwayer.csv'

ids = []
with open(csv_path, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None)
    for i, row in enumerate(reader):
        if i >= 5:
            break
        if row:
            ids.append(row[0])

session = requests.Session()
session.headers.update({'Authorization': f'Bearer {ARC_ACCESS_TOKEN}', 'Content-Type': 'application/json'})

for vid in ids:
    params = {'q': f'_id:{vid}', 'size': 1, 'website': 'fayerwayer', '_sourceInclude': '_id,type,publish_date,headlines'}
    r = session.get(API_BASE_URL, params=params, timeout=30)
    try:
        r.raise_for_status()
    except Exception as e:
        print(f'Error fetching {vid}:', e, r.text[:200])
        continue
    data = r.json()
    elems = data.get('content_elements', [])
    if not elems:
        print(f'{vid}: not found in API response')
        continue
    el = elems[0]
    t = el.get('type')
    pd = el.get('publish_date')
    title = el.get('headlines', {}).get('basic') if el.get('headlines') else None
    print(f'{vid} -> type={t} publish_date={pd} title={title}')
