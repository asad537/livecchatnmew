"""Re-fetch crawled photos whose source has transparency and flatten them on white (JPEG conversion had made them black)."""
import io, gzip, os, pymysql, requests
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
from common import DB
UA = {'User-Agent': 'Mozilla/5.0 (compatible; AlexPhotoBot/1.0; +https://chatlive.agency)'}
db = pymysql.connect(**DB); cur = db.cursor()
cur.execute("SELECT id, original_name, path FROM alex_photos WHERE website LIKE 'site:%'")
rows = cur.fetchall(); print('rows', len(rows), flush=True)
def work(r):
    pid, url, path = r
    try:
        rr = requests.get(url, headers=UA, timeout=30)
        if rr.status_code != 200: return 'fetchfail'
        img = Image.open(io.BytesIO(rr.content)); img.load()
        has_alpha = img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info)
        if not has_alpha: return 'opaque'
        img = img.convert('RGBA')
        bg = Image.new('RGB', img.size, (255, 255, 255)); bg.paste(img, mask=img.split()[3])
        bg.thumbnail((1600, 1600))
        buf = io.BytesIO(); bg.save(buf, 'JPEG', quality=88)
        tmp = path + '.tmp'
        with gzip.open(tmp, 'wb') as f: f.write(buf.getvalue())
        os.replace(tmp, path)
        wm = f'/root/livechat2/alex/photos-wm/{pid}.gz'
        if os.path.exists(wm): os.remove(wm)
        return 'fixed'
    except Exception as e:
        return 'error'
from collections import Counter
c = Counter()
with ThreadPoolExecutor(6) as ex:
    for res in ex.map(work, rows):
        c[res] += 1
        if sum(c.values()) % 200 == 0: print(dict(c), flush=True)
print('done', dict(c), flush=True)
