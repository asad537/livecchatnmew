"""Flag crawled photos that show a packaging company's own logo/name/URL (not a customer brand) and blurry ones. Re-runnable."""
import io, gzip, base64, pymysql, requests
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
from common import BASE, KEY, MODEL, parse_json, DB
db = pymysql.connect(**DB); cur = db.cursor()
cur.execute("SHOW COLUMNS FROM alex_photos LIKE 'company_logo'")
if not cur.fetchone():
    cur.execute("ALTER TABLE alex_photos ADD COLUMN company_logo TINYINT DEFAULT NULL, ADD COLUMN blurry TINYINT DEFAULT NULL"); db.commit()
cur.execute("SELECT id, path FROM alex_photos WHERE website LIKE 'site:%' AND company_logo IS NULL")
rows = cur.fetchall(); print('to screen', len(rows), flush=True)
def work(r):
    pid, path = r
    try:
        img = Image.open(io.BytesIO(gzip.open(path).read())).convert('RGB'); w, h = img.size
        img.thumbnail((768, 768)); buf = io.BytesIO(); img.save(buf, 'JPEG', quality=85)
        data = 'data:image/jpeg;base64,' + base64.b64encode(buf.getvalue()).decode()
        prompt = ("Look at this packaging product photo. Return ONLY JSON: {\"company_logo\": true/false — true if a PACKAGING COMPANY's own logo, company name, website address or watermark is visible "
                  "(e.g. 'TheCustomBoxes', 'My Box Printing', 'premiumboxes.com', a 'smart packaging solutions' tagline, a sample logo mark of the printer placed on the box); false if the box only shows a customer brand, generic text like 'custom mailer packaging' without a company logo, or no text; "
                  "\"blurry\": true/false — true if the image is clearly low resolution, pixelated or blurry}")
        r2 = requests.post(f'{BASE}/chat/completions', headers={'Authorization': f'Bearer {KEY}'}, timeout=120, json={
            'model': MODEL, 'max_tokens': 80, 'temperature': 0, 'chat_template_kwargs': {'enable_thinking': False},
            'messages': [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': data}}, {'type': 'text', 'text': prompt}]}]})
        j = parse_json(r2.json()['choices'][0]['message']['content']) or {}
        return (1 if j.get('company_logo') else 0, 1 if j.get('blurry') or min(w, h) < 450 else 0, pid)
    except Exception as e:
        print('fail', pid, e, flush=True); return None
n = 0
with ThreadPoolExecutor(4) as ex:
    for res in ex.map(work, rows):
        if not res: continue
        cur.execute('UPDATE alex_photos SET company_logo=%s, blurry=%s WHERE id=%s', res); db.commit(); n += 1
        if n % 100 == 0: print('screened', n, flush=True)
cur.execute("SELECT website, company_logo, blurry, COUNT(*) FROM alex_photos WHERE website LIKE 'site:%' GROUP BY website, company_logo, blurry"); print('summary', cur.fetchall(), flush=True)
