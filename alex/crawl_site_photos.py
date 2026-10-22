"""Crawl packaging websites, pull product images into Alex's photo library (alex_photos), caption them with the 35B vision model.
Usage: venv/bin/python crawl_site_photos.py https://site1 https://site2 ...   (re-runnable: skips URLs/hashes already stored)"""
import sys, os, re, io, gzip, hashlib, time, base64, json, collections, urllib.parse as up
import requests, pymysql
from bs4 import BeautifulSoup
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
from common import BASE, KEY, MODEL, parse_json, DB

DEST = '/root/livechat2/alex/photos'
MAX_PAGES = int(os.environ.get('MAX_PAGES', 400))
MAX_IMAGES = int(os.environ.get('MAX_IMAGES', 700))
UA = {'User-Agent': 'Mozilla/5.0 (compatible; AlexPhotoBot/1.0; +https://chatlive.agency)'}
SKIP_IMG = re.compile(r'logo|icon|sprite|banner|flag|payment|badge|avatar|favicon|arrow|star|rating|trust|paypal|visa|master|button|loader|spinner|placeholder|blank|pixel|\.svg|\.gif|\.webp\?|emoji|whatsapp|chat|social|facebook|instagram|twitter|linkedin|youtube|pinterest', re.I)
SKIP_PAGE = re.compile(r'\.(jpg|jpeg|png|gif|webp|pdf|zip|css|js|xml|txt|ico|svg|mp4|woff2?)(\?|$)|/wp-json|/feed|/cart|/checkout|/account|/login|/tag/|/author/|\?s=|/search|/page/\d+|add-to-cart|/wp-admin|/cdn-cgi|mailto:|tel:', re.I)

db = pymysql.connect(**DB); cur = db.cursor()
cur.execute('SELECT original_name FROM alex_photos WHERE website LIKE %s', ('site:%',))
known_urls = {r[0] for r in cur.fetchall()}
cur.execute("SELECT id FROM alex_photos WHERE id LIKE 'site-%'")
known_hashes = {r[0] for r in cur.fetchall()}
print('already stored:', len(known_urls), flush=True)

def norm_host(u):
    return up.urlsplit(u).netloc.lower().removeprefix('www.')

def crawl(root):
    host = norm_host(root)
    seen, queue, imgs = set(), collections.deque([root]), {}
    s = requests.Session(); s.headers.update(UA)
    while queue and len(seen) < MAX_PAGES and len(imgs) < MAX_IMAGES * 3:
        url = queue.popleft()
        key = url.split('#')[0].rstrip('/')
        if key in seen: continue
        seen.add(key)
        try:
            r = s.get(url, timeout=25, allow_redirects=True)
            if r.status_code != 200 or 'text/html' not in r.headers.get('content-type', ''): continue
        except Exception as e:
            continue
        soup = BeautifulSoup(r.text, 'lxml')
        title = (soup.title.string if soup.title and soup.title.string else '').strip()[:150]
        h1 = soup.find(['h1'])
        heading = h1.get_text(' ', strip=True)[:150] if h1 else ''
        slug = up.urlsplit(url).path.strip('/').split('/')[-1][:60]
        for img in soup.find_all('img'):
            src = img.get('data-src') or img.get('data-lazy-src') or img.get('src') or ''
            ss = img.get('data-srcset') or img.get('srcset') or ''
            if ss:  # take the largest candidate
                cands = [c.strip().split(' ') for c in ss.split(',') if c.strip()]
                try:
                    best = max(cands, key=lambda c: int(re.sub(r'\D', '', c[1])) if len(c) > 1 and re.sub(r'\D', '', c[1]) else 0)
                    src = best[0] or src
                except Exception:
                    pass
            if not src or src.startswith('data:'): continue
            full = up.urljoin(url, src).split('#')[0]
            if norm_host(full) not in (host,) and not norm_host(full).endswith('.' + host) and 'cdn' not in norm_host(full) and 'cloudfront' not in norm_host(full) and 'shopify' not in norm_host(full): continue
            if SKIP_IMG.search(full): continue
            try:
                w, h = int(re.sub(r'\D', '', str(img.get('width') or '0')) or 0), int(re.sub(r'\D', '', str(img.get('height') or '0')) or 0)
            except Exception:
                w = h = 0
            if (w and w < 250) or (h and h < 250): continue
            alt = (img.get('alt') or '').strip()[:200]
            if full not in imgs:
                imgs[full] = {'alt': alt, 'title': title, 'heading': heading, 'page': url, 'slug': slug}
        for a in soup.find_all('a', href=True):
            link = up.urljoin(url, a['href']).split('#')[0]
            if norm_host(link) != host or SKIP_PAGE.search(link): continue
            if link.rstrip('/') not in seen: queue.append(link)
        time.sleep(0.25)
    print(f'{host}: {len(seen)} pages, {len(imgs)} candidate images', flush=True)
    return host, imgs

def fetch_image(url):
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code != 200 or len(r.content) < 15_000: return None
        img = Image.open(io.BytesIO(r.content))
        img.load()
        if min(img.size) < 320 or max(img.size) / min(img.size) > 3.2: return None
        if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
            rgba = img.convert('RGBA'); bg = Image.new('RGB', rgba.size, (255, 255, 255)); bg.paste(rgba, mask=rgba.split()[3]); img = bg
        img = img.convert('RGB')
        img.thumbnail((1600, 1600))
        buf = io.BytesIO(); img.save(buf, 'JPEG', quality=88)
        return buf.getvalue()
    except Exception:
        return None

def caption(jpeg, meta):
    img = Image.open(io.BytesIO(jpeg)); img.thumbnail((768, 768))
    buf = io.BytesIO(); img.save(buf, 'JPEG', quality=80)
    data = 'data:image/jpeg;base64,' + base64.b64encode(buf.getvalue()).decode()
    ctx = f"Website context: page title \"{meta['title']}\", heading \"{meta['heading']}\", image alt text \"{meta['alt']}\"."
    prompt = ("This image comes from a custom packaging (boxes) company website. " + ctx + " Return ONLY JSON: "
              "{\"is_product_photo\": true/false (true only if it shows a packaging box / pouch / label / bag product photo, render or mockup that a sales agent could send to a customer as an example; false for logos, icons, banners with text, people, diagrams, screenshots, blog illustrations, certificates), "
              "\"caption\": one descriptive sentence (box type, material, colours, finish, closure, printed branding, industry), "
              "\"tags\": comma-separated 8-15 search keywords}")
    r2 = requests.post(f'{BASE}/chat/completions', headers={'Authorization': f'Bearer {KEY}'}, timeout=120, json={
        'model': MODEL, 'max_tokens': 300, 'temperature': 0, 'chat_template_kwargs': {'enable_thinking': False},
        'messages': [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': data}}, {'type': 'text', 'text': prompt}]}]})
    r2.raise_for_status()
    return parse_json(r2.json()['choices'][0]['message']['content']) or {}

def process(item):
    host, url, meta = item
    if url in known_urls: return None
    jpeg = fetch_image(url)
    if not jpeg: return None
    hid = 'site-' + hashlib.sha1(jpeg).hexdigest()[:20]
    if hid in known_hashes: return None
    known_hashes.add(hid)
    try:
        j = caption(jpeg, meta)
    except Exception as e:
        print('caption failed', url[:80], e, flush=True); j = {}
    dst = f'{DEST}/{hid}.gz'
    with gzip.open(dst, 'wb') as f: f.write(jpeg)
    tags = ', '.join(t for t in [str(j.get('tags', ''))[:800], 'website', host, meta['slug'].replace('-', ' ')] if t)
    return (hid, f'site:{host}', url[:500], 'image/jpeg', len(jpeg), dst, str(j.get('caption', ''))[:1000], tags[:1000], 1 if j.get('is_product_photo') else 0)

total = collections.Counter()
for root in sys.argv[1:]:
    host, imgs = crawl(root)
    items = [(host, u, m) for u, m in list(imgs.items())[:MAX_IMAGES * 2] if u not in known_urls]
    kept = 0
    with ThreadPoolExecutor(4) as ex:
        for res in ex.map(process, items):
            if not res: continue
            cur.execute('INSERT IGNORE INTO alex_photos (id, website, original_name, mime, size, path, caption, tags, is_product) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)', res)
            db.commit(); kept += 1; total[host] += 1; total['product'] += res[8]
            if kept % 25 == 0: print(f'{host}: stored {kept}', flush=True)
            if kept >= MAX_IMAGES: break
    print(f'{host}: done, stored {kept}', flush=True)
cur.execute("SELECT website, is_product, COUNT(*) FROM alex_photos WHERE website LIKE 'site:%' GROUP BY website, is_product")
print('summary', cur.fetchall(), flush=True)
