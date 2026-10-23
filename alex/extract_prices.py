"""Pull every real price an agent quoted (original livechat DB) with the customer's context, structure it with the 35B, write /root/livechat2/alex/prices.json."""
import json, pymysql, re
from concurrent.futures import ThreadPoolExecutor
from common import chat, parse_json, DB
db = pymysql.connect(user='root', unix_socket='/var/run/mysqld/mysqld.sock', database='livechat', charset='utf8mb4'); cur = db.cursor()
cur.execute("""SELECT m.id, m.conversation_id, m.body, m.created_at, w.name FROM messages m JOIN conversations c ON c.id=m.conversation_id JOIN websites w ON w.id=c.website_id
               WHERE m.sender_type='AGENT' AND m.body REGEXP '[$][0-9]' ORDER BY m.created_at""")
rows = cur.fetchall(); print('price messages', len(rows), flush=True)
def ctx(conv_id, before):
    cur2 = db.cursor()
    cur2.execute("SELECT sender_type, body FROM messages WHERE conversation_id=%s AND created_at<=%s AND kind='TEXT' ORDER BY created_at DESC LIMIT 14", (conv_id, before))
    return '\n'.join(f"{'Customer' if s=='VISITOR' else 'Agent'}: {(b or '')[:300]}" for s, b in reversed(cur2.fetchall()))
def work(r):
    mid, cid, body, created, site = r
    transcript = ctx(cid, created)
    prompt = ("From this packaging sales chat, extract every price the AGENT quoted in the LAST agent message as JSON: {\"quotes\":[{\"product\":\"e.g. mailer box / rigid box / perfume box / pizza box / pouch / label\",\"style_material\":\"short, e.g. 16pt cardstock full colour, rigid magnetic, kraft corrugated\",\"size\":\"LxWxH if stated else null\",\"quantity\":number or null,\"unit_price_usd\":number or null,\"total_usd\":number or null,\"includes\":\"shipping/printing/design if stated\",\"note\":\"ballpark/firm/discount context, max 12 words\"}]} . Use null for unknown. Only prices for boxes/packaging, skip setup-cost remarks without a product. Return ONLY JSON.\n\nChat:\n" + transcript)
    try:
        j = parse_json(chat([{'role': 'user', 'content': prompt}], max_tokens=500)) or {}
        out = []
        for q in j.get('quotes', []) or []:
            q['site'] = site; q['date'] = str(created)[:10]; q['source_msg'] = body[:240]; out.append(q)
        return out
    except Exception as e:
        print('fail', mid, e, flush=True); return []
quotes = []
for r in rows: quotes.extend(work(r))  # single connection: no threads
quotes = [q for q in quotes if q.get('product') and (q.get('unit_price_usd') or q.get('total_usd'))]
json.dump({'generated': __import__('datetime').date.today().isoformat(), 'count': len(quotes), 'quotes': quotes}, open('/root/livechat2/alex/prices.json', 'w'), ensure_ascii=False, indent=1)
print('quotes kept', len(quotes), flush=True)
