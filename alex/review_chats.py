"""Nightly self-review: judge every Alex conversation of the last N hours with the 35B, write a markdown report + rule suggestions.
Usage: venv/bin/python review_chats.py [hours=26]"""
import sys, json, pymysql, datetime, os, re
from concurrent.futures import ThreadPoolExecutor
from common import chat, parse_json, DB
hours = int(sys.argv[1]) if len(sys.argv) > 1 else 26
db = pymysql.connect(**DB); cur = db.cursor()
cur.execute("""SELECT DISTINCT c.id FROM conversations c JOIN messages m ON m.conversation_id=c.id
               WHERE m.sender_type='BOT' AND m.created_at > DATE_SUB(NOW(), INTERVAL %s HOUR)""", (hours,))
convs = [r[0] for r in cur.fetchall()]
RULES = open('/root/livechat2/alex/review_rules.md').read() if os.path.exists('/root/livechat2/alex/review_rules.md') else ''
def transcript(cid):
    c2 = pymysql.connect(**DB).cursor()  # own connection per call: pymysql is not thread-safe
    c2.execute("SELECT sender_type, kind, body FROM messages WHERE conversation_id=%s AND kind IN ('TEXT','FILE') AND (agent_only IS NULL OR agent_only=0) ORDER BY created_at", (cid,))
    rows = c2.fetchall()
    if sum(1 for s, k, b in rows if s == 'VISITOR') < 2: return None
    return '\n'.join(f"{i+1}. {'Customer' if s=='VISITOR' else ('Alex' if s=='BOT' else 'Agent')}: {('[file: '+(b or '')+']') if k=='FILE' else (b or '')[:400]}" for i, (s, k, b) in enumerate(rows))
def judge(cid):
    t = transcript(cid)
    if not t: return None
    prompt = ("You are the QA lead for Alex, an AI live-chat sales agent of a custom packaging company. Review this chat and return ONLY JSON: "
              "{\"score\":1-10,\"issues\":[{\"msg\":number,\"problem\":\"short\",\"better\":\"the reply Alex should have given (max 25 words)\",\"rule\":\"one-line generic rule to prevent this, or null\"}],\"good\":\"one line on what went well\"}. "
              "Judge: understood the customer, no repeated/unnecessary questions, no invented facts (prices, lead times, specs), honest about what it did (photos/mockups actually sent), short replies, correct language mirroring, asked for contact before promising a quote, kept the product type, handled complaints with a brief apology. " + ("Known rules: " + RULES[:3000]) + "\n\nChat:\n" + t)
    try:
        j = parse_json(chat([{'role': 'user', 'content': prompt}], max_tokens=900)) or {}
        if isinstance(j, list): j = next((x for x in j if isinstance(x, dict)), {})
        if not isinstance(j, dict): j = {}
        j['conversation'] = cid; j['turns'] = t.count('\n') + 1
        return j
    except Exception as e:
        return {'conversation': cid, 'error': str(e)}
results = []
with ThreadPoolExecutor(4) as ex:
    for r in ex.map(judge, convs):
        if r: results.append(r)
os.makedirs('/root/alex/review', exist_ok=True)
day = datetime.date.today().isoformat()
scores = [r['score'] for r in results if isinstance(r.get('score'), (int, float))]
rules = {}
for r in results:
    for i in r.get('issues', []) or []:
        if i.get('rule'): rules[i['rule'].strip()] = rules.get(i['rule'].strip(), 0) + 1
md = [f"# Alex self-review {day} (last {hours}h)", f"Conversations: {len(results)}  Avg score: {sum(scores)/len(scores):.1f}" if scores else "No scored conversations", "", "## Suggested rules (by frequency)"]
for rule, n in sorted(rules.items(), key=lambda x: -x[1])[:25]: md.append(f"- ({n}) {rule}")
md.append("\n## Conversations")
for r in sorted(results, key=lambda x: x.get('score', 0)):
    md.append(f"\n### {r['conversation']}  score {r.get('score')}  turns {r.get('turns')}")
    if r.get('good'): md.append(f"good: {r['good']}")
    for i in r.get('issues', []) or []: md.append(f"- msg {i.get('msg')}: {i.get('problem')}  → better: {i.get('better')}")
    if r.get('error'): md.append(f"error: {r['error']}")
open(f'/root/alex/review/{day}.md', 'w').write('\n'.join(md))
json.dump({'day': day, 'results': results}, open(f'/root/alex/review/{day}.json', 'w'), ensure_ascii=False, indent=1)
print(f"reviewed {len(results)} conversations, avg {sum(scores)/len(scores):.1f}" if scores else f"reviewed {len(results)}", flush=True)
print('\n'.join(md[:30]))
