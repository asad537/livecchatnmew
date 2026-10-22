import fs from 'node:fs';
import sharp from 'sharp';
const llm = process.env.ALEX_BASE_URL, key = process.env.ALEX_API_KEY, model = process.env.ALEX_MODEL;
const [inp, out] = process.argv.slice(2);
const png = fs.readFileSync(inp);
const { width: W, height: H } = await sharp(png).metadata();
const ask = async (text) => {
  const r = await fetch(`${llm}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${key}` }, body: JSON.stringify({
    model, max_tokens: 200, temperature: 0,
    messages: [{ role: 'user', content: [{ type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } }, { type: 'text', text }] }],
    chat_template_kwargs: { enable_thinking: false } }) });
  const j = await r.json(); return j.choices?.[0]?.message?.content ?? '';
};
const a = await ask(`Locate the closed gift box (only the box itself, not the table or decor) in the LARGE top-left photo. Output ONLY JSON {"bbox_2d":[x1,y1,x2,y2]} in absolute pixel coordinates of this ${W}x${H} image.`);
console.log('pixels:', a.slice(0, 200));
const b = await ask(`Locate the closed gift box (only the box itself, not the table or decor) in the LARGE top-left photo. Output ONLY JSON {"bbox_2d":[x1,y1,x2,y2]} with coordinates normalized to 0-1000.`);
console.log('norm1000:', b.slice(0, 200));
const parse = (t) => { try { return JSON.parse((t.match(/\{[\s\S]*\}/) ?? ['{}'])[0]).bbox_2d; } catch { return null; } };
const pa = parse(a), pb = parse(b);
const rect = (bb, color, scale) => bb ? `<rect x="${bb[0] * scale[0]}" y="${bb[1] * scale[1]}" width="${(bb[2] - bb[0]) * scale[0]}" height="${(bb[3] - bb[1]) * scale[1]}" fill="none" stroke="${color}" stroke-width="4"/>` : '';
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">${rect(pa, 'red', [1, 1])}${rect(pb, 'lime', [W / 1000, H / 1000])}</svg>`;
fs.writeFileSync(out, await sharp(png).composite([{ input: Buffer.from(svg), top: 0, left: 0 }]).png().toBuffer());
console.log('red=pixels lime=norm1000 →', out);
