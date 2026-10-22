// Test: Qwen draws unlabeled dimension arrows; the 35B vision model locates them; we print the correct labels.
import fs from 'node:fs';
import sharp from 'sharp';
const base = process.env.ALEX_COMFY_URL, llm = process.env.ALEX_BASE_URL, key = process.env.ALEX_API_KEY, model = process.env.ALEX_MODEL;
const [out] = process.argv.slice(2);
const prompt = 'Professional packaging presentation sheet for a luxury perfume box, one image laid out as a grid of five photos with thin white gutters. Large main photo top-left: a closed black rigid gift box with a full-depth magnetic lid, wider than tall with a slim depth, matte soft-touch black paper, a thin gold foil border line framing the front panel and a large elegant gold foil letter R centered, standing on a cream marble table with blurred gold decor; in this main photo only, three thin white dimension lines with small end ticks are drawn parallel to the box edges: one along the bottom front edge for the length, one along the side edge for the depth, one vertical beside the box for the height, with no numbers and no text on them. Top-right photo: the same box open with its lid lifted up, a golden perfume bottle resting in a black velvet insert. Bottom row, three smaller photos of the same box: seen straight from the front, from a three-quarter angle, and standing upright on its narrow end. Every panel shows the identical box and design. The only printed text on the box is the brand name R in clean lettering; no other letters, initials, monograms, emblems, marks or small secondary lines under the name anywhere on the box, and no numbers, labels or other text anywhere in the image. Every panel shows the identical box; nothing else besides the box, its insert and its product. Photorealistic product photography, warm soft studio light, sharp.';
const seed = Math.floor(Math.random() * 999999999) + 1, prefix = `arr-${Math.random().toString(16).slice(2, 8)}`;
const wq = {
  '1': { class_type: 'UNETLoader', inputs: { unet_name: 'qwen_image_fp8_e4m3fn.safetensors', weight_dtype: 'default' } },
  '2': { class_type: 'CLIPLoader', inputs: { clip_name: 'qwen_2.5_vl_7b_fp8_scaled.safetensors', type: 'qwen_image', device: 'default' } },
  '3': { class_type: 'VAELoader', inputs: { vae_name: 'qwen_image_vae.safetensors' } },
  '11': { class_type: 'LoraLoaderModelOnly', inputs: { lora_name: 'Qwen-Image-Lightning-8steps-V2.0-bf16.safetensors', strength_model: 1.0, model: ['1', 0] } },
  '12': { class_type: 'ModelSamplingAuraFlow', inputs: { shift: 3.1, model: ['11', 0] } },
  '4': { class_type: 'CLIPTextEncode', inputs: { text: prompt, clip: ['2', 0] } },
  '5': { class_type: 'CLIPTextEncode', inputs: { text: ' ', clip: ['2', 0] } },
  '6': { class_type: 'EmptySD3LatentImage', inputs: { width: 1344, height: 1024, batch_size: 1 } },
  '8': { class_type: 'KSampler', inputs: { seed, steps: 8, cfg: 1.0, sampler_name: 'euler', scheduler: 'simple', denoise: 1.0, model: ['12', 0], positive: ['4', 0], negative: ['5', 0], latent_image: ['6', 0] } },
  '9': { class_type: 'VAEDecode', inputs: { samples: ['8', 0], vae: ['3', 0] } },
  '10': { class_type: 'SaveImage', inputs: { filename_prefix: prefix, images: ['9', 0] } },
};
const t0 = Date.now();
const { prompt_id } = await (await fetch(`${base}/prompt`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ prompt: wq }) })).json();
let png;
for (;;) {
  await new Promise((r) => setTimeout(r, 2000));
  const h = await (await fetch(`${base}/history/${prompt_id}`)).json();
  const img = Object.values(h[prompt_id]?.outputs ?? {}).flatMap((o) => o.images ?? [])[0];
  if (!img) continue;
  png = Buffer.from(await (await fetch(`${base}/view?filename=${encodeURIComponent(img.filename)}&subfolder=&type=output`)).arrayBuffer());
  break;
}
console.log(`rendered in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
fs.writeFileSync(out.replace('.png', '-raw.png'), png);
const meta = await sharp(png).metadata();
const W = meta.width, H = meta.height;
const r = await fetch(`${llm}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${key}` }, body: JSON.stringify({
  model, max_tokens: 300, temperature: 0,
  messages: [
    { role: 'system', content: 'You locate drawn dimension lines in product images. Reply with ONLY a JSON object.' },
    { role: 'user', content: [
      { type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } },
      { type: 'text', text: `The image is ${W}x${H} px. In the LARGE main photo (top-left) there are thin white dimension lines drawn along the box edges. Return JSON: {"lines":[{"dim":"length|width|height","x1":..,"y1":..,"x2":..,"y2":..}]} with pixel coordinates of each line's two ends in the full image. "length" = the line along the long bottom front edge, "width" = along the short receding side edge (depth), "height" = the vertical line. Only include lines that really exist in the main photo; at most one per dim.` },
    ] },
  ], chat_template_kwargs: { enable_thinking: false },
}) });
const j = await r.json();
const raw = j.choices?.[0]?.message?.content ?? '';
console.log('vision:', raw.slice(0, 400));
let lines = [];
try { lines = JSON.parse((raw.match(/\{[\s\S]*\}/) ?? ['{}'])[0]).lines ?? []; } catch {}
const labels = { length: '5 in', width: '2 in', height: '4 in' };
const fsz = Math.round(W / 48);
const svgParts = lines.map((l) => {
  const t = labels[l.dim]; if (!t) return '';
  const mx = (Number(l.x1) + Number(l.x2)) / 2, my = (Number(l.y1) + Number(l.y2)) / 2;
  const vertical = Math.abs(l.x1 - l.x2) < Math.abs(l.y1 - l.y2);
  const bw = Math.round(t.length * fsz * 0.65) + fsz, bh = Math.round(fsz * 1.6);
  const bx = vertical ? mx - bw - fsz * 0.4 : mx - bw / 2, by = vertical ? my - bh / 2 : my + fsz * 0.3;
  return `<rect x="${bx}" y="${by}" width="${bw}" height="${bh}" rx="${bh / 3}" fill="rgba(0,0,0,0.6)"/><text x="${bx + bw / 2}" y="${by + bh * 0.72}" text-anchor="middle" font-family="DejaVu Sans" font-size="${fsz}" font-weight="bold" fill="#fff">${t}</text>`;
}).join('');
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">${svgParts}</svg>`;
const outBuf = await sharp(png).composite([{ input: Buffer.from(svg), top: 0, left: 0 }]).png().toBuffer();
fs.writeFileSync(out, outBuf);
console.log(`labels drawn: ${lines.map((l) => l.dim).join(',')} → ${out}`);
