// Direct Qwen-Image render test (same workflow as Alex). Usage: node sheet_test.mjs out.png steps "prompt"
import fs from 'node:fs';
const base = process.env.ALEX_COMFY_URL;
const [out, stepsArg, prompt] = process.argv.slice(2);
const steps = Number(stepsArg) === 8 ? 8 : 4;
const seed = Math.floor(Math.random() * 999999999) + 1;
const prefix = `sheet-${Math.random().toString(16).slice(2, 8)}`;
const wq = {
  '1': { class_type: 'UNETLoader', inputs: { unet_name: 'qwen_image_fp8_e4m3fn.safetensors', weight_dtype: 'default' } },
  '2': { class_type: 'CLIPLoader', inputs: { clip_name: 'qwen_2.5_vl_7b_fp8_scaled.safetensors', type: 'qwen_image', device: 'default' } },
  '3': { class_type: 'VAELoader', inputs: { vae_name: 'qwen_image_vae.safetensors' } },
  '11': { class_type: 'LoraLoaderModelOnly', inputs: { lora_name: steps === 8 ? 'Qwen-Image-Lightning-8steps-V2.0-bf16.safetensors' : 'Qwen-Image-Lightning-4steps-V2.0-bf16.safetensors', strength_model: 1.0, model: ['1', 0] } },
  '12': { class_type: 'ModelSamplingAuraFlow', inputs: { shift: 3.1, model: ['11', 0] } },
  '4': { class_type: 'CLIPTextEncode', inputs: { text: prompt, clip: ['2', 0] } },
  '5': { class_type: 'CLIPTextEncode', inputs: { text: ' ', clip: ['2', 0] } },
  '6': { class_type: 'EmptySD3LatentImage', inputs: { width: 1344, height: 1024, batch_size: 1 } },
  '8': { class_type: 'KSampler', inputs: { seed, steps, cfg: 1.0, sampler_name: 'euler', scheduler: 'simple', denoise: 1.0, model: ['12', 0], positive: ['4', 0], negative: ['5', 0], latent_image: ['6', 0] } },
  '9': { class_type: 'VAEDecode', inputs: { samples: ['8', 0], vae: ['3', 0] } },
  '10': { class_type: 'SaveImage', inputs: { filename_prefix: prefix, images: ['9', 0] } },
};
const t0 = Date.now();
const r = await fetch(`${base}/prompt`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ prompt: wq }) });
const { prompt_id, error } = await r.json();
if (!prompt_id) { console.error('queue failed', JSON.stringify(error).slice(0, 300)); process.exit(1); }
for (;;) {
  await new Promise((res) => setTimeout(res, 2000));
  const h = await (await fetch(`${base}/history/${prompt_id}`)).json();
  const item = h[prompt_id];
  if (!item) continue;
  if (item.status?.status_str === 'error') { console.error('comfy error'); process.exit(1); }
  const img = Object.values(item.outputs ?? {}).flatMap((o) => o.images ?? [])[0];
  if (!img) continue;
  const b = await (await fetch(`${base}/view?filename=${encodeURIComponent(img.filename)}&subfolder=${encodeURIComponent(img.subfolder ?? '')}&type=output`)).arrayBuffer();
  fs.writeFileSync(out, Buffer.from(b));
  console.log(`saved ${out} seed=${seed} steps=${steps} ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  break;
}
