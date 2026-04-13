# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║              C H R O N O G R A P H I X                      ║
║   Génération automatique + Upload Instagram (Mode GitHub)    ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, sys, time, shutil, subprocess, random, string, urllib.parse, hashlib
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from tqdm import tqdm
from PIL import Image, ImageDraw, ImageFont
from concurrent.futures import ThreadPoolExecutor, as_completed

# Force la console GitHub à accepter les caractères spéciaux (é, ═, ❌, ✅)
sys.stdout.reconfigure(encoding='utf-8')

# Désactiver numba JIT pour éviter les conflits serveurs
os.environ["NUMBA_DISABLE_JIT"] = "1"
from moviepy.editor import (
    AudioFileClip, ImageClip, concatenate_videoclips, 
    CompositeVideoClip, ImageSequenceClip
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⚙️  CONFIGURATION (CHEMINS RELATIFS POUR GITHUB)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ROOT = os.getcwd()

CSV_FILE           = os.path.join(ROOT, "histoire_test.csv")
OUTPUT_VIDEOS      = os.path.join(ROOT, "IG_Video_montee")     
OUTPUT_UPLOADED    = os.path.join(ROOT, "IG_Video_uploaded")   
OUTPUT_IMAGES      = os.path.join(ROOT, "images")
TEMP_DIR           = Path(os.path.join(ROOT, "_temp"))

# ── RÉCUPÉRATION DES SECRETS GITHUB ──
INSTAGRAM_ACCESS_TOKEN = os.environ.get("IG_TOKEN")
INSTAGRAM_BUSINESS_ID  = os.environ.get("IG_BUSINESS_ID")
PEXELS_KEY             = os.environ.get("PEXELS_KEY")
CLOUDINARY_CLOUD_NAME  = os.environ.get("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_API_KEY     = os.environ.get("CLOUDINARY_API_KEY")
CLOUDINARY_API_SECRET  = os.environ.get("CLOUDINARY_API_SECRET")

# ── Piper TTS ──
PIPER_EXE  = os.path.join(ROOT, "piper", "piper.exe")
VOICES_DIR = os.path.join(ROOT, "piper", "voices")
PIPER_VOICES = ["en_US-ryan-high.onnx", "en_US-lessac-high.onnx"]

# ── Pexels & Vidéo ──
IMAGES_PER_VIDEO = 5
VIDEO_W, VIDEO_H = 1080, 1920
FPS              = 24
KB_FPS           = 12
FONT_PATH        = os.path.join(ROOT, "Bebas_Neue", "BebasNeue-Regular.ttf")
FONT_SIZE        = 110
WORDS_GROUP      = 2

_whisper_model = None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🛡️  VÉRIFICATION DE L'ENVIRONNEMENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def check_environment():
    print(f"\n{'═'*58}\n  🔍  VÉRIFICATION DE L'ENVIRONNEMENT GITHUB\n{'═'*58}")
    
    missing_secrets = []
    if not INSTAGRAM_ACCESS_TOKEN: missing_secrets.append("IG_TOKEN")
    if not INSTAGRAM_BUSINESS_ID: missing_secrets.append("IG_BUSINESS_ID")
    if not PEXELS_KEY: missing_secrets.append("PEXELS_KEY")
    if not CLOUDINARY_CLOUD_NAME: missing_secrets.append("CLOUDINARY_CLOUD_NAME")
    if not CLOUDINARY_API_KEY: missing_secrets.append("CLOUDINARY_API_KEY")
    if not CLOUDINARY_API_SECRET: missing_secrets.append("CLOUDINARY_API_SECRET")
    
    if missing_secrets:
        print(f"  ❌  ERREUR FATALE : Secrets manquants : {', '.join(missing_secrets)}")
        sys.exit(1)
    
    if not os.path.exists(PIPER_EXE):
        print(f"  ❌  ERREUR FATALE : piper.exe introuvable dans {PIPER_EXE}")
        sys.exit(1)
        
    print("  ✅  Variables d'environnement et fichiers vitaux trouvés.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🌐  MODULE 1 — CONNEXION API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def verify_instagram_connection_and_get_existing_posts():
    url_info = f"https://graph.instagram.com/v19.0/{INSTAGRAM_BUSINESS_ID}?fields=id,username,media_count&access_token={INSTAGRAM_ACCESS_TOKEN}"
    try:
        r_info = requests.get(url_info).json()
        if 'error' in r_info:
            print(f"  ❌  Erreur API Instagram : {r_info['error'].get('message', r_info['error'])}")
            sys.exit(1)
            
        print(f"  ✅  Connecté avec succès au compte : @{r_info.get('username', 'Inconnu')}")
        
        url_media = f"https://graph.instagram.com/v19.0/{INSTAGRAM_BUSINESS_ID}/media?fields=caption&limit=50&access_token={INSTAGRAM_ACCESS_TOKEN}"
        r_media = requests.get(url_media).json()
        
        existing_captions = []
        if 'data' in r_media:
            for item in r_media['data']:
                if 'caption' in item:
                    existing_captions.append(item['caption'].strip().lower())
                    
        print(f"  ✅  {len(existing_captions)} anciennes publications analysées (Anti-doublons).")
        return existing_captions
    except Exception as e:
        print(f"  ❌  Impossible de contacter Instagram : {e}")
        sys.exit(1)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ☁️  MODULE 3 — UPLOAD CLOUD & INSTAGRAM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _safe_filename(path):
    """Nom de fichier sans espaces ni caractères spéciaux (Instagram-safe)."""
    name = os.path.splitext(os.path.basename(path))[0]
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
    return safe.strip("_") + ".mp4"

def _try_cloudinary(video_path):
    if not CLOUDINARY_CLOUD_NAME:
        return None
    try:
        timestamp = int(time.time())
        params_to_sign = f"timestamp={timestamp}"
        signature = hashlib.sha1(
            f"{params_to_sign}{CLOUDINARY_API_SECRET}".encode("utf-8")
        ).hexdigest()
        upload_url = f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD_NAME}/video/upload"
        with open(video_path, "rb") as f:
            r = requests.post(
                upload_url,
                data={"api_key": CLOUDINARY_API_KEY, "timestamp": timestamp, "signature": signature},
                files={"file": (_safe_filename(video_path), f, "video/mp4")},
                timeout=600,
            )
        data = r.json()
        secure_url = data.get("secure_url")
        if secure_url:
            return secure_url
        print(f"       Cloudinary erreur : {data.get('error', {}).get('message', r.text[:150])}")
    except Exception as e:
        print(f"       Cloudinary exception : {e}")
    return None

def _try_pixeldrain(video_path):
    try:
        with open(video_path, "rb") as f:
            r = requests.post(
                "https://pixeldrain.com/api/file",
                files={"file": (_safe_filename(video_path), f, "video/mp4")},
                timeout=300,
            )
        file_id = r.json().get("id")
        if file_id:
            return f"https://pixeldrain.com/api/file/{file_id}"
        print(f"       Pixeldrain : pas d'ID : {r.text[:100]}")
    except Exception as e:
        print(f"       Pixeldrain exception : {e}")
    return None

def _try_litterbox(video_path):
    try:
        with open(video_path, "rb") as f:
            r = requests.post(
                "https://litterbox.catbox.moe/resources/internals/api.php",
                data={"reqtype": "fileupload", "time": "72h"},
                files={"fileToUpload": (_safe_filename(video_path), f, "video/mp4")},
                timeout=300,
            )
        if r.status_code == 200 and r.text.startswith("https://"):
            return r.text.strip()
        print(f"       Litterbox refusé : {r.text[:100]}")
    except Exception as e:
        print(f"       Litterbox exception : {e}")
    return None

def upload_video_to_cloud(video_path):
    print(f"  ☁️  Envoi de la vidéo sur le cloud temporaire...")
    providers = [
        ("Cloudinary",  _try_cloudinary),
        ("Pixeldrain",  _try_pixeldrain),
        ("Litterbox",   _try_litterbox),
    ]
    for name, fn in providers:
        print(f"       🔄  Tentative : {name}...")
        url = fn(video_path)
        if url:
            print(f"  ✅  Lien cloud obtenu via {name} : {url}")
            return url
        time.sleep(2)
    print("  ❌  Tous les hébergeurs ont échoué.")
    return None

def upload_to_instagram_reels(video_url: str, caption: str) -> str | None:
    print(f"  📤  Upload vers Instagram depuis l'URL : {video_url}")
    container_endpoint = f"https://graph.instagram.com/v19.0/{INSTAGRAM_BUSINESS_ID}/media"
    payload = {'media_type': 'REELS', 'video_url': video_url, 'caption': caption, 'access_token': INSTAGRAM_ACCESS_TOKEN}

    try:
        print(f"       ⬆️   Étape 1 : Création du conteneur...")
        r = requests.post(container_endpoint, data=payload)
        res = r.json()
        
        if 'id' not in res:
            print(f"  ❌  Erreur API (Conteneur) : {res}")
            return None
            
        creation_id = res['id']
        print(f"       ⏳  Étape 2 : Traitement vidéo par Instagram...")
        status_url = f"https://graph.instagram.com/v19.0/{creation_id}"
        
        for _ in range(15):
            time.sleep(10)
            status_res = requests.get(status_url, params={'fields': 'status_code', 'access_token': INSTAGRAM_ACCESS_TOKEN}).json()
            status = status_res.get('status_code')
            if status == 'FINISHED': break
            elif status == 'ERROR':
                print(f"  ❌  Instagram a rejeté la vidéo.")
                return None
        
        print("       🚀  Étape 3 : Publication finale...")
        publish_endpoint = f"https://graph.instagram.com/v19.0/{INSTAGRAM_BUSINESS_ID}/media_publish"
        publish_res = requests.post(publish_endpoint, data={'creation_id': creation_id, 'access_token': INSTAGRAM_ACCESS_TOKEN}).json()
        
        if 'id' in publish_res:
            print(f"  🎉  SUCCÈS ! Vidéo publiée. (IG ID: {publish_res['id']})")
            return publish_res['id']
    except Exception as e:
        print(f"  ❌  Erreur réseau/API : {e}")
    return None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🛠  MODULE 4 — HELPERS ET CSV
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def ensure_dir(path): Path(path).mkdir(parents=True, exist_ok=True)

def safe_name(s):
    for c in r'\/:*?"<>|#': s = s.replace(c, "_")
    return s.strip().strip("_")

def load_csv(filepath):
    raw_lines = None
    for enc in ["cp1252", "utf-8-sig", "latin-1", "utf-8"]:
        try:
            with open(filepath, "r", encoding=enc) as f: raw_lines = f.readlines()
            break
        except UnicodeDecodeError: continue
    if raw_lines is None: sys.exit(1)

    rows, columns = [], None
    for line in raw_lines:
        line = line.strip()
        if not line: continue
        if line.startswith('"') and line.endswith('"'): line = line[1:-1]
        line = line.replace('""', '\x00')
        parts = [p.replace('\x00', '"') for p in line.split(";")]
        if columns is None: columns = [c.strip() for c in parts]
        else:
            if len(parts) > len(columns):
                extra = len(parts) - len(columns)
                parts = parts[:2] + [";".join(parts[2:2+extra+1])] + parts[2+extra+1:]
            row = {col: (parts[i].strip() if i < len(parts) else "") for i, col in enumerate(columns)}
            rows.append(row)
    return pd.DataFrame(rows, columns=columns)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎙  MODULES AUDIO ET IMAGES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_audio(text, out_wav, row_id):
    """Génère l'audio avec capture détaillée de l'erreur pour Piper et forçage du dossier de travail"""
    voice_name = random.choice(PIPER_VOICES)
    voice = os.path.join(VOICES_DIR, voice_name)
    
    if not os.path.exists(voice): 
        print(f"  ❌  Erreur : Le fichier de voix '{voice_name}' est introuvable !")
        return False
        
    if not os.path.exists(voice + ".json"):
        print(f"  ❌  Erreur : Le fichier de configuration '{voice_name}.json' est manquant !")
        return False

    cmd = [PIPER_EXE, "--model", voice, "--output_file", out_wav]
    try:
        # L'ajout CRUCIAL pour que Windows trouve les .dll dans le dossier de Piper
        res = subprocess.run(
            cmd, 
            input=text.encode("utf-8"), 
            capture_output=True, 
            timeout=120,
            cwd=os.path.dirname(PIPER_EXE)  
        )
        
        if res.returncode != 0:
            erreur_stderr = res.stderr.decode('utf-8', errors='ignore').strip()
            erreur_stdout = res.stdout.decode('utf-8', errors='ignore').strip()
            
            message = erreur_stderr if erreur_stderr else erreur_stdout
            if not message:
                message = f"Crash fatal (Code {res.returncode}). Fichiers DLL manquants ou erreur d'exécution dans {os.path.dirname(PIPER_EXE)} ?"
                
            print(f"  ❌  Erreur interne Piper : {message}")
            return False
            
        return True
    except Exception as e: 
        print(f"  ❌  Crash critique de Piper : {e}")
        return False

def fetch_pexels_images(query, save_dir, n=5):
    headers = {"Authorization": PEXELS_KEY}
    to_dl, seen, page = [], set(), 1
    while len(to_dl) < n and page <= 5:
        try:
            r = requests.get("https://api.pexels.com/v1/search", headers=headers, params={"query": query, "orientation": "portrait", "per_page": 15, "page": page}, timeout=15)
            photos = r.json().get("photos", [])
            if not photos: break
            for p in photos:
                if len(to_dl) >= n: break
                url = p["src"].get("portrait") or p["src"].get("large2x")
                if url and url not in seen:
                    seen.add(url)
                    to_dl.append((url, Path(save_dir) / f"img_{len(to_dl):02d}.jpg"))
            page += 1
        except: break

    saved = []
    def _dl_one(args):
        url, path = args
        try:
            with open(path, "wb") as f: f.write(requests.get(url, timeout=20).content)
            return str(path)
        except: return None
        
    print("  📷  Téléchargement des images Pexels...")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_dl_one, a): a for a in to_dl}
        for fut in as_completed(futures):
            r = fut.result()
            if r: saved.append(r)
    saved.sort()
    return saved

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎬  MODULES VIDEO ET SOUS-TITRES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def smart_crop(img_path):
    img = Image.open(img_path).convert("RGB")
    w, h = img.size
    ratio = VIDEO_W / VIDEO_H
    if w / h > ratio:
        nw = int(h * ratio)
        img = img.crop(((w - nw) // 2, 0, (w + nw) // 2, h))
    else:
        nh = int(w / ratio)
        img = img.crop((0, 0, w, nh))
    return np.array(img.resize((VIDEO_W, VIDEO_H), Image.LANCZOS))

def make_ken_burns_clip(img_path, duration):
    arr = smart_crop(img_path)
    n = max(2, int(duration * KB_FPS))
    frames = []
    for i in range(n):
        zoom = 1.0 + 0.15 * (i / max(1, n - 1))
        nw, nh = int(VIDEO_W / zoom), int(VIDEO_H / zoom)
        x1, y1 = (VIDEO_W - nw) // 2, (VIDEO_H - nh) // 2
        frames.append(np.array(Image.fromarray(arr[y1:y1+nh, x1:x1+nw]).resize((VIDEO_W, VIDEO_H), Image.BILINEAR)))
    return ImageSequenceClip(frames, fps=KB_FPS).set_duration(duration)

def get_word_timestamps(wav_path):
    global _whisper_model
    try: 
        import whisper
        if _whisper_model is None: 
            print("  🤖  Chargement du modèle d'IA Whisper...")
            _whisper_model = whisper.load_model("base")
        result = _whisper_model.transcribe(wav_path, language="en", word_timestamps=True, verbose=False)
        return [{"word": w["word"].strip(), "start": float(w["start"]), "end": float(w["end"])} for seg in result.get("segments", []) for w in seg.get("words", []) if w.get("word", "").strip()]
    except Exception as e: 
        print(f"Erreur Whisper : {e}")
        return None

def make_subtitle_frame(text):
    img  = Image.new("RGBA", (VIDEO_W, VIDEO_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    try: font = ImageFont.truetype(FONT_PATH, FONT_SIZE)
    except: font = ImageFont.load_default()
    line = text.strip()
    try: bbox = font.getbbox(line); text_w = bbox[2] - bbox[0]; text_h = bbox[3] - bbox[1]
    except: text_w, text_h = int(font.getlength(line)), FONT_SIZE

    x = (VIDEO_W - text_w) // 2
    y = (VIDEO_H - text_h) // 2
    draw.text((x, y), line, font=font, fill=(255, 255, 255, 255), stroke_width=5, stroke_fill=(0, 0, 0, 255))
    return np.array(img)

def build_video(audio_path, image_paths, output_path):
    audio_clip = AudioFileClip(audio_path)
    total_dur = audio_clip.duration
    img_dur = total_dur / len(image_paths)
    
    print("  🎬  Génération des animations...")
    clips = [make_ken_burns_clip(p, img_dur) for p in image_paths]
    base = concatenate_videoclips(clips, method="compose")
    
    print("  📝  Génération des sous-titres...")
    word_data = get_word_timestamps(audio_path)
    subs = []
    if word_data:
        for i in range(0, len(word_data), WORDS_GROUP):
            chunk = word_data[i:i + WORDS_GROUP]
            txt = " ".join(w["word"] for w in chunk)
            s_clip = ImageClip(make_subtitle_frame(txt)).set_start(chunk[0]["start"]).set_duration(max(0.05, chunk[-1]["end"]-chunk[0]["start"]))
            subs.append(s_clip)

    final = CompositeVideoClip([base] + subs, size=(VIDEO_W, VIDEO_H)).set_audio(audio_clip).set_duration(total_dur)
    
    print("  💾  Rendu final (export en cours)...")
    final.write_videofile(
        output_path, fps=FPS, codec="libx264", audio_codec="aac",
        preset="ultrafast", bitrate="3000k", audio_bitrate="192k",
        threads=2, logger=None,
        # ── Paramètres obligatoires pour l'API Instagram Reels ──
        ffmpeg_params=[
            "-pix_fmt",   "yuv420p",   # seul format accepté par Instagram
            "-profile:v", "high",       # profil H.264 requis pour 1080p
            "-level",     "4.0",        # level requis pour les Reels
            "-movflags",  "+faststart", # metadata en tête du fichier
        ],
    )
    
    final.close()
    audio_clip.close()
    return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔄  MODULE PIPELINE LIGNE PAR LIGNE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_row(row, existing_captions):
    row_id = str(row.get("id", "")).strip().zfill(4)
    titre = str(row.get("Titre_Video", "")).strip() or f"short_{row_id}"
    script = str(row.get("Script_Audio", "")).strip()
    tags = str(row.get("Tags", "")).strip()
    description = str(row.get("Description", "")).strip()
    queries_raw = str(row.get("Search_Queries", "")).strip() or titre

    print(f"\n{'═'*58}\n  📌  #{row_id} — {titre}\n{'═'*58}")
    
    titre_lower = titre.lower()
    for caption in existing_captions:
        if titre_lower in caption:
            return "duplicate"

    if not script: return False

    ensure_dir(OUTPUT_VIDEOS)
    sf = safe_name(titre)
    img_dir = Path(OUTPUT_IMAGES) / f"{row_id}_{sf}"
    row_temp = TEMP_DIR / f"row_{row_id}"
    ensure_dir(img_dir); ensure_dir(row_temp)

    wav_path = str(row_temp / f"{row_id}.wav")
    video_name = f"{row_id}_{sf}.mp4"
    video_path = os.path.join(OUTPUT_VIDEOS, video_name)

    print("  🎙️  Synthèse vocale...")
    if not generate_audio(script, wav_path, row_id): return False
    
    queries = [q.strip() for q in queries_raw.replace("|", ",").split(",") if q.strip()]
    image_paths = fetch_pexels_images(queries[0], img_dir, IMAGES_PER_VIDEO)
    if not image_paths: return False

    if not build_video(wav_path, image_paths, video_path): return False

    tag_list = [t.strip() for t in tags.replace(",", " ").split() if t.strip()]
    hashtags = " ".join(t if t.startswith("#") else f"#{t}" for t in tag_list)
    instagram_caption = f"{titre}\n\n{description}\n\n{hashtags}"

    final_video_url = upload_video_to_cloud(video_path)
    if not final_video_url:
        shutil.rmtree(row_temp, ignore_errors=True)
        return False
    
    ig_id = upload_to_instagram_reels(final_video_url, instagram_caption)
    
    if ig_id:
        ensure_dir(OUTPUT_UPLOADED)
        try: shutil.move(video_path, os.path.join(OUTPUT_UPLOADED, os.path.basename(video_path)))
        except: pass

    shutil.rmtree(row_temp, ignore_errors=True)
    return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🚀  MAIN — ONE SHOT (déclenché par le cron YML)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("\n" + "="*58)
    print("      C H R O N O G R A P H I X   ( G I T H U B )")
    print("      Mode one-shot — publie et quitte")
    print("="*58)

    check_environment()

    for d in [OUTPUT_VIDEOS, OUTPUT_UPLOADED, OUTPUT_IMAGES, TEMP_DIR]:
        ensure_dir(d)

    existing_captions = verify_instagram_connection_and_get_existing_posts()
    df = load_csv(CSV_FILE)
    rows = [row.to_dict() for _, row in df.iterrows()]

    print(f"\n  {len(rows)} videos chargees depuis le CSV.")

    # Parcourir les lignes jusqu'à trouver une vidéo non publiée
    for i, row in enumerate(rows):
        try:
            ok = process_row(row, existing_captions=existing_captions)
            if ok == "duplicate":
                print(f"  Doublon — passage a la video suivante.")
                continue
            elif ok:
                print(f"\n  Publication reussie. Fin du script.")
                break
            else:
                print(f"  Echec de la publication. Fin du script.")
                break
        except Exception as e:
            print(f"\n  Erreur inattendue ligne {i} : {e}")
            break

    # Nettoyage
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    shutil.rmtree(OUTPUT_IMAGES, ignore_errors=True)
    print(f"\n{'='*58}\n  RUN TERMINE\n{'='*58}\n")

if __name__ == "__main__":
    main()
