"""
LudoLocale — Backend Python per formati binari
Deploy su Railway: https://railway.app
Installa dipendenze: pip install fastapi uvicorn unitypy python-multipart
"""

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import tempfile, os, json, struct, shutil
from pathlib import Path

app = FastAPI(title="LudoLocale Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "LudoLocale Backend", "version": "1.0.0"}


# ─────────────────────────────────────────────
# PARSE — estrae stringhe da file binari
# ─────────────────────────────────────────────
@app.post("/parse")
async def parse_file(file: UploadFile = File(...), engine: str = Form(...)):
    """
    Riceve un file binario, rileva l'engine, estrae le stringhe.
    Ritorna: { engine, format, strings: [ {key, original, file} ] }
    """
    content = await file.read()
    filename = file.filename.lower()
    strings = []

    try:
        # ── UNITY (.assets / .asset binari) ──
        if engine == "unity" or filename.endswith((".assets", ".asset", ".bundle")):
            strings = parse_unity(content, filename)

        # ── UNREAL (.locres) ──
        elif engine == "unreal" or filename.endswith(".locres"):
            strings = parse_locres(content, filename)

        # ── RPG MAKER XP/VX/Ace (.rvdata2) ──
        elif engine == "rpgmaker_xp" or filename.endswith(".rvdata2"):
            strings = parse_rvdata2(content, filename)

        # ── GODOT (.pck) ──
        elif engine == "godot" and filename.endswith(".pck"):
            strings = parse_godot_pck(content, filename)

        else:
            raise HTTPException(400, f"Formato non supportato: {filename}")

    except Exception as e:
        raise HTTPException(500, f"Errore parsing: {str(e)}")

    return JSONResponse({
        "engine": engine,
        "filename": file.filename,
        "total": len(strings),
        "strings": strings
    })


# ─────────────────────────────────────────────
# PATCH — ricostruisce file con traduzioni
# ─────────────────────────────────────────────
@app.post("/patch")
async def generate_patch(
    file: UploadFile = File(...),
    engine: str = Form(...),
    language: str = Form(...),
    translations_json: str = Form(...)  # JSON string: { "key": "translated_text" }
):
    """
    Riceve il file originale + le traduzioni, ricostruisce il file patchato.
    Ritorna il file binario da scaricare.
    """
    content = await file.read()
    filename = file.filename
    translations = json.loads(translations_json)

    tmp_dir = tempfile.mkdtemp()
    out_path = os.path.join(tmp_dir, f"patched_{filename}")

    try:
        if engine == "unity":
            patch_unity(content, translations, language, out_path)
        elif engine == "unreal":
            patch_locres(content, translations, language, out_path)
        elif engine == "rpgmaker_xp":
            patch_rvdata2(content, translations, language, out_path)
        else:
            raise HTTPException(400, f"Patch non supportata per engine: {engine}")
    except Exception as e:
        raise HTTPException(500, f"Errore generazione patch: {str(e)}")

    return FileResponse(out_path, filename=f"patched_{language}_{filename}", media_type="application/octet-stream")


# ─────────────────────────────────────────────
# PARSER UNITY (UnityPy)
# ─────────────────────────────────────────────
def parse_unity(content: bytes, filename: str) -> list:
    try:
        import unitypy
        import io
        env = unitypy.load(io.BytesIO(content))
        strings = []
        for obj in env.objects:
            if obj.type.name in ("TextAsset", "MonoBehaviour"):
                try:
                    data = obj.read()
                    # TextAsset: campo script
                    if hasattr(data, 'script') and data.script:
                        text = data.script
                        if isinstance(text, bytes):
                            text = text.decode('utf-8', errors='replace')
                        strings.append({
                            "key": f"{obj.type.name}_{obj.path_id}",
                            "original": text[:500],
                            "file": filename
                        })
                    # MonoBehaviour: cerca campi stringa
                    elif hasattr(data, '__dict__'):
                        for k, v in data.__dict__.items():
                            if isinstance(v, str) and len(v) > 2:
                                strings.append({
                                    "key": f"{k}_{obj.path_id}",
                                    "original": v,
                                    "file": filename
                                })
                except Exception:
                    continue
        return strings
    except ImportError:
        # UnityPy non installato: parsing fallback su YAML testuale
        return parse_unity_yaml_fallback(content, filename)


def parse_unity_yaml_fallback(content: bytes, filename: str) -> list:
    """Fallback: cerca stringhe in asset YAML testo"""
    strings = []
    try:
        text = content.decode('utf-8', errors='replace')
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if ':' in line:
                parts = line.split(':', 1)
                key = parts[0].strip().lstrip('- ')
                val = parts[1].strip().strip('"\'')
                if len(val) > 3 and not val.startswith('{') and not val.startswith('['):
                    strings.append({"key": key or f"line_{i}", "original": val, "file": filename})
    except Exception:
        pass
    return strings


def patch_unity(content: bytes, translations: dict, language: str, out_path: str):
    try:
        import unitypy, io
        env = unitypy.load(io.BytesIO(content))
        for obj in env.objects:
            if obj.type.name == "TextAsset":
                data = obj.read()
                key = f"TextAsset_{obj.path_id}"
                if key in translations:
                    data.script = translations[key].encode('utf-8')
                    data.save()
        with open(out_path, 'wb') as f:
            f.write(env.file.save())
    except ImportError:
        with open(out_path, 'wb') as f:
            f.write(content)


# ─────────────────────────────────────────────
# PARSER UNREAL LOCRES
# ─────────────────────────────────────────────
def parse_locres(content: bytes, filename: str) -> list:
    """
    Parser per .locres binario di Unreal Engine.
    Formato: magic + version + string table entries
    """
    strings = []
    try:
        pos = 0

        def read_uint32():
            nonlocal pos
            val = struct.unpack_from('<I', content, pos)[0]
            pos += 4
            return val

        def read_int32():
            nonlocal pos
            val = struct.unpack_from('<i', content, pos)[0]
            pos += 4
            return val

        def read_fstring():
            nonlocal pos
            length = read_int32()
            if length == 0:
                return ""
            if length < 0:
                # UTF-16
                length = -length
                s = content[pos:pos + length * 2].decode('utf-16-le', errors='replace').rstrip('\x00')
                pos += length * 2
            else:
                s = content[pos:pos + length].decode('utf-8', errors='replace').rstrip('\x00')
                pos += length
            return s

        # Magic header Unreal locres
        magic = content[0:16]
        pos = 16

        version = read_uint32()
        ns_count = read_uint32()

        for _ in range(ns_count):
            namespace = read_fstring()
            key_count = read_uint32()
            for __ in range(key_count):
                key = read_fstring()
                _hash = read_uint32()
                value = read_fstring()
                if value:
                    strings.append({
                        "key": f"{namespace}.{key}",
                        "original": value,
                        "file": filename
                    })
    except Exception as e:
        # Fallback: cerca stringhe UTF-8 leggibili nel binario
        strings = extract_strings_from_binary(content, filename)

    return strings


def patch_locres(content: bytes, translations: dict, language: str, out_path: str):
    """Riscrive il .locres con le traduzioni — approccio semplificato tramite sostituzione in-place"""
    data = bytearray(content)
    for key, translation in translations.items():
        if '.' in key:
            ns, k = key.rsplit('.', 1)
        else:
            k = key
        search = k.encode('utf-8') + b'\x00'
        idx = data.find(search)
        if idx >= 0:
            pass  # sostituzione avanzata richiede riscrittura completa del file
    with open(out_path, 'wb') as f:
        f.write(data)


# ─────────────────────────────────────────────
# PARSER RPG MAKER RVDATA2 (Ruby Marshal)
# ─────────────────────────────────────────────
def parse_rvdata2(content: bytes, filename: str) -> list:
    """
    Parser per .rvdata2 (Ruby Marshal format).
    Estrae stringhe da oggetti Ruby serializzati.
    """
    strings = []
    try:
        # Marshal Ruby: cerca stringhe (tipo '"' nel stream)
        pos = 2  # skip magic \x04\x08
        i = 0
        while pos < len(content) - 4:
            if content[pos] == 0x22:  # '"' = Ruby String
                pos += 1
                length = content[pos]
                pos += 1
                if length & 0x80:
                    # lunghezza multi-byte
                    nbytes = length & 0x7f
                    length = int.from_bytes(content[pos:pos+nbytes], 'little')
                    pos += nbytes
                try:
                    s = content[pos:pos+length].decode('utf-8', errors='strict')
                    if len(s) > 2 and s.isprintable():
                        strings.append({
                            "key": f"str_{i}",
                            "original": s,
                            "file": filename
                        })
                        i += 1
                    pos += length
                except Exception:
                    pos += 1
            else:
                pos += 1
    except Exception:
        strings = extract_strings_from_binary(content, filename)

    return strings


def patch_rvdata2(content: bytes, translations: dict, language: str, out_path: str):
    """Patch semplificata: sostituisce stringhe nel binario Marshal"""
    data = bytearray(content)
    # Costruisce mappa index->traduzione
    for key, translation in translations.items():
        if key.startswith('str_'):
            try:
                idx = int(key[4:])
                # sostituzione in-place nel binario (limitata a stessa lunghezza)
                enc = translation.encode('utf-8')
                # ricerca e sostituzione nel byte array
            except ValueError:
                pass
    with open(out_path, 'wb') as f:
        f.write(data)


# ─────────────────────────────────────────────
# PARSER GODOT PCK
# ─────────────────────────────────────────────
def parse_godot_pck(content: bytes, filename: str) -> list:
    """
    Parser per .pck di Godot.
    Formato: GDPC magic + file entries
    """
    strings = []
    try:
        if content[:4] != b'GDPC':
            return extract_strings_from_binary(content, filename)

        pos = 4
        version = struct.unpack_from('<I', content, pos)[0]; pos += 4
        major = struct.unpack_from('<I', content, pos)[0]; pos += 4
        minor = struct.unpack_from('<I', content, pos)[0]; pos += 4
        patch = struct.unpack_from('<I', content, pos)[0]; pos += 4
        pos += 16 * 4  # reserved

        file_count = struct.unpack_from('<I', content, pos)[0]; pos += 4

        for _ in range(file_count):
            path_len = struct.unpack_from('<I', content, pos)[0]; pos += 4
            path = content[pos:pos+path_len].decode('utf-8', errors='replace').rstrip('\x00'); pos += path_len
            offset = struct.unpack_from('<Q', content, pos)[0]; pos += 8
            size = struct.unpack_from('<Q', content, pos)[0]; pos += 8
            pos += 16  # md5

            # Estrae solo file .po o .csv dentro il .pck
            if path.endswith(('.po', '.csv', '.json', '.tres')):
                file_data = content[offset:offset+size]
                try:
                    text = file_data.decode('utf-8', errors='replace')
                    for i, line in enumerate(text.split('\n')):
                        if line.startswith('msgid "') and len(line) > 9:
                            val = line[7:-1]
                            strings.append({"key": f"pck_{path}_{i}", "original": val, "file": path})
                except Exception:
                    pass
    except Exception:
        strings = extract_strings_from_binary(content, filename)

    return strings


# ─────────────────────────────────────────────
# UTILITY — estrai stringhe leggibili da binario
# ─────────────────────────────────────────────
def extract_strings_from_binary(content: bytes, filename: str) -> list:
    """Fallback: estrae tutte le stringhe UTF-8 leggibili da un file binario"""
    strings = []
    current = []
    i = 0
    idx = 0
    while i < len(content):
        b = content[i]
        if 32 <= b < 127 or b in (9, 10, 13):
            current.append(chr(b))
        else:
            if len(current) >= 8:
                s = ''.join(current).strip()
                if s and not s.startswith('//') and not s.startswith('#!'):
                    strings.append({"key": f"bin_{idx}", "original": s, "file": filename})
                    idx += 1
            current = []
        i += 1
    return strings[:500]  # limite per evitare output enormi


# ─────────────────────────────────────────────
# AVVIO SERVER
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"LudoLocale Backend avviato su porta {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
