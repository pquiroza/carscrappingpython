import json
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Iterable
from tqdm import tqdm

import firebase_admin
from firebase_admin import credentials, firestore


# =========================
# Config
# =========================
SCHEMA_VERSION = "nissan_specs_v1"
CL_TZ = timezone(timedelta(hours=-3))

# Firestore collection root
ROOT = "brands"  # brands/{brand}/models/{model}/...


# =========================
# Normalizers
# =========================
def norm_space(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_feature_name(s: str) -> str:
    s = norm_space(s)
    s = s.replace("voZ", "voz")
    s = s.replace("TECNOLOGIA", "TECNOLOGÍA")
    s = s.replace("TECNOLOGíA", "TECNOLOGÍA")
    s = s.replace("ITELLIGENT", "INTELLIGENT")
    s = re.sub(r"\b([A-Z]{2,5})\)", r"(\1)", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    return s

def clean_section_name(s: str) -> str:
    s = norm_space(s).upper()
    s = s.replace("TRANSMISION", "TRANSMISIÓN")
    s = s.replace("TECNOLOGIA", "TECNOLOGÍA")

    if s in {"PESO", "DIMENSIONES", "PESO Y DIMENSIONES"}:
        return "PESO Y DIMENSIONES"
    if s in {"CONFORT", "CONFORT Y TECNOLOGÍA", "CONFORT Y TECNOLOGIA"}:
        return "CONFORT Y TECNOLOGÍA"
    if s in {"NISSAN INTELLIGENT", "NISSAN INTELLIGENT MOBILITY", "NISSAN INTELLIGENT MOBILITY NISSAN INTELLIGENT MOBILITY"}:
        return "NISSAN INTELLIGENT MOBILITY"
    return s

def parse_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    s = norm_space(str(v))
    if s in {"●", "Sí", "Si", "SI", "SÍ", "true", "True"}:
        return True
    if s in {"-", "No", "NO", "false", "False"}:
        return False
    return None

def slugify_id(s: str, max_len: int = 120) -> str:
    """
    Firestore doc IDs: evita '/' y caracteres raros
    """
    s = norm_space(s).lower()
    s = re.sub(r"[^\w\- ]+", "", s, flags=re.UNICODE)
    s = s.replace(" ", "-")
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:max_len] or "unknown"

def sha1_str(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def now_dt():
    return datetime.now(CL_TZ)

def guess_brand_model_from_pdf(pdf_path: str) -> Tuple[str, str]:
    # Ajusta a tu caso: aquí asumimos NISSAN por ahora.
    brand = "NISSAN"
    fname = Path(pdf_path).name.upper()
    for m in ["VERSA", "SENTRA", "KICKS", "QASHQAI", "PATHFINDER", "XTRAIL", "X-TRAIL"]:
        if m in fname:
            return brand, m.replace("XTRAIL", "X-TRAIL")
    # fallback
    return brand, slugify_id(Path(pdf_path).stem).upper()


# =========================
# Transform: extractor JSON -> Firestore documents
# =========================
def normalize_extracted_json(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entrada: JSON producido por nissan.py (extractor)
    Salida: formato normalizado para persistir.
    """
    pdf_path = raw.get("pdf", "")
    brand = norm_space(raw.get("brand") or "NISSAN").upper()
    model_guess = norm_space(raw.get("model_guess") or "")
    if not brand or brand == "NISSAN":
        b2, m2 = guess_brand_model_from_pdf(pdf_path)
        brand = brand or b2
        model = m2
    else:
        model = model_guess or guess_brand_model_from_pdf(pdf_path)[1]

    versions = [norm_space(v) for v in (raw.get("versions") or []) if norm_space(v)]
    versions = list(dict.fromkeys(versions))  # unique in order

    base_specs = raw.get("base_specs_or_equipment") or {}
    specs_by_version = raw.get("specs_by_version") or {}
    equip_by_version = raw.get("equipment_by_version") or {}

    # global specs list
    global_specs = []
    for k, v in base_specs.items():
        k2 = clean_feature_name(k)
        v2 = norm_space(v)
        if k2 and v2:
            global_specs.append({
                "section": "ESPECIFICACIONES",
                "name": k2,
                "value_text": v2,
                "value_bool": None,
                "raw": v
            })

    # trims with flat facts
    trims = []
    facts = []  # flatten all facts to save in /facts collection too

    for ver in versions:
        ver_specs = specs_by_version.get(ver, {}) or {}
        ver_equips = equip_by_version.get(ver, {}) or {}

        spec_entries = []
        equip_entries = []

        # specs sections
        for sec, feats in ver_specs.items():
            sec2 = clean_section_name(sec)
            for feat, val in (feats or {}).items():
                feat2 = clean_feature_name(feat)
                val2 = norm_space(val)
                if not feat2 or not val2:
                    continue
                entry = {
                    "section": sec2,
                    "name": feat2,
                    "value_text": val2,
                    "value_bool": None,
                    "raw": val
                }
                spec_entries.append(entry)

        # equipment sections
        for sec, feats in ver_equips.items():
            sec2 = clean_section_name(sec)
            for feat, val in (feats or {}).items():
                feat2 = clean_feature_name(feat)
                if not feat2:
                    continue
                b = parse_bool(val)
                entry = {
                    "section": sec2,
                    "name": feat2,
                    "value_text": None if b is not None else norm_space(str(val)),
                    "value_bool": b,
                    "raw": val
                }
                equip_entries.append(entry)

        trims.append({
            "trim": ver,
            "specs": spec_entries,
            "equipment": equip_entries
        })

        # flatten facts (for querying)
        for e in spec_entries + equip_entries:
            facts.append({
                "brand": brand,
                "model": model,
                "trim": ver,
                "section": e["section"],
                "name": e["name"],
                "value_text": e["value_text"],
                "value_bool": e["value_bool"],
                "raw": e["raw"],
            })

    # add global facts (trim=None)
    for e in global_specs:
        facts.append({
            "brand": brand,
            "model": model,
            "trim": None,
            "section": e["section"],
            "name": e["name"],
            "value_text": e["value_text"],
            "value_bool": e["value_bool"],
            "raw": e["raw"],
        })

    normalized = {
        "schema_version": SCHEMA_VERSION,
        "brand": brand,
        "model": model,
        "pdf": pdf_path,
        "versions": versions,
        "global_specs": global_specs,
        "trims": trims,
        "facts": facts
    }
    return normalized


def build_firestore_docs(normed: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any], List[Tuple[str, Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]]]:
    """
    Retorna:
      brand_id, model_id,
      model_doc,
      trims_docs: list of (trim_id, trim_doc),
      facts_docs: list of (fact_id, fact_doc)
    """
    brand = normed["brand"]
    model = normed["model"]

    brand_id = slugify_id(brand)
    model_id = slugify_id(model)

    extracted_at = firestore.SERVER_TIMESTAMP

    model_doc = {
        "brand": brand,
        "model": model,
        "schema_version": normed.get("schema_version"),
        "source_pdf": Path(normed.get("pdf", "")).name,
        "versions": normed.get("versions", []),
        "global_specs": normed.get("global_specs", []),  # opcional guardar inline
        "updated_at": extracted_at,
    }

    trims_docs = []
    for t in normed.get("trims", []):
        trim_name = t["trim"]
        trim_id = slugify_id(trim_name)
        trim_doc = {
            "brand": brand,
            "model": model,
            "trim": trim_name,
            "schema_version": normed.get("schema_version"),
            "specs": t.get("specs", []),
            "equipment": t.get("equipment", []),
            "updated_at": extracted_at,
        }
        trims_docs.append((trim_id, trim_doc))

    facts_docs = []
    for f in normed.get("facts", []):
        # id estable por contenido clave
        key = f"{brand}|{model}|{f.get('trim')}|{f.get('section')}|{f.get('name')}"
        fact_id = sha1_str(key)  # 40 chars, perfecto para doc id
        fact_doc = {
            "brand": brand,
            "model": model,
            "trim": f.get("trim"),
            "section": f.get("section"),
            "name": f.get("name"),
            "value_text": f.get("value_text"),
            "value_bool": f.get("value_bool"),
            "raw": f.get("raw"),
            "schema_version": normed.get("schema_version"),
            "updated_at": extracted_at,
        }
        facts_docs.append((fact_id, fact_doc))

    return brand_id, model_id, model_doc, trims_docs, facts_docs


# =========================
# Firestore writer
# =========================
def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    return firestore.client()

def chunked(it: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(it), n):
        yield it[i:i+n]

def write_model_to_firestore(db, brand_id: str, model_id: str,
                            model_doc: Dict[str, Any],
                            trims_docs: List[Tuple[str, Dict[str, Any]]],
                            facts_docs: List[Tuple[str, Dict[str, Any]]],
                            dry_run: bool = False) -> None:
    """
    Escribe en:
      brands/{brand}/models/{model}
      brands/{brand}/models/{model}/trims/{trim}
      brands/{brand}/models/{model}/facts/{fact}
    """
    base_ref = db.collection(ROOT).document(brand_id).collection("models").document(model_id)

    # Model doc
    if dry_run:
        print("[DRY] set model:", f"{ROOT}/{brand_id}/models/{model_id}")
    else:
        base_ref.set(model_doc, merge=True)

    # Trims: batch
    trims_ref = base_ref.collection("trims")
    for batch_items in chunked(trims_docs, 400):  # 500 límite, dejamos margen
        if dry_run:
            print(f"[DRY] upsert trims x{len(batch_items)}")
            continue
        batch = db.batch()
        for tid, tdoc in batch_items:
            batch.set(trims_ref.document(tid), tdoc, merge=True)
        batch.commit()

    # Facts: batch
    facts_ref = base_ref.collection("facts")
    for batch_items in chunked(facts_docs, 400):
        if dry_run:
            print(f"[DRY] upsert facts x{len(batch_items)}")
            continue
        batch = db.batch()
        for fid, fdoc in batch_items:
            batch.set(facts_ref.document(fid), fdoc, merge=True)
        batch.commit()


# =========================
# IO
# =========================
def load_inputs(input_path: str) -> List[Dict[str, Any]]:
    p = Path(input_path)
    if p.is_file():
        data = json.loads(p.read_text(encoding="utf-8"))
        # puede ser combined.json (lista) o single json (dict)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict) and "error" not in x]
        if isinstance(data, dict):
            return [data] if "error" not in data else []
        return []
    if p.is_dir():
        items = []
        for fp in sorted(p.glob("*.json")):
            try:
                d = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(d, dict) and "error" not in d:
                    items.append(d)
            except Exception:
                pass
        return items
    raise FileNotFoundError(f"Ruta no existe: {input_path}")


# =========================
# CLI
# =========================
def main(input_path: str, dry_run: bool = False):
    raws = load_inputs(input_path)
    if not raws:
        print("No se encontraron JSONs válidos en:", input_path)
        return

    #db = init_firestore()

    for raw in tqdm(raws, desc="Subiendo a Firestore"):
        normed = normalize_extracted_json(raw)
        #print(normed)
        brand_id, model_id, model_doc, trims_docs, facts_docs = build_firestore_docs(normed)
        print(brand_id)
        print("*" * 50)
        print(model_id)
        print("*" * 50)
        print(model_doc)
        print("*" * 50)
        print(trims_docs)
        print("*" * 50)
        print(facts_docs)
        exit(0)
        #write_model_to_firestore(db, brand_id, model_id, model_doc, trims_docs, facts_docs, dry_run=dry_run)

    print("OK ✅")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso:")
        print("  python3 push_to_firestore.py <out_specs_folder_or_combined.json> [--dry-run]")
        print("")
        print("Ejemplos:")
        print("  python3 push_to_firestore.py out_specs")
        print("  python3 push_to_firestore.py out_specs/combined.json")
        print("  python3 push_to_firestore.py out_specs --dry-run")
        raise SystemExit(1)

    input_path = sys.argv[1]
    dry = "--dry-run" in sys.argv[2:]
    main(input_path, dry_run=dry)
