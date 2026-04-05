import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import re
import json
import time
from pathlib import Path
from typing import List, Dict, Tuple, Any, Optional, Union

import numpy as np
import cv2
import fitz  # pymupdf
from tqdm import tqdm
import easyocr


# ============================
# OCR init
# ============================
print("Inicializando EasyOCR...")
_t0 = time.time()
READER = easyocr.Reader(["es"], gpu=False)
print(f"OCR listo en {time.time()-_t0:.1f}s")


# ============================
# Helpers
# ============================
def norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def is_empty(s: str) -> bool:
    s2 = norm(s).lower()
    return s2 in ("", "n/a", "na", "no aplica")

def has_ink(cell_bgr: np.ndarray) -> bool:
    gray = cv2.cvtColor(cell_bgr, cv2.COLOR_BGR2GRAY)
    return (gray < 240).mean() > 0.004

def looks_like_version(s: str) -> bool:
    s2 = norm(s)
    if not s2 or len(s2) > 70:
        return False
    u = s2.upper()
    return any(k in u for k in ["SENSE", "ADVANCE", "EXCLUSIVE", "PLATINUM", "SR", "CVT", "MT", "AT", "E-POWER", "EPOWER"])

def normalize_yesno_symbol(raw: str) -> str:
    """
    Devuelve:
      "●" o "-" o texto normalizado (Sí/No/otro)
    """
    s = norm(raw)
    if s == "●":
        return "●"
    if s == "-":
        return "-"
    if s.lower() in {"si", "sí"}:
        return "●"
    if s.lower() == "no":
        return "-"
    return s

def to_bool_or_keep(raw: str) -> Union[bool, str, None]:
    """
    Convierte a bool cuando corresponde:
      ● -> True
      - -> False
    Si está vacío -> None
    Si es otra cosa (ej "Airbags: 6") -> str
    """
    s = norm(raw)
    if is_empty(s):
        return None
    s2 = normalize_yesno_symbol(s)
    if s2 == "●":
        return True
    if s2 == "-":
        return False
    return s  # string raro -> mantener


# ============================
# Symbol detector (● / -) anti falsos positivos
# ============================
def detect_symbol_dot_or_dash(cell_bgr: np.ndarray) -> str:
    cell = cv2.copyMakeBorder(cell_bgr, 6, 6, 6, 6, cv2.BORDER_REPLICATE)
    gray = cv2.cvtColor(cell, cv2.COLOR_BGR2GRAY)

    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    thr = cv2.medianBlur(thr, 3)

    h, w = thr.shape[:2]
    ink_ratio = (thr > 0).mean()
    if ink_ratio < 0.0015:
        return ""

    cnts, _ = cv2.findContours(thr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # muchos contornos -> texto/número -> no símbolo
    if len(cnts) >= 10:
        return ""

    # guión
    row_sum = (thr > 0).sum(axis=1)
    max_row = row_sum.max()
    if max_row >= int(0.35 * w):
        thick_rows = (row_sum >= int(0.20 * w)).sum()
        if thick_rows <= max(3, int(0.08 * h)):
            return "-"

    # ●
    cell_area = float(h * w)
    for c in sorted(cnts, key=cv2.contourArea, reverse=True)[:6]:
        area = float(cv2.contourArea(c))
        if area < 6:
            continue
        if area > 0.12 * cell_area:
            continue

        x, y, ww, hh = cv2.boundingRect(c)
        if ww < 3 or hh < 3:
            continue

        ar = ww / float(hh)
        if not (0.55 <= ar <= 1.8):
            continue

        box_area = float(ww * hh)
        fill_ratio = area / (box_area + 1e-6)

        if fill_ratio >= 0.22 and area >= 0.0015 * cell_area:
            return "●"

    return ""


# ============================
# OCR
# ============================
def ocr_text_only(cell_bgr: np.ndarray) -> str:
    if not has_ink(cell_bgr):
        return ""
    gray = cv2.cvtColor(cell_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

    inv = cv2.bitwise_not(thr)
    inv = cv2.dilate(inv, np.ones((2, 2), np.uint8), iterations=1)
    thr2 = cv2.bitwise_not(inv)

    rgb = cv2.cvtColor(thr2, cv2.COLOR_GRAY2RGB)
    lines = READER.readtext(rgb, detail=0, paragraph=True)
    return norm(" ".join(lines))

def ocr_value_cell_bool(cell_bgr: np.ndarray) -> str:
    if not has_ink(cell_bgr):
        return ""
    sym = detect_symbol_dot_or_dash(cell_bgr)
    if sym:
        return sym
    return ocr_text_only(cell_bgr)

def ocr_value_cell_text(cell_bgr: np.ndarray) -> str:
    return ocr_text_only(cell_bgr)


# ============================
# Render PDF (page 0)
# ============================
def render_page(pdf_path: str, dpi: int = 220) -> np.ndarray:
    doc = fitz.open(pdf_path)
    page = doc.load_page(0)
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3)
    return img


# ============================
# Find tables
# ============================
def find_table_boxes(img_bgr: np.ndarray, min_area: int = 180_000, line_scale: int = 70) -> List[Tuple[int,int,int,int]]:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 25, 15)

    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (line_scale, 1))
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, line_scale))
    h = cv2.morphologyEx(thr, cv2.MORPH_OPEN, h_kernel, iterations=2)
    v = cv2.morphologyEx(thr, cv2.MORPH_OPEN, v_kernel, iterations=2)

    grid = cv2.add(h, v)
    grid = cv2.dilate(grid, np.ones((3, 3), np.uint8), iterations=2)

    contours, _ = cv2.findContours(grid, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    boxes = []
    for c in contours:
        x, y, w, h2 = cv2.boundingRect(c)
        if w * h2 >= min_area and w > 650 and h2 > 250:
            boxes.append((x, y, w, h2))

    boxes.sort(key=lambda b: (b[1], b[0]))
    return boxes


# ============================
# Grid extraction
# ============================
def cluster(vals: List[int], tol: int = 14) -> List[int]:
    vals = sorted(vals)
    if not vals:
        return []
    out = []
    cur = [vals[0]]
    for v in vals[1:]:
        if abs(v - cur[-1]) <= tol:
            cur.append(v)
        else:
            out.append(int(np.mean(cur)))
            cur = [v]
    out.append(int(np.mean(cur)))
    return out

def grid_lines(region_bgr: np.ndarray) -> Tuple[List[int], List[int]]:
    gray = cv2.cvtColor(region_bgr, cv2.COLOR_BGR2GRAY)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                                cv2.THRESH_BINARY_INV, 25, 15)

    hk = max(35, region_bgr.shape[1] // 70)
    vk = max(35, region_bgr.shape[0] // 70)
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (hk, 1))
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vk))

    h = cv2.morphologyEx(thr, cv2.MORPH_OPEN, h_kernel, iterations=2)
    v = cv2.morphologyEx(thr, cv2.MORPH_OPEN, v_kernel, iterations=2)

    h = cv2.dilate(h, np.ones((1, 3), np.uint8), iterations=1)
    v = cv2.dilate(v, np.ones((3, 1), np.uint8), iterations=1)

    inter = cv2.bitwise_and(h, v)
    ys, xs = np.where(inter > 0)
    if len(xs) < 30:
        return [], []

    x_lines = cluster(xs.tolist(), tol=14)
    y_lines = cluster(ys.tolist(), tol=14)
    return x_lines, y_lines

def grid_cells(region_bgr: np.ndarray) -> List[List[Tuple[int,int,int,int]]]:
    x_lines, y_lines = grid_lines(region_bgr)
    if len(x_lines) < 3 or len(y_lines) < 3:
        return []

    cells: List[List[Tuple[int,int,int,int]]] = []
    for r in range(len(y_lines) - 1):
        row = []
        y1, y2 = y_lines[r], y_lines[r+1]
        for c in range(len(x_lines) - 1):
            x1, x2 = x_lines[c], x_lines[c+1]
            x = min(x1, x2); y = min(y1, y2)
            w = abs(x2 - x1); h = abs(y2 - y1)
            if w > 10 and h > 10:
                row.append((x, y, w, h))
        if row:
            cells.append(row)
    return cells

def ocr_matrix(region_bgr: np.ndarray, cell_pad: int = 3, col_mode: str = "mixed") -> List[List[str]]:
    """
    col_mode:
      - "text": OCR texto en todas (tabla superior)
      - "mixed": col0 texto; resto bool-detector+OCR (tabla comparativa)
    """
    cells = grid_cells(region_bgr)
    if not cells:
        return []

    max_cols = max(len(r) for r in cells)
    if max_cols < 2:
        return []

    matrix: List[List[str]] = []
    for row in cells:
        texts = []
        for j, (x, y, w, h) in enumerate(row):
            x1 = max(x + cell_pad, 0)
            y1 = max(y + cell_pad, 0)
            x2 = min(x + w - cell_pad, region_bgr.shape[1])
            y2 = min(y + h - cell_pad, region_bgr.shape[0])
            crop = region_bgr[y1:y2, x1:x2]

            if j == 0:
                texts.append(ocr_text_only(crop))
            else:
                texts.append(ocr_value_cell_text(crop) if col_mode == "text" else ocr_value_cell_bool(crop))

        if any(not is_empty(t) for t in texts):
            texts.extend([""] * (max_cols - len(texts)))
            matrix.append(texts)

    if not matrix:
        return []

    cols = len(matrix[0])
    keep = [0]
    for c in range(1, cols):
        if any(not is_empty(r[c]) for r in matrix):
            keep.append(c)
    matrix = [[r[c] for c in keep] for r in matrix]
    return matrix


# ============================
# Parse tables
# ============================
NUMERIC_SECTIONS = {"RUEDAS", "CAPACIDADES", "PESO Y DIMENSIONES", "TRANSMISIÓN", "RENDIMIENTO"}
BOOL_SECTIONS = {"INTERIOR", "EXTERIOR", "CONFORT Y TECNOLOGÍA", "SEGURIDAD", "NISSAN INTELLIGENT MOBILITY", "NISSAN INTELLIGENT"}

def detect_header_row(mat: List[List[str]]) -> Optional[int]:
    best_i = None
    best_score = 0
    for i, row in enumerate(mat):
        score = sum(1 for c in row[1:] if looks_like_version(c))
        if score >= 2 and score > best_score:
            best_score = score
            best_i = i
    return best_i

def guess_section_from_key(key: str) -> Optional[str]:
    k = norm(key).upper()
    first = k.split(" ")[0] if k else ""
    known = {"RUEDAS","CAPACIDADES","PESO","DIMENSIONES","TRANSMISIÓN","TRANSMISION","RENDIMIENTO",
             "INTERIOR","EXTERIOR","CONFORT","SEGURIDAD","NISSAN"}
    if first in known:
        if first == "TRANSMISION":
            return "TRANSMISIÓN"
        if first in {"PESO","DIMENSIONES"}:
            return "PESO Y DIMENSIONES"
        if first == "CONFORT":
            return "CONFORT Y TECNOLOGÍA"
        if first == "NISSAN":
            return "NISSAN INTELLIGENT MOBILITY"
        return first
    return None

def strip_section_prefix(key: str, section: str) -> str:
    k = norm(key)
    if not section:
        return k
    u = k.upper()
    s_u = section.upper()
    if u.startswith(s_u + " "):
        return norm(k[len(section):])
    return k

def parse_key_value_table(mat: List[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in mat:
        if not row:
            continue
        key = norm(row[0])
        if is_empty(key):
            continue
        val = norm(" ".join([c for c in row[1:] if not is_empty(c)]))
        if is_empty(val):
            continue
        out[key] = val
    return out

def parse_comparative_table(mat: List[List[str]]) -> Tuple[List[str], Dict[str, Dict[str, Dict[str, str]]], Dict[str, Dict[str, Dict[str, Union[bool,str]]]]]:
    """
    specs_by_version: texto
    equipment_by_version: boolean (True/False) cuando detecta ●/-
    """
    header_i = detect_header_row(mat)
    if header_i is None:
        return [], {}, {}

    header = mat[header_i]
    versions = [norm(c) for c in header[1:] if not is_empty(c)]
    if len(versions) < 2:
        return [], {}, {}

    specs_by_version: Dict[str, Dict[str, Dict[str, str]]] = {v: {} for v in versions}
    equipment_by_version: Dict[str, Dict[str, Dict[str, Union[bool,str]]]] = {v: {} for v in versions}

    current_section: Optional[str] = None
    rows = mat[:header_i] + mat[header_i+1:]

    for row in rows:
        if not row:
            continue
        key_raw = norm(row[0])
        if is_empty(key_raw):
            continue

        sec_guess = guess_section_from_key(key_raw)
        if sec_guess:
            current_section = sec_guess
        if current_section is None:
            current_section = "OTROS"

        feature = strip_section_prefix(key_raw, current_section).strip(":-–— ")

        vals = row[1:1+len(versions)]

        if current_section in BOOL_SECTIONS:
            for ver, raw in zip(versions, vals):
                value = to_bool_or_keep(raw)  # True/False/str/None
                if value is None:
                    continue
                equipment_by_version[ver].setdefault(current_section, {})
                equipment_by_version[ver][current_section][feature] = value
        else:
            for ver, raw in zip(versions, vals):
                v = norm(raw)
                if is_empty(v):
                    continue
                specs_by_version[ver].setdefault(current_section, {})
                specs_by_version[ver][current_section][feature] = v

    return versions, specs_by_version, equipment_by_version


# ============================
# Extract PDF
# ============================
def crop_box(img: np.ndarray, box: Tuple[int,int,int,int], pad: int = 6) -> np.ndarray:
    x, y, w, h = box
    H, W = img.shape[:2]
    x1 = max(0, x - pad)
    y1 = max(0, y - pad)
    x2 = min(W, x + w + pad)
    y2 = min(H, y + h + pad)
    return img[y1:y2, x1:x2].copy()

def extract_pdf(pdf_path: str) -> Dict[str, Any]:
    page = render_page(pdf_path, dpi=220)
    boxes = find_table_boxes(page, min_area=180_000, line_scale=70)

    candidates = []
    for b in boxes:
        region = crop_box(page, b, pad=8)
        mat_text = ocr_matrix(region, cell_pad=3, col_mode="text")
        mat_mixed = ocr_matrix(region, cell_pad=3, col_mode="mixed")
        header_i = detect_header_row(mat_mixed) if mat_mixed else None
        candidates.append({
            "box": b,
            "mat_text": mat_text,
            "mat_mixed": mat_mixed,
            "has_versions_header": header_i is not None,
            "header_i": header_i,
            "rows": len(mat_mixed) if mat_mixed else 0,
            "cols": len(mat_mixed[0]) if mat_mixed else 0,
        })

    comp = None
    for c in sorted(candidates, key=lambda x: (x["has_versions_header"], x["rows"] * x["cols"]), reverse=True):
        if c["has_versions_header"] and c["rows"] >= 8 and c["cols"] >= 4:
            comp = c
            break

    kv = None
    for c in sorted(candidates, key=lambda x: x["box"][1]):
        mt = c["mat_text"]
        if not mt:
            continue
        cols = len(mt[0])
        if 2 <= cols <= 5 and len(mt) >= 8 and not c["has_versions_header"]:
            kv = c
            break

    versions: List[str] = []
    specs_by_version: Dict[str, Dict[str, Dict[str, str]]] = {}
    equipment_by_version: Dict[str, Dict[str, Dict[str, Union[bool,str]]]] = {}
    base_specs: Dict[str, str] = {}

    if comp and comp["mat_mixed"]:
        versions, specs_by_version, equipment_by_version = parse_comparative_table(comp["mat_mixed"])

    if kv and kv["mat_text"]:
        base_specs = parse_key_value_table(kv["mat_text"])

    return {
        "pdf": pdf_path,
        "versions": versions,
        "base_specs_or_equipment": base_specs,
        "specs_by_version": specs_by_version,
        "equipment_by_version": equipment_by_version,
        "debug": {
            "boxes_found": boxes,
            "picked_kv_box": kv["box"] if kv else None,
            "picked_comp_box": comp["box"] if comp else None,
            "comp_matrix_preview": (comp["mat_mixed"][:60] if comp and comp["mat_mixed"] else []),
            "kv_matrix_preview": (kv["mat_text"][:40] if kv and kv["mat_text"] else []),
        }
    }


# ============================
# Batch (folder) support
# ============================
def list_pdfs(path: Union[str, Path]) -> List[Path]:
    p = Path(path)
    if p.is_file() and p.suffix.lower() == ".pdf":
        return [p]
    if p.is_dir():
        return sorted([x for x in p.glob("*.pdf") if x.is_file()])
    raise FileNotFoundError(f"Ruta inválida: {path}")

def safe_stem(pdf_path: Path) -> str:
    s = pdf_path.stem
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
    return s[:180]

def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main(input_path: str, out_path: str = "out_specs"):
    pdfs = list_pdfs(input_path)
    outp = Path(out_path)

    if len(pdfs) == 1 and Path(input_path).is_file() and outp.suffix.lower() == ".json":
        data = extract_pdf(str(pdfs[0]))
        write_json(outp, data)
        print("OK ->", outp.as_posix())
        return

    outp.mkdir(parents=True, exist_ok=True)
    combined = []

    for pdf in tqdm(pdfs, desc="Procesando PDFs"):
        try:
            data = extract_pdf(str(pdf))
            out_file = outp / f"{safe_stem(pdf)}.json"
            write_json(out_file, data)
            combined.append(data)
        except Exception as e:
            err = {"pdf": str(pdf), "error": repr(e)}
            out_file = outp / f"{safe_stem(pdf)}.error.json"
            write_json(out_file, err)
            combined.append(err)

    write_json(outp / "combined.json", combined)
    print("OK ->", (outp / "combined.json").as_posix())


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python3 nissan.py <pdf_o_carpeta_pdfs> [out_json_or_folder]")
        print("")
        print("Ejemplos:")
        print("  python3 nissan.py out_nissan_brand/pdfs out_specs")
        print("  python3 nissan.py out_nissan_brand/pdfs/archivo.pdf versa.json")
        raise SystemExit(1)

    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "out_specs")
