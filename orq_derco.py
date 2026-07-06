# -*- coding: utf-8 -*-
# Scraper PLP genérico por marcas: guarda JSON/CSV separados por marca e incluye Precio Lista / Bono Marca / Bono Financiamiento

import time
import re
import csv
import json
import unicodedata
import urllib.parse
import os
import sys
import traceback
from typing import List, Dict, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from utils import saveCar

# =============== CONFIG ===============
URL = "https://www.dercocenter.cl/busqueda"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 950}

BRANDS = ["Suzuki", "GWM", "Renault", "Changan", "Deepal"]

# =============== SELECTORES ===============
SEL_COOKIES_MODAL = ".inner-body:has(.title-cookie)"
SEL_BTN_ENTENDIDO = ".inner-body .acepted a"

SEL_BRAND_LABELS = "label.custom-control-label"
SEL_BRAND_INPUTS = "input.custom-control-input[name='filter_brand_']"

SEL_GRID_CONTAINER = ".container-card"
SEL_CARD = ".card.card-search.show-desktop"
SEL_CARD_BODY = ".card-body"

SEL_DISCOUNT = ".container-head-card p.mb-0, .container-head-card p"
SEL_BRAND_TEXT = ".brand-text"
SEL_MODEL_TEXT = ".model-text"
SEL_VERSION_TEXT = ".version-text"
SEL_PRICE_MAIN = ".prices h3.price-red"
SEL_PRICES_BLOCK = ".prices-disclaimer"
SEL_URLS_IN_CARD = "a[href^='/auto/']"

SEL_SPECS_CONTAINER = "#list-card-spec"
SEL_SPEC_ROWS = f"{SEL_SPECS_CONTAINER} .d-flex"

MONEY_RE = re.compile(r"[^\d]")


# =============== UTILS ===============
def abs_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href or "")


def clean_money(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    nums = MONEY_RE.sub("", s)
    return int(nums) if nums.isdigit() else None


def strip_accents_lower(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()


def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip()


def parse_discount(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"(\d+[.,]?\d*)\s*%+", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return None


def wait_grid_ready(page, timeout_ms: int = 15000):
    page.wait_for_selector(SEL_GRID_CONTAINER, state="visible", timeout=timeout_ms)
    page.wait_for_selector(f"{SEL_CARD} {SEL_CARD_BODY}", state="visible", timeout=timeout_ms)


def wait_grid_refresh(page, prev_signature: str, timeout_ms: int = 15000):
    # Playwright Python recibe un solo arg. Usamos destructuring para evitar errores
    # tipo: Page.wait_for_function() takes 2 positional arguments...
    page.wait_for_function(
        """([sel, prev]) => {
            const c = document.querySelector(sel);
            if (!c) return false;
            const sig = (c.innerText || '').slice(0, 2000);
            return sig !== prev;
        }""",
        arg=[SEL_GRID_CONTAINER, prev_signature],
        timeout=timeout_ms
    )


def grid_signature(page) -> str:
    try:
        el = page.query_selector(SEL_GRID_CONTAINER)
        return (el.inner_text() if el else "")[:2000]
    except Exception:
        return ""


# =============== COOKIES ===============
def close_cookies_modal(page, timeout_ms: int = 6000) -> bool:
    """
    Cierra/oculta banners de cookies, incluyendo Usercentrics.
    El error actual viene de:
    <aside id="usercentrics-cmp-ui"> intercepts pointer events
    """

    # 1) Intentar aceptar/cerrar el CMP moderno de Usercentrics por botones visibles.
    possible_buttons = [
        "#accept",
        "button#accept",
        "button[data-testid*='accept']",
        "button:has-text('Aceptar')",
        "button:has-text('Aceptar todo')",
        "button:has-text('Acepto')",
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "button:has-text('Allow all')",
        "button:has-text('Permitir todo')",
        "button:has-text('Entendido')",
        "a:has-text('Entendido')",
        SEL_BTN_ENTENDIDO,
    ]

    for sel in possible_buttons:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible(timeout=800):
                loc.click(force=True, timeout=2000)
                time.sleep(0.5)
                return True
        except Exception:
            pass

    # 2) Intentar cerrar el modal antiguo si aparece.
    try:
        if page.locator(SEL_COOKIES_MODAL).first.is_visible(timeout=800):
            try:
                page.locator(SEL_BTN_ENTENDIDO).first.click(force=True, timeout=1500)
                time.sleep(0.3)
                return True
            except Exception:
                pass
    except Exception:
        pass

    # 3) Último recurso: remover overlays que bloquean los clicks.
    try:
        page.evaluate("""
            () => {
                const selectors = [
                    '#usercentrics-cmp-ui',
                    'aside#usercentrics-cmp-ui',
                    '[id*="usercentrics"]',
                    '[class*="usercentrics"]',
                    '[data-testid*="uc-"]',
                    '.uc-overlay',
                    '.modal-backdrop',
                    '.cookies-overlay',
                    '.v-modal',
                    '.overlay'
                ];

                for (const sel of selectors) {
                    document.querySelectorAll(sel).forEach(el => {
                        try { el.remove(); } catch(e) {}
                    });
                }

                document.body.style.overflow = 'auto';
                document.documentElement.style.overflow = 'auto';
            }
        """)
        time.sleep(0.3)
        return True
    except Exception:
        return False


def safe_click(page, locator, timeout_ms: int = 2500) -> bool:
    """
    Click con fallback para overlays de cookies.
    Primero intenta normal, luego cierra cookies, luego force=True.
    """
    try:
        locator.click(timeout=timeout_ms)
        return True
    except Exception:
        close_cookies_modal(page)
        try:
            locator.click(force=True, timeout=max(timeout_ms, 3000))
            return True
        except Exception:
            return False


# =============== MARCAS (GENÉRICO) ===============
def find_brand_block(page, brand_text: str) -> Dict[str, str]:
    label = page.locator(SEL_BRAND_LABELS, has_text=brand_text).first
    if not label.count():
        raise RuntimeError(f"No encontré la marca '{brand_text}' en el sidebar")

    label_for = label.get_attribute("for")
    if not label_for:
        raise RuntimeError(f"Label de '{brand_text}' sin atributo 'for'")

    checkbox_sel = f"input#{label_for}.custom-control-input[name='filter_brand_']"
    label_sel = f"label.custom-control-label[for='{label_for}']"

    brand_row = label.locator("xpath=ancestor::div[contains(@class,'d-flex') and contains(@class,'justify-content-between')]").first
    toggle = brand_row.locator("[aria-controls]").first
    aria_controls = toggle.get_attribute("aria-controls") if toggle.count() else None

    if not aria_controls:
        sec = label.locator("xpath=ancestor::section").first
        toggle = sec.locator("[aria-controls]").first
        aria_controls = toggle.get_attribute("aria-controls") if toggle.count() else None

    if not aria_controls:
        raise RuntimeError(f"No pude encontrar aria-controls (collapse-*) para '{brand_text}'")

    collapse_sel = f"#{aria_controls}"

    return {
        "checkbox_sel": checkbox_sel,
        "label_sel": label_sel,
        "collapse_sel": collapse_sel
    }


def ensure_brand_checked(page, brand_text: str, want_checked=True):
    close_cookies_modal(page)

    b = find_brand_block(page, brand_text)
    chk = page.locator(b["checkbox_sel"]).first
    page.wait_for_selector(b["checkbox_sel"], state="attached", timeout=10000)
    is_on = chk.is_checked()

    if want_checked and not is_on:
        ok = safe_click(page, page.locator(b["label_sel"]).first, timeout_ms=2500)
        if not ok:
            raise RuntimeError(f"No pude marcar la marca '{brand_text}'")
        time.sleep(0.3)

    elif not want_checked and is_on:
        ok = safe_click(page, page.locator(b["label_sel"]).first, timeout_ms=2500)
        if not ok:
            raise RuntimeError(f"No pude desmarcar la marca '{brand_text}'")
        time.sleep(0.3)


def expand_brand_models(page, brand_text: str):
    b = find_brand_block(page, brand_text)
    if page.locator(b["collapse_sel"]).first.is_visible():
        return b

    lab = page.locator(b["label_sel"]).first
    brand_row = lab.locator("xpath=ancestor::div[contains(@class,'d-flex') and contains(@class,'justify-content-between')]").first
    toggle = brand_row.locator("[aria-controls]").first
    if not safe_click(page, toggle, timeout_ms=2500):
        raise RuntimeError(f"No pude expandir modelos para '{brand_text}'")
    page.locator(b["collapse_sel"]).first.wait_for(state="visible", timeout=6000)
    return b


def uncheck_all_models_in_collapse(page, collapse_sel: str):
    checked = page.locator(f"{collapse_sel} input.custom-control-input[id^='model-']:checked")
    for i in range(checked.count()):
        inp = checked.nth(i)
        input_id = inp.get_attribute("id")
        if input_id:
            lab = page.locator(f'label.custom-control-label[for="{input_id}"]').first
            try:
                lab.click(timeout=1000)
            except Exception:
                try:
                    inp.uncheck(force=True)
                except Exception:
                    pass
        time.sleep(0.05)


def get_model_values_in_collapse(page, collapse_sel: str) -> List[str]:
    page.wait_for_selector(f"{collapse_sel} input.custom-control-input[id^='model-']", state="attached", timeout=8000)
    loc = page.locator(f"{collapse_sel} input.custom-control-input[id^='model-']")
    vals = []

    for i in range(loc.count()):
        v = (loc.nth(i).get_attribute("value") or "").strip()
        if v:
            vals.append(v)

    seen = set()
    return [x for x in vals if not (x in seen or seen.add(x))]


def click_model_in_collapse(page, collapse_sel: str, model_value: str) -> bool:
    inp = page.locator(f'{collapse_sel} input.custom-control-input[id^="model-"][value="{model_value}"]').first
    if not inp.count():
        return False

    input_id = inp.get_attribute("id")
    lab = page.locator(f'label.custom-control-label[for="{input_id}"]').first if input_id else None

    if inp.is_checked():
        return True

    try:
        if lab and lab.count():
            if not safe_click(page, lab, timeout_ms=2000):
                inp.check(force=True, timeout=2000)
        else:
            inp.check(force=True, timeout=2000)
    except Exception:
        close_cookies_modal(page)
        try:
            inp.check(force=True, timeout=2500)
        except Exception:
            return False

    for _ in range(12):
        if inp.is_checked():
            return True
        time.sleep(0.05)

    return False


def clear_all_brands(page):
    inputs = page.locator(SEL_BRAND_INPUTS)
    for i in range(inputs.count()):
        inp = inputs.nth(i)
        try:
            if inp.is_checked():
                input_id = inp.get_attribute("id")
                if input_id:
                    lab = page.locator(f'label.custom-control-label[for="{input_id}"]').first
                    try:
                        safe_click(page, lab, timeout_ms=1200)
                    except Exception:
                        try:
                            inp.uncheck(force=True, timeout=800)
                        except Exception:
                            pass
            time.sleep(0.05)
        except Exception:
            pass

    try:
        page.wait_for_load_state("networkidle", timeout=3000)
    except Exception:
        pass


def select_only_brand(page, brand_text: str):
    prev = grid_signature(page)
    clear_all_brands(page)
    ensure_brand_checked(page, brand_text, True)
    try:
        wait_grid_refresh(page, prev, timeout_ms=10000)
    except Exception:
        pass


# =============== EXTRACCIÓN DE TARJETAS ===============
def extract_money_from_h5(h5_locator) -> Optional[int]:
    try:
        if h5_locator.locator("span").count():
            txt = h5_locator.locator("span").first.inner_text().strip()
            val = clean_money(txt)
            if val is not None:
                return val
        txt_all = h5_locator.inner_text().strip()
        val = clean_money(txt_all)
        return val
    except Exception:
        return None


def extract_spec_value(card, label_prefixes: List[str]) -> Optional[str]:
    def norm(s: str) -> str:
        s = strip_accents_lower(norm_text(s)).replace(":", "")
        return s

    rows = card.locator(SEL_SPEC_ROWS)
    for i in range(rows.count()):
        row = rows.nth(i)
        txt = norm(row.inner_text())
        for p in label_prefixes:
            if norm(p) in txt:
                spans = row.locator("span")
                if spans.count() >= 2:
                    try:
                        return spans.nth(1).inner_text().strip()
                    except Exception:
                        pass
                after = txt.split(norm(p), 1)[-1].strip().lstrip(":").strip()
                if after:
                    return after
    return None


def extract_cards_from_grid(page, base_url: str, current_brand: str) -> List[Dict]:
    wait_grid_ready(page, 15000)
    cards = page.locator(SEL_CARD)
    data: List[Dict] = []

    for i in range(cards.count()):
        card = cards.nth(i)
        try:
            url_modelo = None
            links = card.locator(SEL_URLS_IN_CARD)
            if links.count():
                href_rel = links.first.get_attribute("href") or ""
                url_modelo = abs_url(base_url, href_rel)

            discount_txt = card.locator(SEL_DISCOUNT).first.inner_text().strip() if card.locator(SEL_DISCOUNT).count() else None
            discount_pct = parse_discount(discount_txt)

            brand = card.locator(SEL_BRAND_TEXT).first.inner_text().strip() if card.locator(SEL_BRAND_TEXT).count() else None
            model = card.locator(SEL_MODEL_TEXT).first.inner_text().strip() if card.locator(SEL_MODEL_TEXT).count() else None
            version = card.locator(SEL_VERSION_TEXT).first.inner_text().strip() if card.locator(SEL_VERSION_TEXT).count() else None

            if brand and current_brand and strip_accents_lower(brand) != strip_accents_lower(current_brand):
                continue

            price_main_text = card.locator(SEL_PRICE_MAIN).first.inner_text().strip() if card.locator(SEL_PRICE_MAIN).count() else None
            price_main = clean_money(price_main_text)

            price_lista = bono_marca = bono_fin = None
            if card.locator(SEL_PRICES_BLOCK).count():
                pb = card.locator(SEL_PRICES_BLOCK).first
                h5s = pb.locator("h5")
                for j in range(h5s.count()):
                    h5 = h5s.nth(j)
                    label_txt = strip_accents_lower(norm_text(h5.inner_text()))
                    if "precio lista" in label_txt or "precio de lista" in label_txt or "lista:" in label_txt:
                        price_lista = extract_money_from_h5(h5)
                    elif "bono marca" in label_txt:
                        bono_marca = extract_money_from_h5(h5)
                    elif "bono financiamiento" in label_txt or "bono financia" in label_txt:
                        bono_fin = extract_money_from_h5(h5)

                if price_lista is None and pb.locator("h5.list span").count():
                    price_lista = clean_money(pb.locator("h5.list span").first.inner_text().strip())

            consumo_urbano = extract_spec_value(card, ["Consumo urbano", "Consumo urbano (km/lts)"])
            traccion = extract_spec_value(card, ["Tracción"])
            pasajeros = extract_spec_value(card, ["Capacidad de pasajeros", "Pasajeros"])

            data.append({
                "brand": brand,
                "model": model,
                "version": version,
                "discount_pct": discount_pct,
                "price_main_text": price_main_text,
                "price_main": price_main,
                "price_lista": price_lista,
                "bono_marca": bono_marca,
                "bono_financiamiento": bono_fin,
                "consumo_urbano": consumo_urbano,
                "traccion": traccion,
                "pasajeros": pasajeros,
                "url_modelo": url_modelo,
            })
        except Exception as e:
            data.append({"_error": str(e)})

    seen = set()
    out = []
    for r in data:
        if r.get("_error"):
            out.append(r)
            continue

        if r.get("url_modelo"):
            key = ("u", r["url_modelo"], norm_text(r.get("version")))
        else:
            key = ("k", norm_text(r.get("brand")), norm_text(r.get("model")), norm_text(r.get("version")), r.get("price_main"))

        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


# =============== OUTPUT HELPERS ===============
def save_json(fname: str, rows: List[Dict]):
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def save_csv(fname: str, rows: List[Dict]):
    cols = [
        "marca_filtro", "modelo_filtro",
        "brand", "model", "version",
        "discount_pct",
        "price_main_text", "price_main",
        "price_lista", "bono_marca", "bono_financiamiento",
        "consumo_urbano", "traccion", "pasajeros",
        "url_modelo"
    ]
    with open(fname, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            row = [r.get(k, "") if r.get(k) is not None else "" for k in cols]
            row = [str(x).replace("\n", " ").strip() for x in row]
            w.writerow(row)


# =============== HELPERS DE GUARDADO ===============
def build_precio_array(row: Dict) -> List[Optional[int]]:
    price_main = row.get("price_main")
    price_lista = row.get("price_lista")
    bono_marca = row.get("bono_marca")

    if price_lista is None:
        return [price_main, price_main, price_main, price_main]

    precio_conv = price_lista - bono_marca if bono_marca is not None else price_lista
    return [price_main, precio_conv, price_lista, price_lista]


def build_savecar_payload(row: Dict) -> Optional[Dict]:
    if row.get("_error"):
        return None

    marca = row.get("brand")
    modelo = row.get("model")
    version = row.get("version")

    if not marca or not modelo:
        return None

    tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
    precio = build_precio_array(row)

    return {
        "modelo": modelo,
        "marca": marca,
        "modelDetail": version,
        "precio": precio,
        "tiposprecio": tiposprecio
    }


# =============== MAIN ===============
def main():
    os.makedirs("out", exist_ok=True)

    stats = {
        "brands_total": len(BRANDS),
        "brands_processed": 0,
        "brand_errors": 0,
        "models_found": 0,
        "models_processed": 0,
        "rows_extracted": 0,
        "row_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    all_data = {}
    all_rows: List[Dict] = []

    browser = None
    ctx = None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
            ctx = browser.new_context(viewport=VIEWPORT)
            page = ctx.new_page()

            page.add_init_script("""
              try {
                localStorage.setItem('cookies-accepted', 'true');
                localStorage.setItem('cookie-consent', 'accepted');
              } catch(e) {}
            """)

            page.goto(URL, wait_until="domcontentloaded", timeout=45000)
            try:
                page.wait_for_load_state("networkidle", timeout=6000)
            except PWTimeoutError:
                pass

            close_cookies_modal(page)

            for brand in BRANDS:
                close_cookies_modal(page)
                print(f"\n=== MARCA: {brand} ===")
                brand_rows: List[Dict] = []

                try:
                    select_only_brand(page, brand)
                    blk = expand_brand_models(page, brand)
                    uncheck_all_models_in_collapse(page, blk["collapse_sel"])

                    modelos = get_model_values_in_collapse(page, blk["collapse_sel"])
                    stats["brands_processed"] += 1
                    stats["models_found"] += len(modelos)

                    print(f"[INFO] Modelos {brand} ({len(modelos)}): {modelos}")

                except Exception as e:
                    stats["brand_errors"] += 1
                    print(f"[WARN] Error preparando marca {brand}: {e}")
                    traceback.print_exc()
                    all_data[brand] = []
                    continue

                for mv in modelos:
                    print(f"[RUN] {brand} -> modelo: {mv}")

                    try:
                        uncheck_all_models_in_collapse(page, blk["collapse_sel"])
                        prev = grid_signature(page)
                        ok = click_model_in_collapse(page, blk["collapse_sel"], mv)

                        if not ok:
                            print(f"[WARN] No pude marcar '{mv}'")
                            continue

                        try:
                            wait_grid_refresh(page, prev, timeout_ms=12000)
                        except Exception:
                            pass

                        wait_grid_ready(page, 15000)
                        cards = extract_cards_from_grid(page, base_url=URL, current_brand=brand)

                        for r in cards:
                            r["marca_filtro"] = brand
                            r["modelo_filtro"] = mv

                        brand_rows.extend(cards)
                        all_rows.extend(cards)
                        stats["models_processed"] += 1
                        stats["rows_extracted"] += len(cards)
                        stats["row_errors"] += sum(1 for r in cards if r.get("_error"))

                        print(f"[OK] Tarjetas extraídas (post filtro marca): {len(cards)}")

                    except PWTimeoutError:
                        print(f"[WARN] Sin tarjetas para '{brand}/{mv}'")
                    except Exception as e:
                        print(f"[WARN] Error extrayendo '{brand}/{mv}': {e}")
                        traceback.print_exc()

                all_data[brand] = brand_rows

                slug = brand.lower().replace(" ", "_")
                json_path = os.path.join("out", f"plp_{slug}.json")
                csv_path = os.path.join("out", f"plp_{slug}.csv")

                save_json(json_path, brand_rows)
                save_csv(csv_path, brand_rows)

                for row in brand_rows:
                    try:
                        payload = build_savecar_payload(row)
                        if not payload:
                            if row.get("_error"):
                                print(f"[WARN] fila con error omitida: {row.get('_error')}")
                            else:
                                print(f"[WARN] fila omitida por datos insuficientes: {row}")
                            stats["save_errors"] += 1
                            continue

                        print(payload)
                        saveCar(payload["marca"], payload, "www.dercocenter.cl")
                        stats["saved_ok"] += 1

                    except Exception as e:
                        stats["save_errors"] += 1
                        print(f"[ERROR] saveCar falló para fila {row}: {e}")
                        traceback.print_exc()

                print(f"→ Guardado {json_path} y {csv_path} ({len(brand_rows)} filas)")

            save_json(os.path.join("out", "plp_all.json"), all_rows)
            save_csv(os.path.join("out", "plp_all.csv"), all_rows)

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        if ctx:
            try:
                ctx.close()
            except Exception:
                pass
        if browser:
            try:
                browser.close()
            except Exception:
                pass

    summary = {
        "status": "success",
        "source": "www.dercocenter.cl",
        **stats
    }

    if stats["brands_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar ninguna marca")
        sys.exit(1)

    if stats["models_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron modelos")
        sys.exit(1)

    if stats["models_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar ningún modelo")
        sys.exit(1)

    if stats["rows_extracted"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se extrajeron filas")
        sys.exit(1)

    if stats["saved_ok"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se guardó ningún registro en Firebase")
        sys.exit(1)

    if stats["models_found"] > 0:
        error_ratio = (stats["models_found"] - stats["models_processed"]) / stats["models_found"]
        summary["error_ratio"] = round(error_ratio, 4)

        if error_ratio >= 0.5:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print(f"[ERROR] Demasiados errores de modelo: {stats['models_found'] - stats['models_processed']} de {stats['models_found']}")
            sys.exit(1)

    print(json.dumps(summary, ensure_ascii=False))
    print(f"\n✅ Total global: {len(all_rows)} (out/plp_all.json & out/plp_all.csv)")
    print("RUN_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()