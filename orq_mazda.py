# -*- coding: utf-8 -*-
"""
Playwright scraper/selector para PLP Mazda:
- Extrae modelos del filtro
- Selecciona uno a uno por checkbox
- Desmarca todo de forma global
- Extrae cards
- ✅ Evita mezcla: elige id_model objetivo desde las URLs (dominante o guess) y filtra
"""

import time
import json
import re
import urllib.parse
import csv
import os
import sys
import traceback
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs
from collections import Counter
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

from utils import saveCar, to_title_custom

# ===================== CONFIG =====================
URL = "https://www.mazda.cl/busqueda"
# Mantengo comportamiento del script original: visible por defecto
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 1000}

# ===================== SELECTORES =================
UL_ID = "plp_list__Modelo"
SEL_UL = f"ul#{UL_ID}"
SEL_LI = f"{SEL_UL} li.plp_checkbox__items.plp_items__Modelo"
SEL_LABEL = f"{SEL_LI} label.plp_label__checkbox"
SEL_CHECKBOX = f"{SEL_LI} input.plp_input__checkbox"
SEL_SCROLL_CONTAINER = ".plp_scroll__container"

SEL_ARTICLE = "article.plp_vehicles_grid__content"
SEL_CARD = f"{SEL_ARTICLE} .plp_vehicles_grid__content__card.plp_grid_card"

SEL_BRAND = ".plp_grid_card__content h5.plp_grid_card__content__h5"
SEL_MODEL = ".plp_grid_card__content h3.plp_grid_card__content__h3"
SEL_VERSION = ".plp_grid_card__content h5.plp_grid_card__content__h5__fit"
SEL_P_DESDE_VAL = ".plp_grid_card__content h2.plp_grid_card__content__h2"
SEL_PRECIO_LISTA_STRONG = ".plp_grid_card__content p.plp_grid_card__content__p strong.plp_grid_card__content__p__strong__price"
SEL_CTA = ".plp_grid_card__buttons_group a.plp_grid_card__buttons_group__primary"

CURRENCY_CLEAN_RE = re.compile(r"[^\d]")

# ===================== UTILIDADES =================
def abs_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href or "")

def clean_money(text: str) -> Optional[int]:
    if not text:
        return None
    nums = CURRENCY_CLEAN_RE.sub("", text)
    return int(nums) if nums.isdigit() else None

def get_id_model(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get("id_model", [None])[0]
        return v.strip() if v else None
    except Exception:
        return None

def guess_id_model_from_label(label: str) -> Optional[str]:
    """
    Heurística:
      "MAZDA CX-90" -> "CX-90"
      "MAZDA BT-50" -> "BT-50"
      "MAZDA MX-5"  -> "MX-5"
      "MAZDA 3 SPORT" -> "3SPORT"
      "MAZDA 3" -> "3" (pero en cards suele ser 3SEDAN; por eso hacemos fallback al dominante)
    """
    if not label:
        return None
    s = label.strip()
    if s.upper().startswith("MAZDA "):
        s = s[6:].strip()
    s = s.replace(" ", "")
    return s or None

def wait_grid_update(page, previous_hash: Optional[str] = None, timeout_ms: int = 15000) -> str:
    page.wait_for_selector(SEL_ARTICLE, state="visible", timeout=timeout_ms)
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        html = page.locator(SEL_ARTICLE).first.inner_html()
        h = str(hash(html))
        if previous_hash is None or h != previous_hash:
            return h
        time.sleep(0.15)
    return str(hash(page.locator(SEL_ARTICLE).first.inner_html()))


def close_overlays(page) -> bool:
    """
    Cierra u oculta overlays/modales que puedan interceptar clicks.
    En Mazda a veces el checkbox existe, pero el click no cambia el estado por capas encima
    o por el comportamiento custom del filtro.
    """
    try:
        # Botones típicos de consentimiento/cookies
        for sel in [
            "button:has-text('Aceptar')",
            "button:has-text('Aceptar todo')",
            "button:has-text('Accept')",
            "button:has-text('Accept All')",
            "button:has-text('Entendido')",
            "a:has-text('Aceptar')",
            "a:has-text('Entendido')",
        ]:
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible(timeout=500):
                    loc.click(force=True, timeout=1200)
                    time.sleep(0.2)
            except Exception:
                pass

        page.evaluate("""
            () => {
                const selectors = [
                    '#usercentrics-cmp-ui',
                    'aside#usercentrics-cmp-ui',
                    '[id*="usercentrics"]',
                    '[class*="usercentrics"]',
                    '.modal-backdrop',
                    '.cookies-overlay',
                    '.v-modal',
                    '.overlay',
                    '.swal2-container'
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
        return True
    except Exception:
        return False


def expand_modelos_if_needed(page):
    # === ESTO SE MANTIENE EXACTAMENTE COMO TU ORIGINAL ===
    page.wait_for_selector(SEL_UL, state="attached", timeout=15000)
    if page.locator(SEL_UL).first.is_visible():
        return

    toggles = [
        f'[aria-controls="{UL_ID}"]', f'[data-target="#{UL_ID}"]',
        f'[href="#{UL_ID}"]', f'button[aria-controls="{UL_ID}"]',
        "button:has-text('Modelo')","button:has-text('Modelos')",
        "summary:has-text('Modelo')","summary:has-text('Modelos')",
        "[role=button]:has-text('Modelo')","[role=button]:has-text('Modelos')",
        "a:has-text('Modelo')","a:has-text('Modelos')",
    ]
    for sel in toggles:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.scroll_into_view_if_needed()
                loc.first.click(timeout=1500)
                time.sleep(0.25)
                if page.locator(SEL_UL).first.is_visible():
                    return
            except Exception:
                pass

    cand = page.locator("button:has-text('Modelo')")
    if cand.count() == 0:
        cand = page.locator("summary:has-text('Modelo')")
    if cand.count() > 0:
        try:
            cand.first.press("Enter")
            time.sleep(0.25)
        except Exception:
            try:
                cand.first.press(" ")
                time.sleep(0.25)
            except Exception:
                pass

    page.locator(SEL_UL).first.wait_for(state="visible", timeout=8000)

def scroll_sweep_filter(page):
    target = page.locator(SEL_SCROLL_CONTAINER)
    if target.count() == 0 or not target.first.is_visible():
        target = page.locator(SEL_UL)
    try:
        target.first.evaluate("el => el.scrollTop = 0")
        time.sleep(0.1)
        last = -1
        for _ in range(60):
            target.first.evaluate("el => el.scrollTop = el.scrollHeight")
            time.sleep(0.08)
            new_val = target.first.evaluate("el => el.scrollTop")
            if new_val == last:
                break
            last = new_val
    except Exception:
        pass

def _visible_models_container(page):
    cand = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    if cand.count() > 0:
        return cand.first
    ul_vis = page.locator(f"{SEL_UL}:visible")
    if ul_vis.count() > 0:
        return ul_vis.first
    expand_modelos_if_needed(page)
    cand = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    return cand.first if cand.count() > 0 else page.locator(SEL_UL).first

def _scroll_to_li_by_value(page, container, value: str):
    js = """
    (wrap, ulSelector, value) => {
      const ul = document.querySelector(ulSelector);
      if (!ul) return false;
      const byVal = ul.querySelector(
        `li.plp_checkbox__items.plp_items__Modelo input.plp_input__checkbox[value="${value}"]`
      );
      const li = byVal ? byVal.closest('li') : null;
      if (!li) return false;
      const sc = wrap || ul;
      sc.scrollTop = Math.max(0, li.offsetTop - 60);
      return true;
    }
    """
    try:
        ok = container.evaluate(js, SEL_UL, value)
        time.sleep(0.12)
        return bool(ok)
    except Exception:
        return False

# ===================== MODELOS =================
def get_all_models(page) -> List[Dict]:
    expand_modelos_if_needed(page)
    scroll_sweep_filter(page)

    items = page.locator(SEL_LI)
    out: List[Dict] = []
    seen = set()

    for i in range(items.count()):
        li = items.nth(i)
        inp = li.locator("input.plp_input__checkbox").first
        lab = li.locator("label.plp_label__checkbox").first

        try:
            value = (inp.get_attribute("value") or "").strip()
            label_text = (lab.inner_text() or "").strip()

            key = value or label_text
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)

            out.append({"value": key, "label": label_text})
        except Exception:
            pass

    return out

def uncheck_all_models(page):
    expand_modelos_if_needed(page)
    checked = page.locator(f"{SEL_UL} input.plp_input__checkbox:checked")
    for i in range(checked.count()):
        inp = checked.nth(i)
        try:
            input_id = inp.get_attribute("id")
            if input_id:
                lab = page.locator(f"label[for='{input_id}']").first
                if lab.count():
                    lab.click(timeout=1500)
                else:
                    inp.uncheck(force=True, timeout=1500)
            else:
                inp.uncheck(force=True, timeout=1500)
        except Exception:
            try:
                inp.uncheck(force=True, timeout=1500)
            except Exception:
                pass
    time.sleep(0.2)

def _locate_by_value(page, modelo_value: str):
    inp = page.locator(f"{SEL_LI} input.plp_input__checkbox[value='{modelo_value}']").first
    if inp.count():
        input_id = inp.get_attribute("id")
        lab = page.locator(f"label[for='{input_id}']").first if input_id else None
        if lab and lab.count():
            return lab, inp
        li = inp.locator("xpath=ancestor::li[1]")
        lab2 = li.locator("label.plp_label__checkbox").first
        return (lab2 if lab2.count() else None), inp
    return None, None

def click_model_by_value(page, modelo_value: str, wait_grid: bool = True, prev_hash: Optional[str] = None) -> bool:
    """
    Marca el modelo en el filtro.
    Versión robusta:
    1) ubica el input por value,
    2) intenta click normal en label,
    3) intenta check(force=True),
    4) si el sitio no confirma checked, fuerza checked + dispatch de eventos por JS.
    """
    expand_modelos_if_needed(page)
    close_overlays(page)

    cont = _visible_models_container(page)
    _scroll_to_li_by_value(page, cont, modelo_value)

    label_loc, input_loc = _locate_by_value(page, modelo_value)
    if not label_loc and not input_loc:
        scroll_sweep_filter(page)
        _scroll_to_li_by_value(page, cont, modelo_value)
        label_loc, input_loc = _locate_by_value(page, modelo_value)

    if not input_loc or not input_loc.count():
        raise RuntimeError(f"No encontré el model item por value '{modelo_value}' en {SEL_UL}")

    try:
        input_loc.scroll_into_view_if_needed(timeout=1500)
    except Exception:
        pass

    def is_marked() -> bool:
        try:
            return bool(input_loc.is_checked())
        except Exception:
            return False

    if not is_marked():
        # 1) Click normal sobre label
        if label_loc and label_loc.count():
            try:
                label_loc.scroll_into_view_if_needed(timeout=1500)
                label_loc.click(timeout=2500)
            except Exception:
                close_overlays(page)
                try:
                    label_loc.click(force=True, timeout=2500)
                except Exception:
                    pass

        # 2) Check forzado sobre input
        if not is_marked():
            try:
                input_loc.check(force=True, timeout=2500)
            except Exception:
                pass

        # 3) JS directo: checked + eventos para Vue/React/JS del sitio
        if not is_marked():
            try:
                handle = input_loc.element_handle(timeout=2000)
                if handle:
                    page.evaluate(
                        """
                        (el) => {
                            el.scrollIntoView({block: 'center'});
                            el.checked = true;
                            el.setAttribute('checked', 'checked');
                            el.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
                            el.dispatchEvent(new Event('input', {bubbles: true}));
                            el.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                        """,
                        handle
                    )
            except Exception:
                pass

    # Confirmación con más margen. En algunos casos el DOM se re-renderiza y el handle anterior queda viejo.
    marked = False
    for _ in range(30):
        label_loc, input_loc = _locate_by_value(page, modelo_value)
        if input_loc and input_loc.count():
            try:
                if input_loc.is_checked():
                    marked = True
                    break
            except Exception:
                pass
        time.sleep(0.1)

    if not marked:
        raise RuntimeError(f"No pude marcar el modelo '{modelo_value}'")

    if wait_grid:
        try:
            wait_grid_update(page, previous_hash=prev_hash, timeout_ms=15000)
        except Exception:
            # Si el hash no cambia, igual puede haber tarjetas filtradas. No detenemos el proceso aquí.
            pass

    return True

# ===================== EXTRACCIÓN DE CARDS =================
def extract_cards(page, base_url: str) -> List[Dict]:
    cards = page.locator(SEL_CARD)
    out: List[Dict] = []

    for i in range(cards.count()):
        card = cards.nth(i)
        try:
            brand = card.locator(SEL_BRAND)
            model = card.locator(SEL_MODEL)
            version = card.locator(SEL_VERSION)

            brand_txt = brand.first.inner_text().strip() if brand.count() else None
            model_txt = model.first.inner_text().strip() if model.count() else None
            version_txt = version.first.inner_text().strip() if version.count() else None

            p_desde_val = card.locator(SEL_P_DESDE_VAL).first.inner_text().strip() if card.locator(SEL_P_DESDE_VAL).count() else None
            p_desde_int = clean_money(p_desde_val or "")

            strongs = card.locator(SEL_PRECIO_LISTA_STRONG)
            precio_lista = clean_money(strongs.nth(0).inner_text().strip()) if strongs.count() >= 1 else None
            bono_directo = clean_money(strongs.nth(1).inner_text().strip()) if strongs.count() >= 2 else None
            bono_financiamiento = clean_money(strongs.nth(2).inner_text().strip()) if strongs.count() >= 3 else None

            href_abs = None
            if card.locator(SEL_CTA).count():
                href_rel = card.locator(SEL_CTA).first.get_attribute("href") or ""
                href_abs = abs_url(base_url, href_rel)

            id_model = get_id_model(href_abs or "")

            out.append({
                "brand": brand_txt,
                "model": model_txt,
                "version": version_txt,
                "precio_desde_texto": p_desde_val,
                "precio_desde": p_desde_int,
                "precio_lista": precio_lista,
                "bono_directo": bono_directo,
                "bono_financiamiento": bono_financiamiento,
                "cotizar_url": href_abs,
                "id_model": id_model,
            })
        except Exception as e:
            out.append({"_error": str(e)})

    return out

def pick_target_id_model(modelo_label: str, cards: List[Dict]) -> Optional[str]:
    ids = [c.get("id_model") for c in cards if c.get("id_model")]
    if not ids:
        return None

    counts = Counter(ids)
    guess = guess_id_model_from_label(modelo_label)
    if guess and guess in counts:
        return guess

    return counts.most_common(1)[0][0]

# ===================== MAIN =====================
def main():
    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "rows_extracted": 0,
        "row_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    browser = None
    ctx = None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
            ctx = browser.new_context(viewport=VIEWPORT)
            page = ctx.new_page()
            page.goto(URL, wait_until="domcontentloaded")

            try:
                page.wait_for_load_state("networkidle", timeout=7000)
            except PWTimeoutError:
                pass

            close_overlays(page)

            modelos = get_all_models(page)
            stats["models_found"] = len(modelos)
            print(f"[INFO] Modelos detectados ({len(modelos)}): {[m['label'] or m['value'] for m in modelos]}")

            results: List[Dict] = []
            article_hash: Optional[str] = None

            for m in modelos:
                close_overlays(page)
                modelo_value = m["value"]
                modelo_label = m["label"] or modelo_value

                print(f"\n[RUN] Procesando modelo: {modelo_label} (value={modelo_value})")

                try:
                    uncheck_all_models(page)
                    ok = click_model_by_value(page, modelo_value, wait_grid=True, prev_hash=article_hash)
                    if not ok:
                        print(f"[WARN] No se pudo marcar '{modelo_value}'. Sigo…")
                        continue

                    article_hash = str(hash(page.locator(SEL_ARTICLE).first.inner_html()))

                    try:
                        page.wait_for_selector(SEL_CARD, state="visible", timeout=7000)
                    except PWTimeoutError:
                        print(f"[WARN] Sin tarjetas visibles para '{modelo_label}'.")
                        continue

                    cards = extract_cards(page, base_url=URL)
                    target_id = pick_target_id_model(modelo_label, cards)

                    if not target_id:
                        print(f"[WARN] No pude determinar id_model objetivo para '{modelo_label}'. (cards={len(cards)})")
                        continue

                    filtered = []
                    for c in cards:
                        c["modelo_filtro_label"] = modelo_label
                        c["modelo_filtro_value"] = modelo_value
                        c["target_id_model"] = target_id
                        if c.get("id_model") == target_id:
                            filtered.append(c)

                    stats["models_processed"] += 1
                    stats["rows_extracted"] += len(filtered)
                    stats["row_errors"] += len([x for x in filtered if x.get("_error")])

                    print(f"[OK] {len(filtered)}/{len(cards)} tarjetas válidas para '{modelo_label}' (target_id_model={target_id})")
                    results.extend(filtered)

                except Exception as e:
                    stats["model_errors"] += 1
                    print(f"[ERROR] Error procesando {modelo_label}: {e}")
                    traceback.print_exc()

            print("\n==== RESUMEN ====")
            print(f"Total tarjetas válidas: {len(results)}")

            with open("mazda_modelos.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            for r in results:
                try:
                    if r.get("_error"):
                        stats["save_errors"] += 1
                        continue

                    precio = [r.get('precio_desde'), r.get('precio_desde'), r.get('precio_lista'), r.get('precio_lista')]
                    tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
                    datos = {
                        'modelo': to_title_custom(r.get('model')),
                        'marca': to_title_custom(r.get('brand')),
                        'modelDetail': r.get('version'),
                        'tiposprecio': tiposprecio,
                        'precio': precio
                    }

                    print(datos)
                    saveCar('Mazda', datos, 'www.mazda.cl')
                    stats["saved_ok"] += 1

                except Exception as e:
                    stats["save_errors"] += 1
                    print(f"[ERROR] saveCar falló para fila {r}: {e}")
                    traceback.print_exc()

            if results:
                cols = [
                    "modelo_filtro_label", "modelo_filtro_value", "target_id_model",
                    "id_model", "brand", "model", "version",
                    "precio_desde_texto", "precio_desde", "precio_lista",
                    "bono_directo", "bono_financiamiento", "cotizar_url"
                ]
                with open("mazda_modelos.csv", "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(cols)
                    for r in results:
                        row = [str(r.get(k, "") or "").replace("\n", " ").strip() for k in cols]
                        writer.writerow(row)
                print("→ Guardado: mazda_modelos.csv")

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        summary = {
            "status": "error",
            "source": "www.mazda.cl",
            **stats
        }
        print(json.dumps(summary, ensure_ascii=False))
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
        "source": "www.mazda.cl",
        **stats
    }

    if stats["models_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    if stats["models_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    if stats["rows_extracted"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    if stats["saved_ok"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    if stats["models_found"] > 0:
        error_ratio = stats["model_errors"] / stats["models_found"]
        summary["error_ratio"] = round(error_ratio, 4)

        if error_ratio >= 0.5:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            sys.exit(1)

    print("→ Guardado: mazda_modelos.json")
    print("RUN_OK")
    print(json.dumps(summary, ensure_ascii=False))
    sys.exit(0)

if __name__ == "__main__":
    main()