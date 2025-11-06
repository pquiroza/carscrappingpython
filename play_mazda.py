# -*- coding: utf-8 -*-
# Playwright scraper/selector para PLP Mazda:
# - Extrae modelos del filtro
# - Selecciona uno a uno (por input[value=...] o li#ID)
# - Espera re-render de la grilla central
# - Extrae tarjetas (marca, modelo, versión, precios, bonos, cotizar)
#
# Requisitos:
#   pip install playwright
#   playwright install

import time, json, re, urllib.parse, csv
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, expect
from utils import saveCar
# ===================== CONFIG =====================
URL = "https://www.mazda.cl/busqueda"   # <-- AJUSTA a la página real del PLP
HEADLESS = False
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 1000}

# ===================== SELECTORES =================
UL_ID = "plp_list__Modelo"  # id del UL de modelos
SEL_UL = f"ul#{UL_ID}"
SEL_LI = f"{SEL_UL} li.plp_checkbox__items.plp_items__Modelo"
SEL_LABEL = f"{SEL_LI} label.plp_label__checkbox"
SEL_CHECKBOX = f"{SEL_LI} input.plp_input__checkbox"
SEL_SCROLL_CONTAINER = ".plp_scroll__container"  # contenedor con scroll

# Grilla central
SEL_ARTICLE = "article.plp_vehicles_grid__content"
SEL_CARD = f"{SEL_ARTICLE} .plp_vehicles_grid__content__card.plp_grid_card"

# Dentro de la card
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

def wait_grid_update(page, previous_hash: Optional[str] = None, timeout_ms: int = 12000) -> str:
    page.wait_for_selector(SEL_ARTICLE, state="visible", timeout=timeout_ms)
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        html = page.locator(SEL_ARTICLE).first.inner_html()
        h = str(hash(html))
        if previous_hash is None or h != previous_hash:
            return h
        time.sleep(0.15)
    return str(hash(page.locator(SEL_ARTICLE).first.inner_html()))

def expand_modelos_if_needed(page):
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
        "div:has-text('Modelo')","div:has-text('Modelos')",
    ]
    for sel in toggles:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.scroll_into_view_if_needed()
                loc.first.click(timeout=1500)
                time.sleep(0.2)
                if page.locator(SEL_UL).first.is_visible(): return
            except Exception:
                pass
    cand = page.locator("button:has-text('Modelo')")
    if cand.count() == 0:
        cand = page.locator("summary:has-text('Modelo')")
    if cand.count() > 0:
        try:
            cand.first.press("Enter"); time.sleep(0.2)
        except Exception:
            try:
                cand.first.press(" "); time.sleep(0.2)
            except Exception:
                pass
    page.locator(SEL_UL).first.wait_for(state="visible", timeout=8000)

def scroll_sweep_filter(page):
    target = page.locator(SEL_SCROLL_CONTAINER)
    if target.count() == 0 or not target.first.is_visible():
        target = page.locator(SEL_UL)
    try:
        target.first.evaluate("el => el.scrollTop = 0"); time.sleep(0.1)
        last = -1
        for _ in range(40):
            target.first.evaluate("el => el.scrollTop = el.scrollHeight")
            time.sleep(0.08)
            new_val = target.first.evaluate("el => el.scrollTop")
            if new_val == last: break
            last = new_val
    except Exception:
        pass

# ========= helpers contenedor visible / scroll preciso =========
def _visible_models_container(page):
    cand = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    if cand.count() > 0: return cand.first
    ul_vis = page.locator(f"{SEL_UL}:visible")
    if ul_vis.count() > 0: return ul_vis.first
    expand_modelos_if_needed(page)
    cand = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    return cand.first if cand.count() > 0 else page.locator(SEL_UL).first

def _scroll_to_li_by_offset(page, container, modelo_texto: str):
    """Intenta posicionar el scroll en el LI cuyo input tenga value=modelo_texto o cuyo id sea el texto."""
    js = """
    (wrap, ulSelector, value) => {
      const ul = document.querySelector(ulSelector);
      if (!ul) return false;
      // 1) por input[value=...]
      const byVal = ul.querySelector(`li.plp_checkbox__items.plp_items__Modelo input.plp_input__checkbox[value="${value}"]`);
      let li = byVal ? byVal.closest('li') : null;
      // 2) por li#ID
      if (!li) {
        li = ul.querySelector(`li#${CSS.escape(value)}`);
      }
      if (!li) return false;
      const sc = wrap || ul;
      sc.scrollTop = Math.max(0, li.offsetTop - 60);
      return true;
    }
    """
    try:
        ok = container.evaluate(js, SEL_UL, modelo_texto)
        time.sleep(0.12)
        return bool(ok)
    except Exception:
        return False

# ===================== CHECKBOX (MODELOS) =================
def get_all_model_names(page) -> List[str]:
    expand_modelos_if_needed(page)
    scroll_sweep_filter(page)
    # Idealmente ya los tienes; si no, obtén desde labels:
    labels = page.locator(SEL_LABEL)
    modelos = []
    for i in range(labels.count()):
        try:
            txt = labels.nth(i).inner_text().strip()
            if txt: modelos.append(txt)
        except Exception:
            pass
    # Si la UI suministra nombres vía input[value], consolidar también:
    inputs = page.locator(f"{SEL_LI} input.plp_input__checkbox")
    for i in range(inputs.count()):
        try:
            val = (inputs.nth(i).get_attribute("value") or "").strip()
            if val: modelos.append(val)
        except Exception:
            pass
    # dedup preservando orden
    seen=set(); return [m for m in modelos if not (m in seen or seen.add(m))]

def uncheck_all_models(page):
    expand_modelos_if_needed(page)
    cont = _visible_models_container(page)
    items = page.locator(f"{SEL_LI}:has({SEL_CHECKBOX}:checked)")
    for i in range(items.count()):
        li = items.nth(i)
        try:
            lbl = li.locator("label.plp_label__checkbox").first
            if not lbl.count(): continue
            if not lbl.is_visible():
                _scroll_to_li_by_offset(page, cont, lbl.inner_text().strip())
            try:
                lbl.click(timeout=1500)
            except Exception:
                page.evaluate("el => el.click()", lbl)
        except Exception:
            pass
    time.sleep(0.15)

def _locate_by_value_or_id(page, modelo_texto: str) -> Tuple[Optional[object], Optional[object]]:
    """
    Localiza (label, input) por:
      1) input[value='modelo_texto']
      2) li#'modelo_texto' -> su label y input
    """
    # 1) por input[value=...]
    inp = page.locator(f"{SEL_LI} input.plp_input__checkbox[value='{modelo_texto}']").first
    if inp.count():
        # buscar label por for
        input_id = inp.get_attribute("id")
        lab = page.locator(f"label[for='{input_id}']").first if input_id else None
        if lab and lab.count(): return lab, inp

    # 2) por li#ID
    li = page.locator(f"{SEL_LI}#{modelo_texto}").first
    if li.count():
        lab = li.locator("label.plp_label__checkbox").first
        inp = li.locator("input.plp_input__checkbox").first
        return (lab if lab.count() else None, inp if inp.count() else None)

    return None, None

def click_model_by_text(page, modelo_texto: str, wait_grid: bool = True, prev_hash: Optional[str] = None) -> bool:
    """
    Marca el modelo por su 'value' o 'li#ID' (no por texto visible del label).
    - Scrollea el contenedor hasta posicionar el LI.
    - Si el input es interactuable -> check(); si no, click en label.
    - Verifica estado y espera re-render de la grilla.
    """
    expand_modelos_if_needed(page)
    cont = _visible_models_container(page)

    # Posicionar scroll aproximadamente al LI objetivo
    _scroll_to_li_by_offset(page, cont, modelo_texto)

    # Localizar nodos robustos
    label_loc, input_loc = _locate_by_value_or_id(page, modelo_texto)
    if not label_loc and not input_loc:
        # reintento tras scroll adicional
        _scroll_to_li_by_offset(page, cont, modelo_texto)
        label_loc, input_loc = _locate_by_value_or_id(page, modelo_texto)

    if not label_loc and not input_loc:
        raise RuntimeError(f"No encontré el model item por value/ID '{modelo_texto}' en {SEL_UL}")

    # Ejecutar toggle
    marked = False
    try:
        if input_loc and input_loc.count():
            if not input_loc.is_visible():
                # si input está oculto, click en label
                if label_loc and label_loc.count():
                    try:
                        label_loc.click(timeout=1500)
                    except Exception:
                        page.evaluate("el => el.click()", label_loc)
                    # verificar estado si es posible
                    try:
                        for _ in range(10):
                            if input_loc.is_checked():
                                marked = True; break
                            time.sleep(0.05)
                    except Exception:
                        marked = True
                else:
                    # último recurso: check forzado (puede fallar si display:none)
                    try:
                        input_loc.check(force=True, timeout=1500); marked = True
                    except Exception:
                        marked = False
            else:
                # input visible -> check normal
                if not input_loc.is_checked():
                    try:
                        input_loc.check(timeout=1500)
                    except Exception:
                        input_loc.check(force=True)
                marked = input_loc.is_checked()
        else:
            # sin input localizado, usar label
            if label_loc and label_loc.count():
                try:
                    label_loc.click(timeout=1500); marked = True
                except Exception:
                    page.evaluate("el => el.click()", label_loc); marked = True
    except Exception:
        # intento final: click forzado en label si existe
        if label_loc and label_loc.count():
            try:
                page.evaluate("el => el.click()", label_loc); marked = True
            except Exception:
                marked = False

    if not marked:
        raise RuntimeError(f"No pude marcar el modelo '{modelo_texto}' (value/ID localizado pero no togglea)")

    if wait_grid:
        wait_grid_update(page, previous_hash=prev_hash, timeout_ms=15000)

    return True

# ===================== EXTRACCIÓN DE CARDS =================
def extract_cards(page, base_url: str) -> List[Dict]:
    cards = page.locator(SEL_CARD)
    out: List[Dict] = []
    for i in range(cards.count()):
        card = cards.nth(i)
        try:
            brand = card.locator(SEL_BRAND); model = card.locator(SEL_MODEL); version = card.locator(SEL_VERSION)
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
            })
        except Exception as e:
            out.append({"_error": str(e)})
    return out

# ===================== MAIN =====================
def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        try:
            page.wait_for_load_state("networkidle", timeout=7000)
        except PWTimeoutError:
            pass

        try:
            # 1) Obtener lista de modelos (viene de labels + inputs[value])
            modelos = get_all_model_names(page)
            print(f"[INFO] Modelos detectados ({len(modelos)}): {modelos}")

            results: List[Dict] = []
            article_hash: Optional[str] = None

            # 2) Iterar modelos: limpiar -> marcar -> esperar -> extraer
            for modelo in modelos:
                print(f"\n[RUN] Procesando modelo: {modelo}")
                uncheck_all_models(page)

                ok = click_model_by_text(page, modelo, wait_grid=True, prev_hash=article_hash)
                if not ok:
                    print(f"[WARN] No se pudo marcar '{modelo}'. Sigo con el siguiente…")
                    continue

                # Actualizar hash post-render
                article_hash = str(hash(page.locator(SEL_ARTICLE).first.inner_html()))

                # Asegurar tarjetas
                try:
                    page.wait_for_selector(SEL_CARD, state="visible", timeout=7000)
                except PWTimeoutError:
                    print(f"[WARN] Sin tarjetas visibles para '{modelo}'.")
                    continue

                # 3) Extraer
                cards = extract_cards(page, base_url=URL)
                for c in cards:
                    c["modelo_filtro"] = modelo
                results.extend(cards)
                print(f"[OK] {len(cards)} tarjetas extraídas para '{modelo}'")

            # 4) Salidas
            print("\n==== RESUMEN ====")
            print(f"Total tarjetas: {len(results)}")

            with open("mazda_modelos.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print("→ Guardado: mazda_modelos.json")

            if results:
                cols = ["modelo_filtro","brand","model","version","precio_desde_texto","precio_desde","precio_lista","bono_directo","bono_financiamiento","cotizar_url"]
                with open("mazda_modelos.csv", "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f); writer.writerow(cols)
                    for r in results:
                        row = [str(r.get(k, "") or "").replace("\n", " ").strip() for k in cols]
                        writer.writerow(row)
                        print(row)
                        if (row[0]==row[2]):
                            
                            precio = [row[5],row[5],row[6],row[6]]
                            tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
                            
                            datos = {
                                'modelo': row[0],
                                'marca': row[1],
                                'modelDetail':row[3],
                                'precio': precio,
                                'tiposprecio': tiposprecio
                                
                                
                            }
                            saveCar('Mazda',datos,'www.mazda.cl')
                print("→ Guardado: mazda_modelos.csv")

            if HEADLESS:
                ctx.close(); browser.close()
            else:
                print("\nHEADLESS=False: cierra el navegador cuando termines.")

        except Exception as e:
            print("❌ Error:", e)
            if HEADLESS:
                ctx.close(); browser.close()

if __name__ == "__main__":
    main()
