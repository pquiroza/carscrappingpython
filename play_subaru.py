# -*- coding: utf-8 -*-
# Subaru PLP scraper robusto (modelos uno a uno, extracción y deduplicación)
import time, json, re, csv, urllib.parse, unicodedata
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from utils import saveCar
from utils import to_title_custom
# ===================== CONFIG =====================
URL = "https://www.subaru.cl/product-list-page"   # <-- AJUSTA si difiere
HEADLESS = False
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 1000}

# ===================== SELECTORES (según tu HTML) =================
UL_ID = "plp_list__Modelo"
SEL_UL = f"ul#{UL_ID}"
SEL_LI = f"{SEL_UL} li.plp_checkbox__items.plp_items__Modelo"
SEL_LABEL = f"{SEL_LI} label.plp_label__checkbox"
SEL_CHECKBOX = f"{SEL_LI} input.plp_input__checkbox"
SEL_SCROLL_CONTAINER = ".plp_scroll__container"

# Grilla central
SEL_GRID_WRAPPER = "section.plp_grid__wrapper"
SEL_ARTICLE = f"{SEL_GRID_WRAPPER} article.plp_vehicles_grid__content"
SEL_CARD = f"{SEL_ARTICLE} .plp_vehicles_grid__content__card.plp_grid_card"

# Dentro de cada card
SEL_BRAND = ".plp_grid_card__content h5.plp_grid_card__content__h5"
SEL_MODEL = ".plp_grid_card__content h3.plp_grid_card__content__h3"
SEL_VERSION = ".plp_grid_card__content h5.plp_grid_card__content__h5__fit"
SEL_PRICE_MAIN = ".plp_grid_card__content h2.plp_grid_card__content__h2"
SEL_P_ROWS = ".plp_grid_card__content p.plp_grid_card__content__p"
SEL_BTN_COTIZAR = ".plp_grid_card__buttons_group a.btn_primary"
SEL_BTN_PERSON = ".plp_grid_card__buttons_group a.btn_secondary_normal"

# Botón hipotético de aplicar filtros (si existe en la UI)
SEL_APLICAR = "button:has-text('Aplicar'), button:has-text('Ver resultados'), [role=button]:has-text('Aplicar'), [role=button]:has-text('Ver resultados')"

CURRENCY_CLEAN_RE = re.compile(r"[^\d]")



def clean_money(s: str) -> int | None:
    nums = re.sub(r"\D", "", s)  # elimina todo lo que NO sea dígito
    return int(nums) if nums else None
# ===================== UTILIDADES =================
def abs_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href or "")

def clean_money(text: str) -> Optional[int]:
    if not text:
        return None
    nums = CURRENCY_CLEAN_RE.sub("", text)
    return int(nums) if nums.isdigit() else None

def normalize_string(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

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

def expand_modelos_if_needed(page):
    page.wait_for_selector(SEL_UL, state="attached", timeout=15000)
    if page.locator(SEL_UL).first.is_visible():
        return
    # toggles comunes
    toggles = [
        f'[aria-controls="{UL_ID}"]', f'[data-target="#{UL_ID}"]', f'[href="#{UL_ID}"]',
        f'button[aria-controls="{UL_ID}"]', "button:has-text('Modelo')", "button:has-text('Modelos')",
        "summary:has-text('Modelo')","summary:has-text('Modelos')",
        "[role=button]:has-text('Modelo')","[role=button]:has-text('Modelos')",
        "a:has-text('Modelo')","a:has-text('Modelos')",
        "div:has-text('Modelo')","div:has-text('Modelos')",
    ]
    for sel in toggles:
        loc = page.locator(sel)
        if loc.count():
            try:
                loc.first.scroll_into_view_if_needed()
                loc.first.click(timeout=1200)
                time.sleep(0.2)
                if page.locator(SEL_UL).first.is_visible():
                    return
            except Exception:
                pass
    page.locator(SEL_UL).first.wait_for(state="visible", timeout=8000)

def visible_models_container(page):
    cont = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    if cont.count() > 0:
        return cont.first
    ul_vis = page.locator(f"{SEL_UL}:visible")
    if ul_vis.count() > 0:
        return ul_vis.first
    expand_modelos_if_needed(page)
    cont = page.locator(f"{SEL_SCROLL_CONTAINER}:visible")
    return cont.first if cont.count() > 0 else page.locator(SEL_UL).first

def scroll_to_model_value(page, container, modelo_value: str):
    js = """
    (wrap, ulSelector, value) => {
      const ul = document.querySelector(ulSelector);
      if (!ul) return false;
      const byVal = ul.querySelector(`li.plp_checkbox__items.plp_items__Modelo input.plp_input__checkbox[value="${value}"]`);
      let li = byVal ? byVal.closest('li') : ul.querySelector(`li#${CSS.escape(value)}`);
      if (!li) return false;
      const sc = wrap || ul;
      sc.scrollTop = Math.max(0, li.offsetTop - 60);
      return true;
    }
    """
    try:
        container.evaluate(js, SEL_UL, modelo_value)
        time.sleep(0.1)
    except Exception:
        pass

# ===================== CHECKBOX (MODELOS) =================
def get_model_values(page) -> List[str]:
    """Devuelve SOLO los valores de input[value] (fuente canónica para seleccionar)."""
    expand_modelos_if_needed(page)
    inputs = page.locator(f"{SEL_LI} input.plp_input__checkbox")
    vals: List[str] = []
    for i in range(inputs.count()):
        try:
            val = (inputs.nth(i).get_attribute("value") or "").strip()
            if val:
                vals.append(val)
        except Exception:
            pass
    # dedup preservando orden
    seen=set()
    return [v for v in vals if not (v in seen or seen.add(v))]

def locate_item_by_value_or_id(page, modelo_value: str) -> Tuple[Optional[object], Optional[object]]:
    inp = page.locator(f"{SEL_LI} input.plp_input__checkbox[value='{modelo_value}']").first
    if inp.count():
        input_id = inp.get_attribute("id")
        lab = page.locator(f"label[for='{input_id}']").first if input_id else None
        return (lab if lab and lab.count() else None, inp)
    li = page.locator(f"{SEL_LI}#{modelo_value}").first
    if li.count():
        lab = li.locator("label.plp_label__checkbox").first
        inp = li.locator("input.plp_input__checkbox").first
        return (lab if lab.count() else None, inp if inp.count() else None)
    return None, None

def uncheck_all_models(page):
    """Desmarca TODOS los modelos (dos pasadas + forzado JS si quedara alguno)."""
    expand_modelos_if_needed(page)
    cont = visible_models_container(page)

    # 1) Click en labels de los checados visibles/invisibles
    for _ in range(2):  # doble pasada por seguridad
        items = page.locator(f"{SEL_LI}:has({SEL_CHECKBOX}:checked)")
        if items.count() == 0:
            break
        for i in range(items.count()):
            li = items.nth(i)
            try:
                val = li.locator("input.plp_input__checkbox").first.get_attribute("value") or ""
                lbl = li.locator("label.plp_label__checkbox").first
                if not lbl.is_visible():
                    scroll_to_model_value(page, cont, val)
                try:
                    lbl.click(timeout=1000)
                except Exception:
                    page.evaluate("el => el.click()", lbl)
            except Exception:
                pass
        time.sleep(0.12)

    # 2) Si aún queda algo checked, forzar vía JS (no siempre necesario)
    leftover = page.locator(f"{SEL_CHECKBOX}:checked")
    if leftover.count():
        page.evaluate(
            """(ulSel) => {
                const ul = document.querySelector(ulSel);
                if (!ul) return;
                ul.querySelectorAll('input.plp_input__checkbox:checked')
                  .forEach(inp => { inp.checked = false; inp.dispatchEvent(new Event('change', {bubbles:true})); });
            }""",
            SEL_UL
        )
        time.sleep(0.15)

    # 3) Botón aplicar (si existe)
    aplicar = page.locator(SEL_APLICAR)
    if aplicar.count():
        try:
            aplicar.first.click(timeout=800)
            time.sleep(0.15)
        except Exception:
            pass

def click_model_by_value(page, modelo_value: str) -> bool:
    expand_modelos_if_needed(page)
    cont = visible_models_container(page)

    scroll_to_model_value(page, cont, modelo_value)
    label_loc, input_loc = locate_item_by_value_or_id(page, modelo_value)

    if not label_loc and not input_loc:
        # reintenta un poco más por si hay lazy render
        for _ in range(8):
            scroll_to_model_value(page, cont, modelo_value)
            label_loc, input_loc = locate_item_by_value_or_id(page, modelo_value)
            if label_loc or input_loc:
                break

    if not label_loc and not input_loc:
        raise RuntimeError(f"No encontré el modelo '{modelo_value}'")

    marked = False
    try:
        if input_loc and input_loc.count():
            if not input_loc.is_checked():
                if input_loc.is_visible():
                    try:
                        input_loc.check(timeout=1200)
                    except Exception:
                        input_loc.check(force=True)
                else:
                    # input oculto -> click al label
                    if label_loc and label_loc.count():
                        try:
                            label_loc.click(timeout=1200)
                        except Exception:
                            page.evaluate("el => el.click()", label_loc)
            # verificación
            for _ in range(10):
                if input_loc.is_checked():
                    marked = True; break
                time.sleep(0.05)
        else:
            # sin input visible: click en label
            if label_loc and label_loc.count():
                try:
                    label_loc.click(timeout=1200); marked = True
                except Exception:
                    page.evaluate("el => el.click()", label_loc); marked = True
    except Exception:
        if label_loc and label_loc.count():
            try:
                page.evaluate("el => el.click()", label_loc); marked = True
            except Exception:
                marked = False

    # botón aplicar si existe
    aplicar = page.locator(SEL_APLICAR)
    if aplicar.count():
        try:
            aplicar.first.click(timeout=800)
            time.sleep(0.15)
        except Exception:
            pass

    return marked

# ===================== VALIDACIÓN Y EXTRACCIÓN =================
def extract_cards(page, base_url: str) -> List[Dict]:
    cards = page.locator(SEL_CARD)
    out: List[Dict] = []
    for i in range(cards.count()):
        card = cards.nth(i)
        try:
            brand_txt = card.locator(SEL_BRAND).first.inner_text().strip() if card.locator(SEL_BRAND).count() else None
            model_txt = card.locator(SEL_MODEL).first.inner_text().strip() if card.locator(SEL_MODEL).count() else None
            version_txt = card.locator(SEL_VERSION).first.inner_text().strip() if card.locator(SEL_VERSION).count() else None
            price_main_text = card.locator(SEL_PRICE_MAIN).first.inner_text().strip() if card.locator(SEL_PRICE_MAIN).count() else None
            price_main_int = clean_money(price_main_text or "")

            # mapear filas <p><span>Etiqueta</span><strong>valor</strong>
            p_rows = card.locator(SEL_P_ROWS)
            campos: Dict[str, Optional[str]] = {}
            for j in range(p_rows.count()):
                p = p_rows.nth(j)
                span = p.locator("span")
                strong = p.locator("strong.plp_grid_card__content__p__strong__price")
                etiqueta = span.first.inner_text().strip() if span.count() else None
                valor = strong.first.inner_text().strip() if strong.count() else None
                if etiqueta:
                    campos[etiqueta] = valor

            precio_campania_p = clean_money(campos.get("Precio de Campaña") or "") if "Precio de Campaña" in campos else None
            bono_directo = clean_money(campos.get("Bono Directo") or "") if "Bono Directo" in campos else None
            bono_fin = clean_money(campos.get("Bono Financiamiento") or "") if "Bono Financiamiento" in campos else None

            cotizar_url = None
            if card.locator(SEL_BTN_COTIZAR).count():
                href_rel = card.locator(SEL_BTN_COTIZAR).first.get_attribute("href") or ""
                cotizar_url = abs_url(base_url, href_rel)
            personalizar_url = None
            if card.locator(SEL_BTN_PERSON).count():
                href_rel = card.locator(SEL_BTN_PERSON).first.get_attribute("href") or ""
                personalizar_url = abs_url(base_url, href_rel)

            out.append({
                "brand": brand_txt,
                "model": model_txt,
                "version": version_txt,
                "price_main_text": price_main_text,
                "price_main": price_main_int,
                "precio_de_campania_p": precio_campania_p,
                "bono_directo": bono_directo,
                "bono_financiamiento": bono_fin,
                "cotizar_url": cotizar_url,
                "personalizar_url": personalizar_url,
            })
        except Exception as e:
            out.append({"_error": str(e)})
    return out

def filter_cards_by_selected_model(cards: List[Dict], selected_value: str) -> List[Dict]:
    """
    Filtra tarjetas que no correspondan al modelo seleccionado.
    Considera normalización (case/acentos) y equivalencias típicas.
    """
    sel_norm = normalize_string(selected_value)
    keep: List[Dict] = []

    # equivalencias simples (ej: 'all new forester' ≈ 'forester'?) -> personaliza si hace falta
    def equiv(card_model_norm: str) -> bool:
        if card_model_norm == sel_norm:
            return True
        # reglas suaves
        if sel_norm in card_model_norm or card_model_norm in sel_norm:
            return True
        return False

    for c in cards:
        cm = normalize_string(c.get("model"))
        if equiv(cm):
            keep.append(c)
    return keep

def dedupe_rows(rows: List[Dict], selected_value: str) -> List[Dict]:
    """
    Deduplica con prioridad por cotizar_url; si no, (brand, model_norm, version_norm, price_main).
    """
    seen = set()
    out = []
    for r in rows:
        key = None
        if r.get("cotizar_url"):
            key = ("cot", r["cotizar_url"])
        else:
            key = (
                "cmp",
                normalize_string(r.get("brand")),
                normalize_string(r.get("model")),
                normalize_string(r.get("version")),
                r.get("price_main"),
            )
        if key in seen:
            continue
        seen.add(key)
        # guarda también el modelo de filtro (value canónico)
        r["modelo_filtro"] = selected_value
        out.append(r)
    return out

# ===================== MAIN =====================
def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        try:
            try:
                page.wait_for_load_state("networkidle", timeout=7000)
            except PWTimeoutError:
                pass

            model_values = get_model_values(page)
            print(f"[INFO] Modelos detectados (por value): {model_values}")

            results: List[Dict] = []
            article_hash: Optional[str] = None

            for mv in model_values:
                print(f"\n[RUN] {mv}: limpiando y aplicando filtro…")
                uncheck_all_models(page)
                ok = click_model_by_value(page, mv)
                if not ok:
                    print(f"[WARN] No se pudo marcar '{mv}'. Sigo…")
                    continue

                # esperar re-render y asegurar presencia de cards
                article_hash = wait_grid_update(page, previous_hash=article_hash, timeout_ms=15000)
                try:
                    page.wait_for_selector(SEL_CARD, state="visible", timeout=7000)
                except PWTimeoutError:
                    print(f"[WARN] Sin tarjetas visibles para '{mv}'.")
                    continue

                # extraer -> filtrar por modelo seleccionado -> deduplicar
                raw_cards = extract_cards(page, base_url=URL)
                filtered = filter_cards_by_selected_model(raw_cards, mv)
                deduped = dedupe_rows(filtered, mv)

                print(f"[OK] {len(deduped)} tarjetas válidas para '{mv}' (raw {len(raw_cards)})")
                results.extend(deduped)

            # guardar
            print("\n==== RESUMEN ====")
            print(f"Total tarjetas (limpias): {len(results)}")

            with open("subaru_modelos.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
                for r in results:
                    
                    precio = []
                    tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
                    if r['price_main'] !=None and r['precio_de_campania_p'] !=None and r['bono_directo'] != None and r['bono_financiamiento'] != None:
                        precio = [r['price_main'],r['precio_de_campania_p']-r['bono_directo'],r['precio_de_campania_p'],r["precio_de_campania_p"]]
                    if r['bono_directo']   == None:
                        precio = [r['price_main'],r['precio_de_campania_p'],r['precio_de_campania_p'],r["precio_de_campania_p"]]
                    if r['precio_de_campania_p'] == None:
                        precio = [r['price_main'],r['price_main']+r['bono_directo'],r['price_main']+r['bono_directo']+r['bono_financiamiento'],r['price_main']+r['bono_directo']+r['bono_financiamiento']]
                        
                    datos = {
                        'modelo': to_title_custom(r['model']),
                        'marca': to_title_custom(r['brand']),
                        'modelDetail': r['version'],
                        'tiposprecio': tiposprecio,
                        'precio': precio
                        
                    }
                    print(f"Datos a guardar {datos}")
                    print("-"*100)
                    saveCar('Subaru',datos,'www.subaru.cl')
            print("→ Guardado: subaru_modelos.json")

            if results:
                cols = [
                    "modelo_filtro","brand","model","version",
                    "price_main_text","price_main",
                    "precio_de_campania_p","bono_directo","bono_financiamiento",
                    "cotizar_url","personalizar_url"
                ]
                with open("subaru_modelos.csv", "w", encoding="utf-8", newline="") as f:
                    w = csv.writer(f); w.writerow(cols)
                    for r in results:
                        row = [str(r.get(k, "") or "").replace("\n", " ").strip() for k in cols]
                        w.writerow(row)
                       
                        
                
                print("→ Guardado: subaru_modelos.csv")

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
