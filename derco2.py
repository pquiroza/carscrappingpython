# -*- coding: utf-8 -*-
# Scraper PLP genérico por marcas: guarda JSON/CSV separados por marca e incluye Precio Lista / Bono Marca / Bono Financiamiento

import time, re, csv, json, unicodedata, urllib.parse, os
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from utils import saveCar

# =============== CONFIG ===============
URL = "https://www.dercocenter.cl/busqueda"
HEADLESS = False
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 950}

# Marcas del sidebar tal cual aparecen en el label
BRANDS = ["Suzuki", "GWM", "Renault", "Changan", "JAC", "Deepal"]

# =============== SELECTORES ===============
# Cookies
SEL_COOKIES_MODAL = ".inner-body:has(.title-cookie)"
SEL_BTN_ENTENDIDO = ".inner-body .acepted a"

# Sidebar
SEL_BRAND_LABELS = "label.custom-control-label"
SEL_BRAND_INPUTS = "input.custom-control-input[name='filter_brand_']"

# Grilla / tarjetas
SEL_GRID_CONTAINER = ".container-card"
# IMPORTANTE: tomar solo la tarjeta desktop (evita duplicado por versión mobile)
SEL_CARD = ".card.card-search.show-desktop"
SEL_CARD_BODY = ".card-body"

# Campos de tarjeta
SEL_DISCOUNT = ".container-head-card p.mb-0, .container-head-card p"  # "9,6% DCTO"
SEL_BRAND_TEXT = ".brand-text"
SEL_MODEL_TEXT = ".model-text"
SEL_VERSION_TEXT = ".version-text"
SEL_PRICE_MAIN = ".prices h3.price-red"
SEL_PRICES_BLOCK = ".prices-disclaimer"
SEL_URLS_IN_CARD = "a[href^='/auto/']"

# Specs
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
    """
    Espera que el HTML de la grilla cambie para evitar leer resultados viejos.
    """
    page.wait_for_function(
        """(sel, prev) => {
            const c = document.querySelector(sel);
            if (!c) return false;
            const sig = (c.innerText || '').slice(0, 2000);
            return sig !== prev;
        }""",
        arg=(SEL_GRID_CONTAINER, prev_signature),
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
    try:
        page.wait_for_selector(SEL_COOKIES_MODAL, state="visible", timeout=timeout_ms)
    except PWTimeoutError:
        return True
    for action in (
        lambda: page.locator(SEL_BTN_ENTENDIDO).first.click(timeout=1500),
        lambda: page.locator(SEL_BTN_ENTENDIDO).first.click(force=True, timeout=1200),
        lambda: page.keyboard.press("Escape"),
        lambda: page.evaluate("""
            (sel) => {
              const m = document.querySelector(sel);
              if (m && m.parentElement) m.parentElement.removeChild(m);
              const overlay = document.querySelector('.cookies-overlay, .modal-backdrop, .v-modal, .overlay');
              if (overlay && overlay.parentElement) overlay.parentElement.removeChild(overlay);
            }
        """, SEL_COOKIES_MODAL)
    ):
        try:
            action()
            page.locator(SEL_COOKIES_MODAL).first.wait_for(state="detached", timeout=3000)
            return True
        except Exception:
            pass
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

    return {"checkbox_sel": checkbox_sel, "label_sel": label_sel, "collapse_sel": collapse_sel}

def ensure_brand_checked(page, brand_text: str, want_checked=True):
    b = find_brand_block(page, brand_text)
    chk = page.locator(b["checkbox_sel"]).first
    page.wait_for_selector(b["checkbox_sel"], state="attached", timeout=10000)
    is_on = chk.is_checked()
    if want_checked and not is_on:
        page.locator(b["label_sel"]).first.click(timeout=1500)
        time.sleep(0.2)
    elif not want_checked and is_on:
        page.locator(b["label_sel"]).first.click(timeout=1500)
        time.sleep(0.2)

def expand_brand_models(page, brand_text: str):
    b = find_brand_block(page, brand_text)
    if page.locator(b["collapse_sel"]).first.is_visible():
        return b
    lab = page.locator(b["label_sel"]).first
    brand_row = lab.locator("xpath=ancestor::div[contains(@class,'d-flex') and contains(@class,'justify-content-between')]").first
    toggle = brand_row.locator("[aria-controls]").first
    toggle.click(timeout=1500)
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
            lab.click(timeout=1200)
        else:
            inp.check(force=True, timeout=1200)
    except Exception:
        try:
            inp.check(force=True, timeout=1500)
        except Exception:
            return False
    for _ in range(12):
        if inp.is_checked():
            return True
        time.sleep(0.05)
    return False

# ---- NUEVO: limpiar todas las marcas antes de activar una
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
                        lab.click(timeout=800)
                    except Exception:
                        try:
                            inp.uncheck(force=True, timeout=800)
                        except Exception:
                            pass
            time.sleep(0.05)
        except Exception:
            pass
    # pequeña espera a que refresque
    try:
        page.wait_for_load_state("networkidle", timeout=3000)
    except Exception:
        pass

def select_only_brand(page, brand_text: str):
    # Desmarca todo y activa solo la marca pedida
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

            # Filtro de seguridad por marca activa
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

    # Deduplicar
    seen = set()
    out = []
    for r in data:
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
        "marca_filtro","modelo_filtro",
        "brand","model","version",
        "discount_pct",
        "price_main_text","price_main",
        "price_lista","bono_marca","bono_financiamiento",
        "consumo_urbano","traccion","pasajeros",
        "url_modelo"
    ]
    with open(fname, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            row = [r.get(k, "") if r.get(k) is not None else "" for k in cols]
            row = [str(x).replace("\n", " ").strip() for x in row]
            w.writerow(row)

# =============== MAIN ===============
def main():
    os.makedirs("out", exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()

        # Evitar modal cookies
        page.add_init_script("""
          try {
            localStorage.setItem('cookies-accepted', 'true');
            localStorage.setItem('cookie-consent', 'accepted');
          } catch(e) {}
        """)

        page.goto(URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=6000)
        except PWTimeoutError:
            pass
        close_cookies_modal(page)

        all_data = {}
        all_rows: List[Dict] = []

        for brand in BRANDS:
            print(f"\n=== MARCA: {brand} ===")

            # Activar SOLO esta marca (clave para evitar duplicados cruzados)
            select_only_brand(page, brand)

            # expandir modelos
            blk = expand_brand_models(page, brand)
            # limpiar modelos activos de esa marca
            uncheck_all_models_in_collapse(page, blk["collapse_sel"])

            # obtener modelos de esa marca
            modelos = get_model_values_in_collapse(page, blk["collapse_sel"])
            print(f"[INFO] Modelos {brand} ({len(modelos)}): {modelos}")

            brand_rows: List[Dict] = []

            for mv in modelos:
                print(f"[RUN] {brand} -> modelo: {mv}")
                # desmarcar y marcar solo este modelo
                uncheck_all_models_in_collapse(page, blk["collapse_sel"])
                prev = grid_signature(page)
                ok = click_model_in_collapse(page, blk["collapse_sel"], mv)
                if not ok:
                    print(f"[WARN] No pude marcar '{mv}'")
                    continue

                # esperar cambio real en grilla
                try:
                    wait_grid_refresh(page, prev, timeout_ms=12000)
                except Exception:
                    pass

                # esperar y extraer
                try:
                    wait_grid_ready(page, 15000)
                except PWTimeoutError:
                    print(f"[WARN] Sin tarjetas para '{brand}/{mv}'")
                    continue

                cards = extract_cards_from_grid(page, base_url=URL, current_brand=brand)
                print(f"[OK] Tarjetas extraídas (post filtro marca): {len(cards)}")
                for r in cards:
                    r["marca_filtro"] = brand
                    r["modelo_filtro"] = mv
                brand_rows.extend(cards)
                all_rows.extend(cards)

            all_data[brand] = brand_rows

            # guardar por marca
            slug = brand.lower().replace(" ", "_")
            json_path = os.path.join("out", f"plp_{slug}.json")


            for b in brand_rows:
                # si la fila viene con error, sáltala
                if b.get("_error"):
                    print(f"[WARN] fila con error omitida: {b.get('_error')}")
                    continue

                tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']

                price_main = b.get("price_main")
                price_lista = b.get("price_lista")
                bono_marca = b.get("bono_marca")
                
                # si no hay price_lista, no puedes calcular “crédito convencional”; usa fallback
                if price_lista is None:
                    precio = [price_main, price_main, price_main, price_main]
                else:
                    if bono_marca is not None:
                        precio_conv = price_lista - bono_marca
                    else:
                        precio_conv = price_lista
                    precio = [price_main, precio_conv, price_lista, price_lista]

                datos = {
                    "modelo": b.get("model"),
                    "marca": b.get("brand"),
                    "modelDetail": b.get("version"),
                    "precio": precio,
                    "tiposprecio": tiposprecio
                }

                print(datos)
                # evita llamar saveCar si no tienes marca/modelo mínimos
                if datos["marca"] and datos["modelo"]:
                    saveCar(datos["marca"], datos, "www.dercocenter.cl")







            csv_path = os.path.join("out", f"plp_{slug}.csv")
            save_json(json_path, brand_rows)
            for b in brand_rows:
                tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
                if b.get('bono_marca') is not None and b.get('price_lista') is not None:
                    precio = [b['price_main'], b['price_lista'] - b['bono_marca'], b['price_lista'], b['price_lista']]
                else:
                    # fallback: rellena con lo que haya
                    pl = b.get('price_lista')
                    precio = [b.get('price_main'), pl, pl, pl]
                datos = {
                    'modelo': b.get('model'),
                    'marca': b.get('brand'),
                    'modelDetail': b.get('version'),
                    'precio': precio,
                    'tiposprecio': tiposprecio
                }
                saveCar(b['brand'], datos, 'www.dercocenter.cl')
            save_csv(csv_path, brand_rows)
            print(f"→ Guardado {json_path} y {csv_path} ({len(brand_rows)} filas)")

        # global
        save_json(os.path.join("out", "plp_all.json"), all_rows)
        save_csv(os.path.join("out", "plp_all.csv"), all_rows)
        print(f"\n✅ Total global: {len(all_rows)} (out/plp_all.json & out/plp_all.csv)")

        if HEADLESS:
            ctx.close(); browser.close()
        else:
            print("\nHEADLESS=False: cierra el navegador para terminar.")

if __name__ == "__main__":
    main()
