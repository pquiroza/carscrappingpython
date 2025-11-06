# -*- coding: utf-8 -*-
# Scraper Difor (menu Marcas -> página de marca -> modelos -> ficha del modelo)
# Guarda JSON/CSV por marca y un global.

import os, re, csv, json, time, urllib.parse, unicodedata
from typing import List, Dict, Optional, Callable
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# =================== CONFIG ===================
URL_HOME = "https://www.difor.cl/"
HEADLESS = False
SLOWMO_MS = 0
VIEWPORT = {"width": 1400, "height": 950}
OUT_DIR = "out_difor"

# Limitar marcas (None = todas)
BRANDS_LIMIT: Optional[List[str]] = None
# Ejemplo:
# BRANDS_LIMIT = ["Ford", "Chevrolet"]

# =================== UTILS ===================
def abs_url(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urllib.parse.urljoin(base, href)

def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip()

def strip_accents_lower(s: str) -> str:
    s = norm_text(s)
    return s.lower()

def money_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    nums = re.sub(r"[^\d]", "", s)
    try:
        return int(nums) if nums else None
    except Exception:
        return None

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

def dedupe(rows: List[Dict], key_fn: Callable[[Dict], object]) -> List[Dict]:
    seen = set()
    out = []
    for r in rows:
        k = key_fn(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

def save_json(path: str, rows: List[Dict]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def save_csv(path: str, rows: List[Dict], cols: Optional[List[str]] = None):
    if cols is None:
        # columnas habituales
        cols = [
            "brand", "brand_href",
            "model", "model_label",
            "price_main_text", "price_main",
            "price_lista", "bono_marca", "bono_financiamiento",
            "model_url",
            "from_page_title", "from_page_price_text",
        ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            row = [r.get(k, "") if r.get(k) is not None else "" for k in cols]
            row = [str(x).replace("\n", " ").strip() for x in row]
            w.writerow(row)

# =================== COOKIES / ESPERAS ===================
def try_close_cookies(page, timeout_ms=4000):
    """
    Intenta cerrar el modal de cookies (si aparece).
    """
    # General: modal/overlay + botón "Entendido", "Aceptar" o similar
    selectors = [
        ".inner-body .acepted a",            # caso de tu snippet en otra web
        "button:has-text('Entendido')",
        "button:has-text('Aceptar')",
        "button:has-text('Acepto')",
        "button:has-text('Aceptar todas')",
        "text=Entendido",
    ]
    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    except PWTimeoutError:
        pass
    for sel in selectors:
        try:
            if page.locator(sel).first.is_visible():
                page.locator(sel).first.click(timeout=1200)
                time.sleep(0.2)
                break
        except Exception:
            pass
    # Fallback: ESC o remover overlays conocidos
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    try:
        page.evaluate("""
          () => {
            const sel = ['.cookies-overlay','.modal-backdrop','.v-modal','.overlay','.MuiBackdrop-root'];
            for (const s of sel) {
              document.querySelectorAll(s).forEach(e => e.remove());
            }
          }
        """)
    except Exception:
        pass

# =================== MENÚ MARCAS ===================
def open_brands_menu(page) -> bool:
    """
    Abre el menú de Marcas (header). Devuelve True si el <role=menu> queda visible.
    """
    # Variantes posibles: botón "Marcas" o "Modelos" con menú MUI
    try:
        # Opción 1: botón por rol y nombre
        btn = page.get_by_role("button", name=re.compile(r"marcas", re.I)).first
        if btn and btn.count():
            btn.click(timeout=1500)
    except Exception:
        pass

    # Opción 2: botón con data-testid/aria-controls
    if not page.get_by_role("menu").count():
        try:
            toggles = page.locator("[aria-controls]:not([aria-controls=''])").all()
            for t in toggles:
                try:
                    name = norm_text(t.inner_text())
                    # si el texto sugiere Marcas, clic
                    if re.search(r"marca", strip_accents_lower(name)):
                        t.click(timeout=1200)
                        break
                except Exception:
                    pass
        except Exception:
            pass

    # Opción 3: clic en cualquier "Marcas"
    if not page.get_by_role("menu").count():
        try:
            page.locator("text=Marcas").first.click(timeout=1000)
        except Exception:
            pass

    # Esperar menu
    try:
        page.wait_for_selector("[role='menu'], ul[role='menu']", state="visible", timeout=3500)
        return True
    except PWTimeoutError:
        return False

def get_brands_from_menu(page) -> List[Dict]:
    """
    Lee los <li role=menuitem> y saca nombre + href.
    """
    items = page.locator("[role='menu'] [role='menuitem'], ul[role='menu'] [role='menuitem']")
    rows = []
    for i in range(items.count()):
        it = items.nth(i)
        try:
            a = it.locator("a[href]").first
            href = a.get_attribute("href") if a.count() else None
            # Nombre visible
            name = None
            try:
                # P suele contener el nombre
                name = norm_text(it.locator("p").first.inner_text())
            except Exception:
                name = norm_text(it.inner_text())
            if href and name:
                # href puede venir sin slash inicial (ej. "jetour-chile")
                url = abs_url(URL_HOME, href)
                rows.append({
                    "brand": name,
                    "href": href,
                    "url": url
                })
        except Exception:
            pass

    # Filtrar duplicados por URL
    rows = dedupe(rows, key_fn=lambda r: r.get("url"))
    return rows

# =================== PÁGINA DE MARCA (modelos) ===================
def ensure_tab_todos(page):
    """
    Si el contenedor de modelos aún no está visible, intenta activar el tab 'Todos'.
    """
    if page.locator("#listing-collections a#collection-card").count():
        return
    # Por rol 'tab'
    try:
        btn = page.get_by_role("tab", name=re.compile(r"^\s*Todos\s*$", re.I)).first
        if btn and btn.count():
            btn.click(timeout=1500)
            page.wait_for_selector("#listing-collections a#collection-card", state="visible", timeout=6000)
            return
    except Exception:
        pass
    # Por id "*-todos-chile"
    try:
        loc = page.locator("button[id$='-todos-chile'][role='tab']").first
        if loc and loc.count():
            loc.click(timeout=1500)
            page.wait_for_selector("#listing-collections a#collection-card", state="visible", timeout=6000)
    except Exception:
        pass

def wait_brand_listing(page) -> bool:
    try:
        page.wait_for_selector("#listing-collections", state="visible", timeout=8000)
        page.wait_for_selector("#listing-collections a#collection-card", state="visible", timeout=8000)
        return True
    except PWTimeoutError:
        return False

def extract_models_from_brand_page(page, base_url: str, brand_name: str, brand_href: str) -> List[Dict]:
    ok = wait_brand_listing(page)
    if not ok:
        return []

    cards = page.locator("#listing-collections a#collection-card")
    rows: List[Dict] = []

    for i in range(cards.count()):
        a = cards.nth(i)
        try:
            href = a.get_attribute("href")
            model_url = abs_url(base_url, href) if href else None

            # Nombre del modelo (h2 dentro de la tarjeta)
            model = None
            try:
                model = norm_text(a.locator("h2").first.inner_text())
            except Exception:
                pass

            # Precio “Desde $X”
            price_main_text = None
            price_main = None
            try:
                price_main_text = norm_text(a.locator("span.MuiTypography-h6").first.inner_text())
                price_main = money_to_int(price_main_text)
            except Exception:
                # Fallback: buscar cualquier $ dentro de la tarjeta
                try:
                    block_txt = norm_text(a.inner_text())
                    m = re.search(r"\$[\d\.\s]+", block_txt)
                    if m:
                        price_main_text = m.group(0)
                        price_main = money_to_int(price_main_text)
                except Exception:
                    pass

            # Tag como "Nuevo/New" (opcional)
            model_label = None
            try:
                model_label = norm_text(a.locator("span.MuiTypography-body1").first.inner_text())
            except Exception:
                pass

            rows.append({
                "brand": brand_name,
                "brand_href": brand_href,
                "model": model,
                "model_label": model_label,
                "price_main_text": price_main_text,
                "price_main": price_main,
                "price_lista": None,             # se intentará en la ficha
                "bono_marca": None,
                "bono_financiamiento": None,
                "model_url": model_url,
                "from_page_title": None,         # se llenará en la ficha
                "from_page_price_text": None,    # se llenará en la ficha
            })
        except Exception as e:
            rows.append({"_error": repr(e)})

    # Dedupe por URL del modelo
    rows = dedupe(rows, key_fn=lambda r: r.get("model_url"))
    return rows

# =================== FICHA DEL MODELO ===================
def wait_model_page_ready(page) -> bool:
    try:
        # título o imagen principal listo
        page.wait_for_load_state("domcontentloaded", timeout=6000)
        # Algo visible (título, o cualquier h1/h2)
        if page.locator("h1, h2").count():
            return True
        return True
    except PWTimeoutError:
        return False

def extract_from_model_page(page) -> Dict:
    """
    Extrae lo que se pueda de la ficha del modelo.
    No todas las fichas comparten estructura; usamos heurísticas.
    """
    out: Dict = {}

    # Título principal
    try:
        # Prioriza h1 visible
        for sel in ["h1", "#MarcaVehiculosGamaTitulo", ".MuiTypography-h4"]:
            loc = page.locator(sel).first
            if loc.count():
                out["from_page_title"] = norm_text(loc.inner_text())
                break
    except Exception:
        pass

    # Precio visible (primero h6 "Desde", luego cualquier $)
    try:
        # muchos “Desde $X” usan .MuiTypography-h6
        ptxt = None
        if page.locator(".MuiTypography-h6").count():
            ptxt = norm_text(page.locator(".MuiTypography-h6").first.inner_text())
        else:
            txt = norm_text(page.locator("body").inner_text())
            m = re.search(r"\$[\d\.\s]{4,}", txt)
            if m:
                ptxt = m.group(0)
        if ptxt:
            out["from_page_price_text"] = ptxt
            val = money_to_int(ptxt)
            if val and not out.get("price_main"):
                out["price_main"] = val
    except Exception:
        pass

    # Si existieran bloques de "Precio lista / Bono Marca / Bono Financiamiento"
    try:
        txt = strip_accents_lower(norm_text(page.locator("body").inner_text()))
        # Busca el último number luego de 'precio lista'
        m_lista = re.search(r"precio\s+lista[^$]*\$[\d\.\s]+", txt, re.I)
        if m_lista:
            raw = re.search(r"\$[\d\.\s]+", m_lista.group(0))
            if raw:
                out["price_lista"] = money_to_int(raw.group(0))

        m_bmarca = re.search(r"bono\s+marca[^$]*\$[\d\.\s]+", txt, re.I)
        if m_bmarca:
            raw = re.search(r"\$[\d\.\s]+", m_bmarca.group(0))
            if raw:
                out["bono_marca"] = money_to_int(raw.group(0))

        m_bfin = re.search(r"bono\s+financ", txt, re.I)
        if m_bfin:
            raw2 = re.search(r"\$[\d\.\s]+", txt[m_bfin.start(): m_bfin.start()+80])
            if raw2:
                out["bono_financiamiento"] = money_to_int(raw2.group(0))
    except Exception:
        pass

    return out

def visit_model(page, url: str) -> Dict:
    """
    Abre la URL de un modelo y devuelve un dict con info adicional.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
    except PWTimeoutError:
        # segundo intento
        page.goto(url, wait_until="domcontentloaded")
    try_close_cookies(page, timeout_ms=2500)
    wait_model_page_ready(page)
    extra = extract_from_model_page(page)
    return extra

# =================== MAIN ===================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()

        # Evitar algunos bloqueos de cookies por localStorage
        page.add_init_script("""
            try {
              localStorage.setItem('cookies-accepted','true');
              localStorage.setItem('cookie-consent','accepted');
            } catch(e) {}
        """)

        # Ir al home
        page.goto(URL_HOME, wait_until="domcontentloaded")
        try_close_cookies(page)

        # Abrir menú de marcas
        if not open_brands_menu(page):
            raise RuntimeError("No pude abrir el menú de marcas (role=menu).")

        # Obtener marcas
        brands = get_brands_from_menu(page)
        if not brands:
            raise RuntimeError("No encontré marcas en el menú.")

        print(f"[INFO] Marcas encontradas: {len(brands)}")
        if BRANDS_LIMIT:
            bl = set([strip_accents_lower(b) for b in BRANDS_LIMIT])
            brands = [b for b in brands if strip_accents_lower(b["brand"]) in bl]
            print(f"[INFO] Filtrando por límite -> {len(brands)}")

        all_rows: List[Dict] = []

        for b in brands:
            brand_name = b["brand"]
            brand_url = b["url"]
            print(f"\n=== MARCA: {brand_name} -> {brand_url}")

            # Abrir página de marca
            try:
                page.goto(brand_url, wait_until="domcontentloaded", timeout=20000)
            except PWTimeoutError:
                page.goto(brand_url, wait_until="domcontentloaded")
            try_close_cookies(page, timeout_ms=2000)

            # Asegurar tab "Todos"
            ensure_tab_todos(page)

            # Extraer modelos
            models = extract_models_from_brand_page(page, URL_HOME, brand_name, b["href"])
            print(f"[OK] Modelos en {brand_name}: {len(models)}")

            # Visitar cada modelo para extra
            enriched: List[Dict] = []
            for r in models:
                url_model = r.get("model_url")
                if not url_model:
                    enriched.append(r)
                    continue
                try:
                    extra = visit_model(page, url_model)
                    # fusionar: no sobre-escribir price_main si ya viene desde la tarjeta
                    merged = dict(r)
                    for k, v in extra.items():
                        if k == "price_main" and r.get("price_main"):
                            continue
                        merged[k] = v
                    enriched.append(merged)
                    print(f"  → {r.get('model')} OK")
                except Exception as e:
                    print(f"  → {r.get('model')} ERROR: {e}")
                    enriched.append(r)

            # Dedup final por URL
            enriched = dedupe(enriched, key_fn=lambda x: x.get("model_url"))

            # Guardar por marca
            slug = strip_accents_lower(brand_name).replace(" ", "-")
            json_path = os.path.join(OUT_DIR, f"difor_{slug}.json")
            csv_path  = os.path.join(OUT_DIR, f"difor_{slug}.csv")
            save_json(json_path, enriched)
            save_csv(csv_path, enriched)
            print(f"→ Guardado {json_path} / {csv_path} ({len(enriched)} filas)")

            all_rows.extend(enriched)

            # Volver al home y reabrir menú de marcas (para siguiente iteración)
            page.goto(URL_HOME, wait_until="domcontentloaded")
            try_close_cookies(page)
            open_brands_menu(page)

        # Global
        save_json(os.path.join(OUT_DIR, "difor_all.json"), all_rows)
        save_csv(os.path.join(OUT_DIR, "difor_all.csv"), all_rows)
        print(f"\n✅ Total global: {len(all_rows)}")

        if HEADLESS:
            ctx.close(); browser.close()
        else:
            print("\nHEADLESS=False: cierra el navegador para terminar.")

if __name__ == "__main__":
    main()
