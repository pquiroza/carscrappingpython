# -*- coding: utf-8 -*-
import os
import re
import json
import time
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from utils import saveCar
# ============ UTILIDADES ============
marcas_difor = [
    {
    "brand": "Ford",
    "url": "https://www.difor.cl/ford-chile"
  },
    
    {
    "brand": "Jetour",
    "url": "https://www.difor.cl/jetour-chile"
  },
    
    {
    "brand": "Opel",
    "url": "https://www.difor.cl/opel-chile"
  },
    {
    "brand": "Mitsubishi",
    "url": "https://www.difor.cl/mitsubishi-motors-chile"
  },
     {
    "brand": "Maxus",
    "url": "https://www.difor.cl/maxus-chile"
  },
  {
    "brand": "Karry",
    "url": "https://www.difor.cl/karry-chile"
  },
  {
    "brand": "Kaiyi",
    "url": "https://www.difor.cl/kaiyi-chile"
  }
]
PRECIO_RE = re.compile(r"\$[\d\.\s]+")
NUM_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

def precio_a_int(txt: str):
    if not txt: return None
    txt = txt.replace("\xa0", " ")
    m = PRECIO_RE.search(txt)
    if not m: return None
    num = re.sub(r"[^\d]", "", m.group(0))
    return int(num) if num.isdigit() else None

def to_int_num(txt: str):
    if not txt: return None
    t = txt.replace(".", "").replace(",", ".")
    m = NUM_RE.search(t)
    return int(float(m.group(1))) if m else None

def scroll_suave(page, pasos=4, pausa=0.35):
    for i in range(1, pasos + 1):
        page.evaluate("(y)=>window.scrollTo(0,y)", i * 800)
        time.sleep(pausa)
    page.evaluate("window.scrollTo(0,0)")
    time.sleep(0.15)

def ensure_dir():
    os.makedirs("salida_modelos", exist_ok=True)

# ============ EXTRACCIÓN DE MODELOS ============

def extraer_modelos(page):
    """Extrae nombre, precio desde, link, imagen."""
    page.wait_for_selector("#listing-collections", timeout=20000)
    scroll_suave(page)
    cards = page.locator('#listing-collections a#collection-card')
    modelos = []
    for i in range(cards.count()):
        a = cards.nth(i)
        try:
            nombre = a.locator("h2").inner_text(timeout=1500).strip()
        except Exception:
            nombre = ""
        try:
            precio_raw = a.locator(".MuiTypography-h6").inner_text(timeout=1500)
        except Exception:
            precio_raw = ""
        precio_int = precio_a_int(precio_raw)
        href = a.get_attribute("href") or ""
        url = urljoin(page.url, href)
        try:
            img = a.locator("img").first.get_attribute("src") or ""
        except Exception:
            img = ""
        modelos.append({
            "modelo": nombre,
            "precio_desde_raw": precio_raw,
            "precio_desde_int": precio_int,
            "url_modelo": url,
            "img": img
        })
    return modelos

# ============ EXTRACCIÓN DE VERSIONES ============

def parse_item_values(card):
    valores = {}
    filas = card.locator(".MuiGrid-container.item-value")
    for i in range(filas.count()):
        fila = filas.nth(i)
        try:
            label = fila.locator(".css-17fd5p").inner_text().lower()
            val = fila.locator(".css-uztjiy").inner_text()
            val_int = precio_a_int(val)
            if "inteligente" in label: valores["precio_credito_inteligente_int"] = val_int
            elif "convencional" in label: valores["precio_credito_convencional_int"] = val_int
            elif "todo medio" in label: valores["precio_todo_medio_pago_int"] = val_int
            elif "lista" in label: valores["precio_lista_int"] = val_int
        except Exception:
            pass
    return valores

def parse_highlights(card):
    datos = {"cc": None, "combustible": "", "transmision": "", "potencia_hp": None}
    props = card.locator(".highlight-properties-container .highlight-property")
    for i in range(props.count()):
        txt = props.nth(i).locator("p").inner_text()
        if "cc" in txt.lower(): datos["cc"] = to_int_num(txt)
        elif any(k in txt.lower() for k in ["gasolina","diesel","híbr","electr"]): datos["combustible"] = txt
        elif any(k in txt.lower() for k in ["automática","manual","cvt","dct"]): datos["transmision"] = txt
        elif "hp" in txt.lower(): datos["potencia_hp"] = to_int_num(txt)
    return datos

def extraer_versiones(page, modelo, marca):
    versiones = []
    page.wait_for_selector(".splide__list", timeout=20000)
    scroll_suave(page)
    slides = page.locator(".splide__list li.splide__slide:has(#new-car-version-card)")
    for i in range(slides.count()):
        card = slides.nth(i).locator("#new-car-version-card")
        version = ""
        try:
            version = card.locator(".MuiCardHeader-content .css-wp624j").inner_text().strip()
        except Exception:
            pass
        precio_card_raw = card.locator(".card-price-title").inner_text() if card.locator(".card-price-title").count() else ""
        precio_card_int = precio_a_int(precio_card_raw)
        bono_raw = card.locator(".css-ycodjm").inner_text() if card.locator(".css-ycodjm").count() else ""
        bono_int = precio_a_int(bono_raw)
        precios = parse_item_values(card)
        highlights = parse_highlights(card)
        href = card.locator(".MuiCardActions-root a[href]").get_attribute("href") if card.locator(".MuiCardActions-root a[href]").count() else ""
        url_version = urljoin(page.url, href) if href else ""
        versiones.append({
            "marca": marca,
            "modelo": modelo,
            "version": version,
            "precio_card_int": precio_card_int,
            "bono_int": bono_int,
            "precio_credito_inteligente_int": precios.get("precio_credito_inteligente_int"),
            "precio_credito_convencional_int": precios.get("precio_credito_convencional_int"),
            "precio_todo_medio_pago_int": precios.get("precio_todo_medio_pago_int"),
            "precio_lista_int": precios.get("precio_lista_int"),
            **highlights,
            "url_modelo": page.url,
            "url_version": url_version
        })
    return versiones

# ============ MAIN ============

def close_cookies_if_any(page):
    # Intenta cerrar banners comunes
    candidates = [
        "button:has-text('Aceptar')",
        "button:has-text('ACEPTAR')",
        "button:has-text('Acepto')",
        ".MuiDialog-root button:has-text('Aceptar')",
        "button[aria-label='close']",
        "button[aria-label='Close']",
    ]
    for sel in candidates:
        try:
            if page.locator(sel).first.is_visible():
                page.locator(sel).first.click(timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def click_tab_todos_generico(page):
    # 1) si existe un id que termina en -todos-chile (ej: ford-todos-chile, opel-todos-chile...)
    try:
        todos = page.locator("[id$='-todos-chile']")
        if todos.count() > 0 and todos.first.is_visible():
            todos.first.click(timeout=1200)
            time.sleep(0.2)
            return
    except Exception:
        pass
    # 2) si no, intenta con el texto "Todos"
    try:
        btn = page.locator("button[role='tab']:has-text('Todos')")
        if btn.count() > 0 and btn.first.is_visible():
            btn.first.click(timeout=1200)
            time.sleep(0.2)
            return
    except Exception:
        pass
    # 3) si nada, intenta recorrer todos los tabs (por si el grid se monta tras cambiar cualquiera)
    try:
        tabs = page.locator("button[role='tab']")
        for i in range(min(6, tabs.count())):
            t = tabs.nth(i)
            if t.is_visible():
                t.click(timeout=1000)
                time.sleep(0.2)
    except Exception:
        pass

def wait_grid_with_scroll(page, max_tries=8):
    """Hace scroll y espera a que aparezcan tarjetas de modelos."""
    for i in range(max_tries):
        # intenta ver tarjetas directas
        if page.locator('#listing-collections a#collection-card').count() > 0:
            return True
        # respaldo: cards con hrefs relativos (ej: "-chile")
        if page.locator("#listing-collections a[href*='-chile']").count() > 0:
            return True
        # scroll y pequeña pausa para lazy-load
        page.evaluate("(y)=>window.scrollTo(0,y)", (i+1) * 800)
        time.sleep(0.5)
    return (page.locator('#listing-collections a#collection-card').count() > 0)

def scrape_brand_flat_json(brand_url, nombre_marca="marca", headless=False):
    ensure_dir()
    all_versions = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--window-size=1366,900"])
        ctx = browser.new_context(
            locale="es-CL",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
        )
        page = ctx.new_page()
        page.goto(brand_url, wait_until="domcontentloaded", timeout=45000)

        close_cookies_if_any(page)
        click_tab_todos_generico(page)

        # Asegura que el grid cargue
        ok = wait_grid_with_scroll(page, max_tries=10)
        if not ok:
            # Último intento: baja y sube fuerte para forzar lazy-load
            for _ in range(3):
                page.mouse.wheel(0, 1000); time.sleep(0.3)
            page.mouse.wheel(0, -3000); time.sleep(0.3)

        modelos = extraer_modelos(page)
        # Respaldo: si no encontró nada, relanza un par de intentos suaves
        if not modelos:
            click_tab_todos_generico(page)
            ok = wait_grid_with_scroll(page, max_tries=10)
            modelos = extraer_modelos(page) if ok else []

        for m in modelos:
            try:
                page.goto(m["url_modelo"], wait_until="domcontentloaded", timeout=45000)
                scroll_suave(page)
                # Algunas páginas de modelo también tienen banners
                close_cookies_if_any(page)
                vers = extraer_versiones(page, m["modelo"], nombre_marca)
                all_versions.extend(vers)
            except Exception:
                continue

        ctx.close()
        browser.close()

    path = os.path.join("salida_modelos", f"{nombre_marca}_versiones.json")
    for a in all_versions:
        
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [a['precio_credito_inteligente_int'],a['precio_credito_convencional_int'],a['precio_todo_medio_pago_int'],a['precio_lista_int']]
        datos = {
            'modelo': a['modelo'],
            'marca': a['marca'],
            'modelDetail': a['version'],
            'tiposprecio': tiposprecio,
            'precio':precio
            
        }
        saveCar(a['marca'],datos,'www.difor.cl')
        print(datos)
        print("-"*100)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_versions, f, ensure_ascii=False, indent=2)

    print(f"[OK] JSON plano creado: {path} ({len(all_versions)} versiones)")
    return path

# ============ CLI ============

if __name__ == "__main__":
    for m in marcas_difor:
        URL_BRAND = m['url']  # <-- ajusta aquí
        NOMBRE_MARCA = m['brand']
        scrape_brand_flat_json(URL_BRAND, NOMBRE_MARCA, headless=False)
