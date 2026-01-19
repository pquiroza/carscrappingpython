# astararetail_multi_brands_formato.py
import os
import re
import json
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
from urllib.parse import urljoin
from utils import saveCar
from playwright.sync_api import sync_playwright, Page

# ==========================
# CONFIG: agrega aquí marcas
# ==========================
BRANDS = [
    {
        "brand": "SsangYong",
        "list_url": "https://astararetail.cl/ssangyong/",
        "base": "https://astararetail.cl",
        # "gallery_selector": "#eael-filter-gallery-wrapper-f2f0bbc",  # opcional
    },
   
    {
        "brand": "JMC",
         "list_url": "https://astararetail.cl/jmc/",
         "base": "https://astararetail.cl",
     },
    {
       "brand": "GAC",
         "list_url": "https://astararetail.cl/gac/",
         "base": "https://astararetail.cl", 
        
        
    },
    {
       "brand": "BYD",
         "list_url": "https://astararetail.cl/byd/",
         "base": "https://astararetail.cl", 
        
        
    }
]

# ==========================
# Utilidades
# ==========================
MONEY_RX = re.compile(r"\$?\s?\d{1,3}(?:\.\d{3})+")

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def money_to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = MONEY_RX.search(text)
    if not m:
        return None
    return int(re.sub(r"[^\d]", "", m.group(0)) or "0")

def try_dismiss_overlays(page: Page):
    for sel in [
        "button:has-text('Aceptar')",
        "button:has-text('Acepto')",
        "button:has-text('Entendido')",
        "button[aria-label='Cerrar']",
        "[role='dialog'] button:has-text('Cerrar')",
        "div[role='dialog'] button:has-text('OK')",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible():
                btn.click()
                page.wait_for_timeout(150)
        except Exception:
            pass

# ==========================
# Paso 1: obtener URLs de modelos en el grid
# ==========================
def detect_gallery_selector(page: Page) -> Optional[str]:
    """
    Si no se entrega un selector fijo, intenta detectar el primer wrapper de
    Essential Addons Filterable Gallery presente en la página.
    """
    # 1) ID específico usado por EA (comienza con eael-filter-gallery-wrapper-)
    node = page.locator("[id^='eael-filter-gallery-wrapper-']").first
    if node.count() > 0:
        idv = node.get_attribute("id")
        return f"#{idv}"
    # 2) Fallback: clase genérica del wrapper
    node = page.locator(".eael-filter-gallery-wrapper").first
    if node.count() > 0:
        # Retorna la clase genérica; la usaremos para buscar ítems dentro
        return ".eael-filter-gallery-wrapper"
    return None

@dataclass
class ModelLink:
    brand: str
    modelo: str
    url_modelo: str
    img: Optional[str] = None

def wait_gallery_ready(page: Page, gallery_selector: str, timeout_ms: int = 15000):
    wrapper = page.locator(gallery_selector).first
    wrapper.wait_for(state="attached", timeout=timeout_ms)
    # los ítems son .eael-filterable-gallery-item-wrap
    wrapper.locator(".eael-filterable-gallery-item-wrap").first.wait_for(state="attached", timeout=timeout_ms)

def collect_model_links(page: Page, brand_name: str, base: str, gallery_selector: Optional[str]) -> List[ModelLink]:
    if not gallery_selector:
        gallery_selector = detect_gallery_selector(page)
        if not gallery_selector:
            return []
    wait_gallery_ready(page, gallery_selector)

    links: List[ModelLink] = []
    cards = page.locator(f"{gallery_selector} .eael-filterable-gallery-item-wrap")
    for i in range(cards.count()):
        wrap = cards.nth(i)
        a = wrap.locator(".eael-gallery-grid-item a").first
        if a.count() == 0:
            continue
        href = a.get_attribute("href") or ""
        url_abs = urljoin(base, href)
        title_node = a.locator(".fg-item-title").first
        modelo = norm(title_node.inner_text()) if title_node.count() > 0 else norm(a.inner_text())
        img_node = a.locator("img").first
        img = None
        if img_node.count() > 0:
            img = img_node.get_attribute("src") or img_node.get_attribute("data-lazy-src")
            if img:
                img = urljoin(base, img)
        links.append(ModelLink(brand=brand_name, modelo=modelo, url_modelo=url_abs, img=img))
    return links

# ==========================
# Paso 2: extraer versiones/precios dentro de cada modelo
# ==========================
def wait_versions_block(page: Page, timeout_ms: int = 15000):
    # H2 “Elige una versión” o directamente el contenedor
    try:
        page.wait_for_selector("h2.elementor-heading-title:has-text('Elige una versión')", timeout=timeout_ms)
    except Exception:
        pass
    page.wait_for_selector("#ag-list-comparer", timeout=timeout_ms)

def parse_version_card(card) -> Dict:
    # Modelo y versión
    ver_box = card.locator(".version").first
    modelo_full = None
    version_name = None
    if ver_box.count() > 0:
        ps = ver_box.locator("p")
        if ps.count() >= 1:
            modelo_full = norm(ps.nth(0).inner_text())
        if ps.count() >= 2:
            version_name = norm(ps.nth(1).inner_text())

    # Precio lista
    precio_lista = None
    bonus_boxes = card.locator(".bonus_price")
    if bonus_boxes.count() > 0:
        bp = bonus_boxes.nth(0)
        txt = norm(bp.inner_text())
        m = MONEY_RX.search(txt)
        if m:
            precio_lista = money_to_int(m.group(0))

    # Precios “todo medio de pago” y “con financiamiento”
    precio_todo_medio = None
    precio_con_financ = None
    price_boxes = card.locator(".price")
    for i in range(price_boxes.count()):
        pb = price_boxes.nth(i)
        label = norm(pb.locator("p").nth(0).inner_text()) if pb.locator("p").count() > 0 else ""
        amount_node = pb.locator(".h1").first
        amount_txt = norm(amount_node.inner_text()) if amount_node.count() > 0 else ""
        amount = money_to_int(amount_txt)
        if "todo medio" in label.lower():
            precio_todo_medio = amount
        elif "financiam" in label.lower():
            precio_con_financ = amount

    # Bonos (segundo .bonus_price)
    bono_todo_medio = None
    bono_financ = None
    if bonus_boxes.count() >= 2:
        bp2 = bonus_boxes.nth(1)
        ps = bp2.locator("p")
        for i in range(ps.count() - 1):
            label = norm(ps.nth(i).inner_text()).lower()
            value = money_to_int(ps.nth(i + 1).inner_text())
            if "bono todo medio" in label:
                bono_todo_medio = value
            if "bono financ" in label:
                bono_financ = value

    # Precio “card”: usamos “todo medio de pago”
    precio_card = precio_todo_medio

    # Bono total
    bono_total = (bono_todo_medio or 0) + (bono_financ or 0) if (bono_todo_medio or bono_financ) is not None else None

    return {
        "modelo_full": modelo_full,
        "version_name": version_name,
        "precio_lista_int": precio_lista,
        "precio_todo_medio_pago_int": precio_todo_medio,
        "precio_credito_inteligente_int": precio_con_financ,  # "Precio con financiamiento"
        "precio_card_int": precio_card,
        "bono_total_int": bono_total,
    }

def extract_versions_from_model(page: Page, url_modelo: str, brand_name: str, modelo_label_fallback: Optional[str]) -> List[Dict]:
    wait_versions_block(page)
    cards = page.locator("#ag-list-comparer .ag-comparer-model-wrapper")
    out: List[Dict] = []
    for i in range(cards.count()):
        card = cards.nth(i)
        data = parse_version_card(card)

        modelo_full = data.get("modelo_full") or modelo_label_fallback or ""
        # separar marca + modelo (primera palabra ~marca)
        parts = modelo_full.split()
        if len(parts) >= 2:
            marca = parts[0]
            modelo = " ".join(parts[1:])
        else:
            marca = brand_name
            modelo = modelo_full or modelo_label_fallback

        # normaliza marca SsangYong
        if "ssang" in marca.lower():
            marca = "SsangYong"

        row = {
            "marca": marca or brand_name,
            "modelo": modelo,
            "version": data.get("version_name"),
            "precio_card_int": data.get("precio_card_int"),
            "bono_int": data.get("bono_total_int"),
            "precio_credito_inteligente_int": data.get("precio_credito_inteligente_int"),
            "precio_credito_convencional_int": None,  # no aparece en este bloque
            "precio_todo_medio_pago_int": data.get("precio_todo_medio_pago_int"),
            "precio_lista_int": data.get("precio_lista_int"),
            "cc": None,
            "combustible": None,
            "transmision": None,
            "potencia_hp": None,
            "url_modelo": url_modelo,
            "url_version": url_modelo,
        }
        out.append(row)
    return out

# ==========================
# Main
# ==========================
def main(headless: bool = True):
    output = "astararetail_all_formato.json"
    results: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            locale="es-CL",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")
        )
        page = context.new_page()
        page.set_default_timeout(25000)
        page.set_default_navigation_timeout(25000)

        for b in BRANDS:
            brand_name = b["brand"]
            list_url = b["list_url"]
            base = b.get("base") or list_url
            gallery_selector = b.get("gallery_selector")  # opcional

            print(f"\n=== {brand_name} ===")
            page.goto(list_url, wait_until="domcontentloaded")
            page.wait_for_timeout(600)
            try_dismiss_overlays(page)

            # 1) modelos
            model_links = collect_model_links(page, brand_name, base, gallery_selector)
            print(f"[INFO] Modelos en {brand_name}: {len(model_links)}")

            # 2) versiones por modelo
            for idx, m in enumerate(model_links, 1):
                try:
                    page.goto(m.url_modelo, wait_until="domcontentloaded")
                    page.wait_for_timeout(600)
                    try_dismiss_overlays(page)

                    rows = extract_versions_from_model(page, m.url_modelo, brand_name, m.modelo)
                    results.extend(rows)
                    print(f"  [{idx}/{len(model_links)}] {m.modelo}: {len(rows)} versiones")
                except Exception as e:
                    print(f"[WARN] Error en {m.url_modelo}: {e}")

        context.close()
        browser.close()

    with open(output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    for r in results:
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [r['precio_credito_inteligente_int'],r['precio_todo_medio_pago_int'],r['precio_todo_medio_pago_int'],r['precio_lista_int']]
        datos = {
            'marca': r['marca'],
            'modelo': r['modelo'],
            'modelDetail': r['version'],
            'precio': precio,
            'tiposprecio':tiposprecio
        }
        print(datos)
        print("-"*50)
        saveCar(r['marca'],datos,"astararetail.cl")
    print(f"\n[OK] {output} → {len(results)} versiones totales")

if __name__ == "__main__":
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    main(headless=headless)
