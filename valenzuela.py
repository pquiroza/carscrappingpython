# valenzuela_honda_versiones.py
import os, re, json, time
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout
from utils import saveCar
BASE = "https://www.valenzueladelarze.cl"
START = f"{BASE}/honda/"

MONEY_RX = re.compile(r"\$?\s?\d{1,3}(?:\.\d{3})+", re.IGNORECASE)

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def money_to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = MONEY_RX.search(text)
    if not m:
        return None
    val = int(re.sub(r"[^\d]", "", m.group(0)) or "0")
    return val if val > 0 else None

def clean_model(title: str) -> str:
    t = norm(title)
    t = re.sub(r"^NEW\s+HONDA\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^HONDA\s+", "", t, flags=re.IGNORECASE)
    t = t.title()
    t = (t
         .replace("Cr-V", "CR-V")
         .replace("Zr-V", "ZR-V")
         .replace("Hr-V", "HR-V"))
    return t

@dataclass
class Card:
    href: str
    title: str

# --------------------------
# Utilidades básicas
# --------------------------
def try_dismiss_overlays(page: Page):
    for sel in [
        "button:has-text('Aceptar')", "button:has-text('Acepto')",
        "button:has-text('Entendido')", "button[aria-label='Cerrar']",
        ".cky-btn-accept", "#onetrust-accept-btn-handler"
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() and btn.is_visible():
                btn.click()
                page.wait_for_timeout(200)
        except Exception:
            pass

def wait_listing(page: Page):
    page.wait_for_load_state("domcontentloaded")
    try_dismiss_overlays(page)
    page.wait_for_selector(".et_pb_blog_grid", timeout=15000, state="attached")
    page.wait_for_selector(".et_pb_salvattore_content", timeout=15000, state="attached")
    page.wait_for_selector(".et_pb_salvattore_content article.et_pb_post", timeout=15000, state="attached")

def auto_scroll_until_stable(page: Page, step=1200, idle_rounds=4, hard_cap=40):
    last_h = -1
    last_count = -1
    stagnant = 0
    for _ in range(hard_cap):
        page.evaluate(f"window.scrollBy(0, {step});")
        page.wait_for_timeout(250)
        h = page.evaluate("document.body.scrollHeight")
        try:
            count = page.locator(".et_pb_salvattore_content article.et_pb_post").count()
        except Exception:
            count = 0
        if h == last_h and count == last_count:
            stagnant += 1
            if stagnant >= idle_rounds:
                break
        else:
            stagnant = 0
            last_h = h
            last_count = count
    try:
        page.mouse.wheel(0, -2000)
        page.wait_for_timeout(200)
    except Exception:
        pass

def collect_model_links(page: Page) -> List[Card]:
    wait_listing(page)
    auto_scroll_until_stable(page)
    cards: List[Card] = []
    articles = page.locator(".et_pb_salvattore_content article.et_pb_post")
    n = articles.count()
    if not n:
        articles = page.locator("article.et_pb_post")
        n = articles.count()
    for i in range(n):
        art = articles.nth(i)
        a = art.locator("h2.entry-title a").first
        if not a.count():
            a = art.locator(".et_pb_image_container a").first
        href = a.get_attribute("href") or ""
        title = norm(a.inner_text() or art.locator("h2.entry-title").first.inner_text())
        if href:
            cards.append(Card(href=urljoin(BASE, href), title=title))
    seen, out = set(), []
    for c in cards:
        if c.href in seen:
            continue
        seen.add(c.href)
        out.append(c)
    return out

# --------------------------
# Detalle de versiones
# --------------------------
def wait_detail(page: Page, timeout_ms=20000):
    page.wait_for_load_state("domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=6000)
    except Exception:
        pass
    try_dismiss_overlays(page)
    try:
        page.wait_for_selector("#ajax_get_car_prices_call_sample_id", timeout=timeout_ms, state="attached")
    except PWTimeout:
        pass
    page.wait_for_timeout(500)

def read_versions_from_detail(page: Page, model_title_fallback: str) -> List[Dict]:
    wait_detail(page)
    marca = "Honda"
    modelo = None
    for sel in ["h1.entry-title", "h1.et_pb_module_header", "h2.entry-title", "h1", "h2"]:
        el = page.locator(sel).first
        if el.count():
            modelo = clean_model(el.inner_text())
            break
    if not modelo:
        modelo = clean_model(model_title_fallback)

    versions: List[Dict] = []
    boxes = page.locator("#ajax_get_car_prices_call_sample_id .box-version")
    n = boxes.count()
    if n == 0:
        return []  # No hay versiones, salir limpio

    for i in range(n):
        b = boxes.nth(i)
        try:
            version = norm(b.locator(".get-car-prices-modelo").first.inner_text())
            precio_lista = money_to_int(b.locator(".get-car-prices-precio").first.inner_text())
            precio_all = money_to_int(b.locator(".get-car-prices-precio-all").first.inner_text())
            precio_conv = money_to_int(b.locator(".get-car-prices-precio-convencional").first.inner_text())
            precio_int = money_to_int(b.locator(".get-car-prices-precio-inteligente").first.inner_text())

            # Validación final: si no hay precios numéricos, saltar
            if not any([precio_lista, precio_all, precio_conv, precio_int]):
                continue

            row = {
                "marca": marca,
                "modelo": modelo,
                "version": version or None,
                "precio_lista_int": precio_lista,
                "precio_todo_medio_pago_int": precio_all,
                "precio_credito_convencional_int": precio_conv,
                "precio_credito_inteligente_int": precio_int,
                "precio_card_int": None,
                "bono_int": None,
                "cc": None,
                "combustible": None,
                "transmision": None,
                "potencia_hp": None,
                "url_modelo": page.url,
                "url_version": page.url,
            }
            versions.append(row)
        except Exception:
            continue

    # Solo retornar si hay versiones válidas
    valid = [v for v in versions if any([v["precio_lista_int"], v["precio_todo_medio_pago_int"],
                                         v["precio_credito_convencional_int"], v["precio_credito_inteligente_int"]])]
    return valid

# --------------------------
# Main
# --------------------------
def main(headless: bool = True):
    out_json = "valenzuela_honda_versiones.json"
    results: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(locale="es-CL")
        page = context.new_page()
        page.set_default_timeout(22000)
        page.goto(START, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=6000)
        except Exception:
            pass

        cards = collect_model_links(page)
        print(f"[INFO] modelos encontrados: {len(cards)}")

        for idx, c in enumerate(cards, 1):
            try:
                print(f"[{idx}/{len(cards)}] {c.title} → {c.href}")
                page.goto(c.href, wait_until="domcontentloaded")
                page.wait_for_timeout(500)
                vers = read_versions_from_detail(page, model_title_fallback=c.title)
                if not vers:
                    print(f"[WARN] sin versiones o precios válidos en {c.href}")
                    continue
                results.extend(vers)
                print(f"[OK] {len(vers)} versiones válidas para {c.title}")
            except Exception as e:
                print(f"[ERR] {c.href}: {e}")
                continue

        context.close()
        browser.close()

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    for r in results:
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [r['precio_credito_inteligente_int'],r['precio_credito_convencional_int'],r['precio_todo_medio_pago_int'],r['precio_lista_int']]
        datos = {
            'marca': r['marca'],
            'modelo': r['modelo'],
            'modelDetail': r['version'],
            'precio': precio,
            'tiposprecio':tiposprecio
        }
        saveCar("Honda",datos,"www.valenzueladelarze.cl")
    print(f"\n[OK] {out_json} → {len(results)} versiones válidas exportadas")

if __name__ == "__main__":
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    main(headless=headless)
