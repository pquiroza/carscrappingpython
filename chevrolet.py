# coseche_chevrolet_formato.py
import os, re, json, time
from dataclasses import dataclass
from typing import List, Optional, Dict
from urllib.parse import urljoin, urlencode
from utils import to_title_custom
from utils import saveCar
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

BASE = "https://www.coseche.com"
START = f"{BASE}/marcas/chevrolet/nuevo"

MONEY_RX = re.compile(r"\$?\s?\d{1,3}(?:\.\d{3})+", re.IGNORECASE)

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def money_to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = MONEY_RX.search(text)
    if not m:
        return None
    return int(re.sub(r"[^\d]", "", m.group(0)) or "0")

@dataclass
class Card:
    href: str
    title: str

# ----------------------------
# Utilidades de scroll/overlays
# ----------------------------
def auto_scroll(page: Page, max_idle_ms=800, step_px=1200, hard_cap=30):
    last_h, idle = 0, 0
    for _ in range(hard_cap):
        page.evaluate(f"window.scrollBy(0, {step_px});")
        time.sleep(0.35)
        new_h = page.evaluate("document.body.scrollHeight")
        if new_h == last_h:
            idle += 350
            if idle >= max_idle_ms:
                break
        else:
            idle = 0
            last_h = new_h

def try_dismiss_overlays(page: Page):
    for sel in [
        "button:has-text('Aceptar')",
        "button:has-text('Acepto')",
        "button:has-text('Entendido')",
        "button[aria-label='Cerrar']",
        "[role='dialog'] button:has-text('Cerrar')",
        "div[role='dialog'] button:has-text('OK')",
        "button.cookie",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible():
                btn.click()
                page.wait_for_timeout(200)
        except Exception:
            pass



def ensure_detail_ready(page: Page, timeout_ms: int = 20000):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms//3)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms//3)
    except Exception:
        pass

    try_dismiss_overlays(page)

    any_section = page.locator("#price-section, #details-section").first
    any_section.wait_for(state="attached", timeout=timeout_ms//2)

    try:
        any_section.scroll_into_view_if_needed()
    except Exception:
        pass

    try:
        for _ in range(4):
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(150)
        page.mouse.wheel(0, -1600)
        page.wait_for_timeout(150)
    except Exception:
        pass

    # ---- corrección del regex: usar r"""...""" para evitar el warning ----
    try:
        page.wait_for_function(
            r"""
            () => {
              const root = document.querySelector('#price-section') || document;
              if (!root) return false;
              // ¿Hay un texto con $?
              const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
              let n, txt = '';
              while ((n = walker.nextNode())) txt += n.textContent || '';
              if (/\$\s?\d/.test(txt)) return true;
              if (document.querySelector("a[href^='/reservation-payment']")) return true;
              if (/whatsapp/i.test(document.body.innerText || '')) return true;
              return false;
            }
            """,
            timeout=timeout_ms//2
        )
    except Exception:
        pass

# ----------------------------
# Navegación/colección de cards
# ----------------------------
def go_to_category(page: Page, category: Optional[str]):
    url = START if not category else f"{START}?{urlencode({'categoria': category})}"
    page.goto(url, wait_until="domcontentloaded")
    try:
        page.wait_for_selector("#hits ul", timeout=9000)
    except PWTimeout:
        page.wait_for_selector("#hits", timeout=12000)

def collect_cards(page: Page, category: Optional[str]) -> List[Card]:
    page.wait_for_selector("#hits", timeout=15000)
    auto_scroll(page)
    items: List[Card] = []

    arts = page.locator("#hits ul li article")
    for i in range(arts.count()):
        art = arts.nth(i)
        a = art.locator("a[href*='/marcas/chevrolet/nuevo/']").first
        if a.count() == 0:
            continue
        href = urljoin(BASE, a.get_attribute("href") or "")
        title = ""
        for sel in ["h2", "h3", "span.text-xl", "span.font-bold"]:
            loc = art.locator(sel).first
            if loc.count() > 0:
                title = norm(loc.inner_text())
                if title:
                    break
        if not title:
            title = norm(a.inner_text())[:120]
        items.append(Card(href=href, title=title))
    return items

# ----------------------------
# Lectura de detalle/variantes
# ----------------------------
def click_swiper(page: Page):
    try:
        next_btn = page.locator("button[aria-label='Siguiente']").first
        prev_btn = page.locator("button[aria-label='Anterior']").first
        for _ in range(7):
            if next_btn.count() > 0 and next_btn.is_visible():
                next_btn.click()
                page.wait_for_timeout(150)
        for _ in range(3):
            if prev_btn.count() > 0 and prev_btn.is_visible():
                prev_btn.click()
                page.wait_for_timeout(120)
    except Exception:
        pass

def read_variant_card(a) -> Dict:
    out = {
        "variant_href": None,
        "variant_name": None,
        "variant_price_raw": None,
        "variant_price_int": None,
        "fuel": None,
        "transmission": None,
    }
    href = a.get_attribute("href") or ""
    out["variant_href"] = urljoin(BASE, href)

    h3 = a.locator("h3").first
    if h3.count() > 0:
        out["variant_name"] = norm(h3.inner_text())

    price_block = a.locator("[aria-label='Información de precio'], section:has-text('Desde'), div:has-text('Desde')").first
    if price_block.count() > 0:
        p = price_block.locator("p").first
        if p.count() > 0:
            ptxt = norm(p.inner_text())
            out["variant_price_raw"] = ptxt
            out["variant_price_int"] = money_to_int(ptxt)
        else:
            txt = norm(price_block.inner_text())
            out["variant_price_raw"] = txt
            out["variant_price_int"] = money_to_int(txt)

    specs = a.locator("section.bg-CO-quaternary-light article")
    for i in range(min(6, specs.count())):
        t = norm(specs.nth(i).inner_text()).upper()
        if any(k in t for k in ["MANUAL", "MECANICA", "AUTOMATICA", "CVT", "AT", "MT"]):
            m = re.search(r"(MANUAL|MECANICA|AUTOMATICA|CVT|AT|MT)", t)
            if m:
                v = m.group(1)
                out["transmission"] = "Automática" if v.startswith("AUTO") or v in ("CVT","AT") else "Manual"
        if any(k in t for k in ["GASOLINA", "DIÉSEL", "DIESEL", "HÍBRIDO", "HIBRIDO", "ELÉCTRICO", "ELECTRICO"]):
            m = re.search(r"(GASOLINA|DIÉSEL|DIESEL|HÍBRIDO|HIBRIDO|ELÉCTRICO|ELECTRICO)", t)
            if m:
                fuel = m.group(1).replace("É","E").replace("Í","I")
                out["fuel"] = "Diésel" if fuel.upper().startswith("DIE") or fuel.upper().startswith("DÍE") else \
                              "Híbrido" if fuel.upper().startswith("HIB") else \
                              "Eléctrico" if fuel.upper().startswith("ELE") else "Gasolina"
    return out

def extract_detail_core(page: Page) -> Dict:
    """Lee marca, modelo, price-section y opciones de pago (bloques del detalle)."""
    ensure_detail_ready(page, timeout_ms=20000)

    brand = None
    model = None
    ref_text = None

    brand_node = page.locator("#details-section p.text-lg.font-bold").first
    if brand_node.count() == 0:
        brand_node = page.locator("p:text-matches('CHEVROLET', 'i')").first
    if brand_node.count() > 0:
        brand = norm(brand_node.inner_text())

    model_node = page.locator("#details-section p.text-2xl.font-bold").first
    if model_node.count() == 0:
        model_node = page.locator("#details-section h1, #details-section h2").first
    if model_node.count() > 0:
        model = norm(model_node.inner_text())

    ref_node = page.locator("#details-section .text-xs.text-gray-600 span:text-matches('^REF', 'i')").first
    if ref_node.count() == 0:
        ref_node = page.locator("span:text-matches('^REF\\s*:', 'i')").first
    if ref_node.count() > 0:
        ref_text = norm(ref_node.inner_text())
        ref_text = re.sub(r"^REF\\s*:?\s*", "", ref_text, flags=re.IGNORECASE)

    # ---------- PRECIO "Desde" (evitando clases con corchetes) ----------
    # 1) Buscar cualquier texto tipo $9.999.999 dentro de price-section
    price_box = page.locator("#price-section :text-matches('^\\$\\s?\\d', 'i')").first

    # 2) Fallback: cualquier elemento con clase font-bold que tenga $
    if price_box.count() == 0:
        price_box = page.locator("#price-section .font-bold:has-text('$')").first

    # 3) Último recurso: selector con clase escapada (Tailwind)
    if price_box.count() == 0:
        price_box = page.locator("#price-section .text-\\[25px\\].font-bold").first

    price_desde_raw = norm(price_box.inner_text()) if price_box.count() > 0 else None
    price_desde_int = money_to_int(price_desde_raw) if price_desde_raw else None

    # IVA
    iva_node = page.locator("#price-section span:text-matches('IVA', 'i')").first
    iva_text = norm(iva_node.inner_text()) if iva_node.count() > 0 else None

    # Opciones de pago
    pago = {"inteligente": None, "convencional": None, "todo_medio": None}
    payment_root = page.locator("#price-section .payment-options ul").first
    if payment_root.count() == 0:
        payment_root = page.locator(".payment-options ul").first
    if payment_root.count() > 0:
        lis = payment_root.locator("li")
        for i in range(lis.count()):
            li = lis.nth(i)
            label = norm(li.locator(".label").first.inner_text()) if li.locator(".label").count() > 0 else ""
            price = norm(li.locator(".price").first.inner_text()) if li.locator(".price").count() > 0 else ""
            price_int = money_to_int(price)
            labu = label.lower()
            if "inteligente" in labu:
                pago["inteligente"] = price_int
            elif "convencional" in labu:
                pago["convencional"] = price_int
            elif "todo medio" in labu:
                pago["todo_medio"] = price_int

    return {
        "brand": brand,
        "model": model,
        "ref": ref_text,
        "price_desde_raw": price_desde_raw,
        "price_desde_int": price_desde_int,
        "iva_text": iva_text,
        "pago": pago,
    }

def extract_variants(page: Page) -> List[Dict]:
    variants: List[Dict] = []
    slider = page.locator(".or-swiper-slider-wrapper .swiper, .swiper")
    if slider.count() == 0:
        return variants
    click_swiper(page)
    anchors = slider.locator(".swiper-slide a[href]")
    seen = set()
    for i in range(anchors.count()):
        a = anchors.nth(i)
        try:
            data = read_variant_card(a)
            if data["variant_href"] in seen:
                continue
            seen.add(data["variant_href"])
            variants.append(data)
        except Exception:
            continue
    return variants

# ----------------------------
# Main
# ----------------------------
def main(headless: bool = False):
    out_json = "coseche_chevrolet_formato.json"
    categories = [None, "camioneta", "comercial", "sedan", "suv"]

    results: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            locale="es-CL",
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")
        )
        page = context.new_page()
        page.set_default_timeout(25000)
        page.set_default_navigation_timeout(25000)

        # 1) tarjetas
        all_cards: List[Card] = []
        for cat in categories:
            go_to_category(page, cat)
            all_cards.extend(collect_cards(page, cat))
        # de-dup
        seen = set()
        cards = []
        for c in all_cards:
            if c.href not in seen:
                seen.add(c.href)
                cards.append(c)

        # 2) detalle → variantes → mapeo al formato pedido
        total = len(cards)
        for idx, c in enumerate(cards, 1):
            ok = False
            for attempt in range(1, 3):  # hasta 2 intentos
                try:
                    page.goto(c.href, wait_until="domcontentloaded")
                    page.wait_for_timeout(600)

                    core = extract_detail_core(page)  # ya no usa selectores inválidos
                    variants = extract_variants(page)

                    if not variants:
                        row = {
                            "marca": core["brand"],
                            "modelo": core["model"],
                            "version": core["ref"] or c.title,
                            "precio_card_int": core["price_desde_int"],
                            "bono_int": None,
                            "precio_credito_inteligente_int": core["pago"]["inteligente"],
                            "precio_credito_convencional_int": core["pago"]["convencional"],
                            "precio_todo_medio_pago_int": core["pago"]["todo_medio"],
                            "precio_lista_int": None,
                            "cc": None,
                            "combustible": None,
                            "transmision": None,
                            "potencia_hp": None,
                            "url_modelo": c.href,
                            "url_version": c.href,
                        }
                        results.append(row)
                    else:
                        for v in variants:
                            price_card_int = v["variant_price_int"] or core["price_desde_int"]
                            row = {
                                "marca": core["brand"],
                                "modelo": core["model"],
                                "version": v["variant_name"] or core["ref"] or c.title,
                                "precio_card_int": price_card_int,
                                "bono_int": None,
                                "precio_credito_inteligente_int": core["pago"]["inteligente"],
                                "precio_credito_convencional_int": core["pago"]["convencional"],
                                "precio_todo_medio_pago_int": core["pago"]["todo_medio"],
                                "precio_lista_int": None,
                                "cc": None,
                                "combustible": v["fuel"],
                                "transmision": v["transmission"],
                                "potencia_hp": None,
                                "url_modelo": c.href,
                                "url_version": v["variant_href"] or c.href,
                            }
                            print(row)
                            print("-"*50)
                            results.append(row)

                    print(f"[{idx}/{total}] OK {core['model'] or c.title}: {len(variants) or 1} registro(s)")
                    ok = True
                    break
                except Exception as e:
                    print(f"[WARN] intento {attempt} en {c.href}: {e}")
                    try_dismiss_overlays(page)
                    try:
                        page.reload(wait_until="domcontentloaded")
                    except Exception:
                        pass
                    page.wait_for_timeout(700)

            if not ok:
                print(f"[ERR] No se pudo extraer {c.href}")
                continue

        context.close()
        browser.close()

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    for r in results:
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [r['precio_credito_inteligente_int'],r['precio_credito_convencional_int'],r['precio_todo_medio_pago_int'],r['precio_todo_medio_pago_int']]
        datos = {
            'modelo': to_title_custom(r['modelo']),
            'marca': to_title_custom(r['marca']),
            'modelDetail': to_title_custom(r['version']),
            'tiposprecio':tiposprecio,
            'precio':precio,
            "combustible": "Gasolina",
            "transmision": "Manual",
        }
        print(datos)
        saveCar("Chevrolet",datos,"www.coseche.cl")
        print("-"*100)
    print(f"[OK] {out_json} → {len(results)} elementos")

if __name__ == "__main__":
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    main(headless=headless)
