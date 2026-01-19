# valenzuela_honda_versiones.py
import os, re, json
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
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

def fix_model_casing(name: str) -> str:
    t = name.strip()
    # Normalizaciones específicas Honda
    repl = {
        "Cr-V": "CR-V",
        "Zr-V": "ZR-V",
        "Hr-V": "HR-V",
        "Crv": "CR-V",
        "Zrv": "ZR-V",
        "Hrv": "HR-V",
        "Pilot": "Pilot",
        "Civic": "Civic",
    }
    # Title() desarma siglas; por eso mapear después
    t = t.title()
    for k, v in repl.items():
        t = t.replace(k, v)
    return t

def model_from_version_text(version_text: str) -> Optional[str]:
    """
    Intenta extraer el modelo desde la versión.
    Ej: "ZR-V EXL 2.0 Aut. 4X2" -> "ZR-V"
        "CR-V Advance Hybrid" -> "CR-V"
        "Civic EXL 2.0 CVT" -> "Civic"
    """
    if not version_text:
        return None
    txt = norm(version_text)

    # Cortar antes del primer dígito (cilindrada/año/etc.)
    txt = re.split(r"\s+\d", txt, maxsplit=1)[0]

    # Quitar acabados comunes si están inmediatamente después del modelo
    # (por si la versión es "CR-V Advance Hybrid", quedarnos con el primer token compuesto CR-V)
    # Regla: modelo son 1-2 tokens iniciales que no contienen dígitos
    tokens = [t for t in re.split(r"\s+", txt) if t and not re.search(r"\d", t)]
    if not tokens:
        return None

    # Si el primer token tiene guion, puede ser "CR-V", "ZR-V", lo respetamos
    # Si el primer token es "New" o "All-New", ignorarlo.
    IGNORE = {"NEW", "ALL-NEW", "ALL", "NUEVO", "NUEVA"}
    tokens_up = [t for t in tokens if t.upper() not in IGNORE]
    if not tokens_up:
        return None

    # Para Honda, casi siempre el primer token ya es el modelo (CR-V, ZR-V, HR-V, Civic, Pilot)
    model = tokens_up[0]

    return fix_model_casing(model)

def model_from_heading(page: Page, fallback_title: str) -> Optional[str]:
    # Intenta tomar H1/H2
    head = None
    for sel in ["h1.entry-title", "h1.et_pb_module_header", "h2.entry-title", "h1", "h2"]:
        el = page.locator(sel).first
        if el.count():
            head = norm(el.inner_text())
            break
    if not head:
        head = norm(fallback_title)

    # Quitar prefijos
    head = re.sub(r"^NEW\s+HONDA\s+", "", head, flags=re.IGNORECASE)
    head = re.sub(r"^HONDA\s+", "", head, flags=re.IGNORECASE)

    # Si el heading incluye algo como "CR-V Advance Hybrid", quedarnos con el primer token relevante
    m = model_from_version_text(head)
    return m or fix_model_casing(head)

def model_from_url(url: str) -> Optional[str]:
    try:
        path = urlparse(url).path.strip("/").split("/")[-1]
        # Ej: "honda-cr-v", "new-honda-pilot", "honda-zr-v"
        slug = path.lower()
        slug = slug.replace("new-honda-", "").replace("honda-", "")
        slug = slug.replace("-", " ").strip()
        if not slug:
            return None
        # Primer token suele ser el modelo completo (con guiones convertido a espacio)
        # para casos compuestos (cr v) reconstruimos:
        if slug in {"cr v", "crv"}:
            return "CR-V"
        if slug in {"zr v", "zrv"}:
            return "ZR-V"
        if slug in {"hr v", "hrv"}:
            return "HR-V"
        return fix_model_casing(slug.split()[0])
    except Exception:
        return None

@dataclass
class Card:
    href: str
    title: str

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
    # de-dup
    seen, out = set(), []
    for c in cards:
        if c.href in seen:
            continue
        seen.add(c.href)
        out.append(c)
    return out

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
    page.wait_for_timeout(400)

def read_versions_from_detail(page: Page, model_title_fallback: str) -> List[Dict]:
    wait_detail(page)

    # Heading de la página (para fallback)
    model_from_head = model_from_heading(page, model_title_fallback) or ""

    # Contenedor de versiones
    boxes = page.locator("#ajax_get_car_prices_call_sample_id .box-version")
    n = boxes.count()
    if n == 0:
        return []  # No hay versiones

    results: List[Dict] = []
    for i in range(n):
        b = boxes.nth(i)
        try:
            ver_txt = norm(b.locator(".get-car-prices-modelo").first.inner_text())
            # Modelo por versión (PRIORIDAD 1)
            modelo = model_from_version_text(ver_txt)
            if not modelo:
                # PRIORIDAD 2: por heading
                modelo = model_from_head or None
            if not modelo:
                # PRIORIDAD 3: por URL
                modelo = model_from_url(page.url)

            # Precios
            precio_lista = money_to_int(b.locator(".get-car-prices-precio").first.inner_text())
            precio_all   = money_to_int(b.locator(".get-car-prices-precio-all").first.inner_text())
            precio_conv  = money_to_int(b.locator(".get-car-prices-precio-convencional").first.inner_text())
            precio_int   = money_to_int(b.locator(".get-car-prices-precio-inteligente").first.inner_text())

            # Validación: debe haber al menos un precio
            if not any([precio_lista, precio_all, precio_conv, precio_int]):
                continue

            row = {
                "marca": "Honda",
                "modelo": modelo,
                "version": ver_txt or None,
                "precio_lista_int": precio_lista,
                "precio_todo_medio_pago_int": precio_all,
                "precio_credito_convencional_int": precio_conv,
                "precio_credito_inteligente_int": precio_int,
                # Campos no presentes aquí:
                "precio_card_int": None,
                "bono_int": None,
                "cc": None,
                "combustible": None,
                "transmision": None,
                "potencia_hp": None,
                "url_modelo": page.url,
                "url_version": page.url,
            }
            results.append(row)
        except Exception:
            continue

    # Sólo devolver versiones válidas
    valid = [v for v in results if v["modelo"] and any([
        v["precio_lista_int"], v["precio_todo_medio_pago_int"],
        v["precio_credito_convencional_int"], v["precio_credito_inteligente_int"]
    ])]
    return valid

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
                page.wait_for_timeout(400)
                vers = read_versions_from_detail(page, model_title_fallback=c.title)
                if not vers:
                    print(f"[WARN] sin versiones o precios válidos en {c.href}")
                    continue
                results.extend(vers)
                print(f"[OK] {len(vers)} versiones válidas")
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

    print(f"\n[OK] {out_json} → {len(results)} versiones exportadas")

if __name__ == "__main__":
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    main(headless=headless)
