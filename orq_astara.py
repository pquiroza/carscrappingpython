import os
import re
import json
import sys
import traceback
from dataclasses import dataclass
from typing import List, Optional, Dict
from urllib.parse import urljoin

from utils import saveCar
from playwright.sync_api import sync_playwright, Page

# ==========================
# CONFIG: agrega aquí marcas
# ==========================
BRANDS = [
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
    },
    {
        "brand": "KGM",
        "list_url": "https://astararetail.cl/kgm/",
        "base": "https://astararetail.cl",
    }
]

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


def detect_gallery_selector(page: Page) -> Optional[str]:
    node = page.locator("[id^='eael-filter-gallery-wrapper-']").first
    if node.count() > 0:
        idv = node.get_attribute("id")
        return f"#{idv}"

    node = page.locator(".eael-filter-gallery-wrapper").first
    if node.count() > 0:
        return ".eael-filter-gallery-wrapper"

    return None


@dataclass
class ModelLink:
    brand: str
    modelo: str
    url_modelo: str
    img: Optional[str] = None


def wait_gallery_ready(page: Page, gallery_selector: str, timeout_ms: int = 20000):
    wrapper = page.locator(gallery_selector).first
    wrapper.wait_for(state="attached", timeout=timeout_ms)

    wrapper.locator(".eael-filterable-gallery-item-wrap").first.wait_for(
        state="attached",
        timeout=timeout_ms
    )


def collect_model_links(
    page: Page,
    brand_name: str,
    base: str,
    gallery_selector: Optional[str]
) -> List[ModelLink]:

    if not gallery_selector:
        gallery_selector = detect_gallery_selector(page)
        if not gallery_selector:
            print(f"[WARN] No se encontró galería para {brand_name}")
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

        links.append(
            ModelLink(
                brand=brand_name,
                modelo=modelo,
                url_modelo=url_abs,
                img=img
            )
        )

    return links


def wait_versions_block(page: Page, timeout_ms: int = 25000) -> str:
    """
    Espera el bloque de versiones probando varios selectores.
    Retorna el selector detectado.
    """

    try:
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(1200)
    except Exception:
        pass

    selectors = [
        "#ag-list-comparer .ag-comparer-model-wrapper",
        "#ag-list-comparer",
        ".ag-comparer-model-wrapper",
        ".ag-list-comparer",
        "[class*='comparer']",
        "[class*='version']",
        "h2.elementor-heading-title:has-text('Elige una versión')",
        "text=Elige una versión",
    ]

    last_error = None

    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=5000, state="attached")
            print(f"[OK] Bloque versiones detectado con selector: {sel}")
            return sel
        except Exception as e:
            last_error = e

    try:
        print("[DEBUG] URL actual:", page.url)
        print("[DEBUG] Título:", page.title())

        html = page.content()
        with open("debug_astara_no_versions.html", "w", encoding="utf-8") as f:
            f.write(html)

        print("[DEBUG] HTML guardado en debug_astara_no_versions.html")
    except Exception:
        pass

    raise Exception(f"No se encontró bloque de versiones. Último error: {last_error}")


def parse_version_card(card) -> Dict:
    ver_box = card.locator(".version").first
    modelo_full = None
    version_name = None

    if ver_box.count() > 0:
        ps = ver_box.locator("p")

        if ps.count() >= 1:
            modelo_full = norm(ps.nth(0).inner_text())

        if ps.count() >= 2:
            version_name = norm(ps.nth(1).inner_text())

    precio_lista = None
    bonus_boxes = card.locator(".bonus_price")

    if bonus_boxes.count() > 0:
        bp = bonus_boxes.nth(0)
        txt = norm(bp.inner_text())
        m = MONEY_RX.search(txt)

        if m:
            precio_lista = money_to_int(m.group(0))

    precio_todo_medio = None
    precio_con_financ = None
    price_boxes = card.locator(".price")

    for i in range(price_boxes.count()):
        pb = price_boxes.nth(i)

        label = ""
        if pb.locator("p").count() > 0:
            label = norm(pb.locator("p").nth(0).inner_text())

        amount_node = pb.locator(".h1").first
        amount_txt = norm(amount_node.inner_text()) if amount_node.count() > 0 else ""
        amount = money_to_int(amount_txt)

        label_lower = label.lower()

        if "todo medio" in label_lower:
            precio_todo_medio = amount
        elif "financiam" in label_lower or "crédito" in label_lower or "credito" in label_lower:
            precio_con_financ = amount

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

    precio_card = precio_todo_medio
    bono_total = (bono_todo_medio or 0) + (bono_financ or 0)

    return {
        "modelo_full": modelo_full,
        "version_name": version_name,
        "precio_lista_int": precio_lista,
        "precio_todo_medio_pago_int": precio_todo_medio,
        "precio_credito_inteligente_int": precio_con_financ,
        "precio_card_int": precio_card,
        "bono_total_int": bono_total,
    }


def extract_versions_from_model(
    page: Page,
    url_modelo: str,
    brand_name: str,
    modelo_label_fallback: Optional[str]
) -> List[Dict]:

    wait_versions_block(page)

    cards = page.locator("#ag-list-comparer .ag-comparer-model-wrapper")

    if cards.count() == 0:
        cards = page.locator(".ag-comparer-model-wrapper")

    if cards.count() == 0:
        cards = page.locator("[class*='comparer'][class*='wrapper']")

    out: List[Dict] = []
    total_cards = cards.count()

    print(f"[INFO] Cards de versiones detectadas: {total_cards}")

    if total_cards == 0:
        print(f"[WARN] No hay cards de versiones en {url_modelo}")
        return out

    for i in range(total_cards):
        card = cards.nth(i)
        data = parse_version_card(card)

        modelo_full = data.get("modelo_full") or modelo_label_fallback or ""
        parts = modelo_full.split()

        if len(parts) >= 2:
            marca = parts[0]
            modelo = " ".join(parts[1:])
        else:
            marca = brand_name
            modelo = modelo_full or modelo_label_fallback

        if marca and "ssang" in marca.lower():
            marca = "SsangYong"

        row = {
            "marca": marca or brand_name,
            "modelo": modelo,
            "version": data.get("version_name"),
            "precio_card_int": data.get("precio_card_int"),
            "bono_int": data.get("bono_total_int"),
            "precio_credito_inteligente_int": data.get("precio_credito_inteligente_int"),
            "precio_credito_convencional_int": None,
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


def main(headless: bool = True):
    output = "astararetail_all_formato.json"
    results: List[Dict] = []

    stats = {
        "brands_total": len(BRANDS),
        "brands_processed": 0,
        "models_found": 0,
        "models_processed": 0,
        "versions_found": 0,
        "saved_ok": 0,
        "model_errors": 0,
        "save_errors": 0,
    }

    browser = None
    context = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )

            context = browser.new_context(
                locale="es-CL",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 1200},
            )

            page = context.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(45000)

            for b in BRANDS:
                brand_name = b["brand"]
                list_url = b["list_url"]
                base = b.get("base") or list_url
                gallery_selector = b.get("gallery_selector")

                print(f"\n=== {brand_name} ===")

                try:
                    page.goto(list_url, wait_until="networkidle", timeout=45000)
                except Exception:
                    page.goto(list_url, wait_until="domcontentloaded", timeout=45000)

                page.wait_for_timeout(1500)
                try_dismiss_overlays(page)

                model_links = collect_model_links(page, brand_name, base, gallery_selector)

                stats["brands_processed"] += 1
                stats["models_found"] += len(model_links)

                print(f"[INFO] Modelos en {brand_name}: {len(model_links)}")

                for idx, m in enumerate(model_links, 1):
                    try:
                        print(f"\n[RUN] {brand_name} - {m.modelo}")
                        print(f"[URL] {m.url_modelo}")

                        try:
                            page.goto(m.url_modelo, wait_until="networkidle", timeout=45000)
                        except Exception:
                            page.goto(m.url_modelo, wait_until="domcontentloaded", timeout=45000)

                        page.wait_for_timeout(1800)
                        try_dismiss_overlays(page)

                        rows = extract_versions_from_model(
                            page,
                            m.url_modelo,
                            brand_name,
                            m.modelo
                        )

                        results.extend(rows)

                        stats["models_processed"] += 1
                        stats["versions_found"] += len(rows)

                        print(f"[OK] [{idx}/{len(model_links)}] {m.modelo}: {len(rows)} versiones")

                    except Exception as e:
                        stats["model_errors"] += 1
                        print(f"[WARN] Error en {m.url_modelo}: {e}")
                        traceback.print_exc()

            with open(output, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            for r in results:
                try:
                    tiposprecio = [
                        "Crédito inteligente",
                        "Crédito convencional",
                        "Todo medio de pago",
                        "Precio de lista"
                    ]

                    precio = [
                        r["precio_credito_inteligente_int"],
                        r["precio_todo_medio_pago_int"],
                        r["precio_todo_medio_pago_int"],
                        r["precio_lista_int"]
                    ]

                    datos = {
                        "marca": r["marca"],
                        "modelo": r["modelo"],
                        "modelDetail": r["version"],
                        "precio": precio,
                        "tiposprecio": tiposprecio
                    }

                    print(datos)
                    print("-" * 50)

                    saveCar(r["marca"], datos, "astararetail.cl")
                    stats["saved_ok"] += 1

                except Exception as e:
                    stats["save_errors"] += 1
                    print(
                        f"[ERROR] saveCar falló para "
                        f"{r.get('marca')} {r.get('modelo')} {r.get('version')}: {e}"
                    )
                    traceback.print_exc()

    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass

        if browser:
            try:
                browser.close()
            except Exception:
                pass

    summary = {
        "status": "success",
        "source": "astararetail.cl",
        "output_file": output,
        **stats
    }

    if not results:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se extrajeron versiones")
        sys.exit(1)

    if stats["saved_ok"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se guardó ningún registro en Firebase")
        sys.exit(1)

    if stats["model_errors"] > 0 or stats["save_errors"] > 0:
        summary["status"] = "partial_success"

    print(json.dumps(summary, ensure_ascii=False))
    print(f"\n[OK] {output} → {len(results)} versiones totales")
    print("RUN_OK")
    sys.exit(0)


if __name__ == "__main__":
    headless = os.getenv("HEADLESS", "true").lower() == "true"

    try:
        main(headless=headless)
    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)