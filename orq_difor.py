# -*- coding: utf-8 -*-
import os
import re
import json
import time
import sys
import traceback
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from utils import saveCar

marcas_difor = [
    {"brand": "Ford", "url": "https://www.difor.cl/ford-chile"},
    {"brand": "Jetour", "url": "https://www.difor.cl/jetour-chile"},
    {"brand": "Opel", "url": "https://www.difor.cl/opel-chile"},
    {"brand": "Mitsubishi", "url": "https://www.difor.cl/mitsubishi-motors-chile"},
    {"brand": "Maxus", "url": "https://www.difor.cl/maxus-chile"},
    {"brand": "Karry", "url": "https://www.difor.cl/karry-chile"},
    {"brand": "Kaiyi", "url": "https://www.difor.cl/kaiyi-chile"},
]

# Para probar solo Jetour, descomenta esto:
marcas_difor = [
     {"brand": "Jetour", "url": "https://www.difor.cl/jetour-chile"}
 ]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

PRECIO_RE = re.compile(r"\$[\d\.\s]+")
NUM_RE = re.compile(r"(\d+(?:[.,]\d+)?)")


# ============ UTILIDADES ============
def precio_a_int(txt: str):
    if not txt:
        return None
    txt = txt.replace("\xa0", " ")
    m = PRECIO_RE.search(txt)
    if not m:
        return None
    num = re.sub(r"[^\d]", "", m.group(0))
    return int(num) if num.isdigit() else None


def to_int_num(txt: str):
    if not txt:
        return None
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


def safe_text(locator, timeout=1500):
    try:
        if locator.count() == 0:
            return ""
        return locator.first.inner_text(timeout=timeout).strip()
    except Exception:
        return ""


# ============ EXTRACCIÓN DE MODELOS ============
def extraer_modelos(page):
    page.wait_for_selector("#listing-collections", timeout=20000)
    scroll_suave(page)

    cards = page.locator('#listing-collections a#collection-card')
    modelos = []

    for i in range(cards.count()):
        a = cards.nth(i)

        nombre = safe_text(a.locator("h2"))
        precio_raw = safe_text(a.locator(".MuiTypography-h6"))
        precio_int = precio_a_int(precio_raw)

        href = a.get_attribute("href") or ""
        url = urljoin(page.url, href)

        try:
            img = a.locator("img").first.get_attribute("src") or ""
        except Exception:
            img = ""

        if not nombre and not url:
            continue

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
            label = safe_text(fila.locator(".css-17fd5p")).lower()
            val = safe_text(fila.locator(".css-uztjiy"))
            val_int = precio_a_int(val)

            if "inteligente" in label:
                valores["precio_credito_inteligente_int"] = val_int
            elif "convencional" in label:
                valores["precio_credito_convencional_int"] = val_int
            elif "todo medio" in label:
                valores["precio_todo_medio_pago_int"] = val_int
            elif "lista" in label:
                valores["precio_lista_int"] = val_int
        except Exception:
            pass

    return valores


def parse_highlights(card):
    datos = {
        "cc": None,
        "combustible": "",
        "transmision": "",
        "potencia_hp": None
    }

    props = card.locator(".highlight-properties-container .highlight-property")

    for i in range(props.count()):
        txt = safe_text(props.nth(i).locator("p"))

        if "cc" in txt.lower():
            datos["cc"] = to_int_num(txt)
        elif any(k in txt.lower() for k in ["gasolina", "diesel", "híbr", "electr"]):
            datos["combustible"] = txt
        elif any(k in txt.lower() for k in ["automática", "manual", "cvt", "dct"]):
            datos["transmision"] = txt
        elif "hp" in txt.lower():
            datos["potencia_hp"] = to_int_num(txt)

    return datos


def extraer_versiones(page, modelo, marca):
    versiones = []

    page.wait_for_selector(".splide__list", timeout=20000)
    scroll_suave(page)

    slides = page.locator(".splide__list li.splide__slide:has(#new-car-version-card)")

    for i in range(slides.count()):
        try:
            card = slides.nth(i).locator("#new-car-version-card")

            version = safe_text(card.locator(".MuiCardHeader-content .css-wp624j"))
            precio_card_raw = safe_text(card.locator(".card-price-title"))
            precio_card_int = precio_a_int(precio_card_raw)

            bono_raw = safe_text(card.locator(".css-ycodjm"))
            bono_int = precio_a_int(bono_raw)

            precios = parse_item_values(card)
            highlights = parse_highlights(card)

            href = ""
            if card.locator(".MuiCardActions-root a[href]").count():
                href = card.locator(".MuiCardActions-root a[href]").first.get_attribute("href") or ""

            url_version = urljoin(page.url, href) if href else ""

            if not version and precio_card_int is None and not href:
                continue

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

        except Exception as e:
            versiones.append({
                "_error": str(e),
                "marca": marca,
                "modelo": modelo,
                "url_modelo": page.url
            })

    return versiones


# ============ MAIN HELPERS ============
def close_cookies_if_any(page):
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


def click_tab_by_text(page, text):
    try:
        tab = page.locator(f"button[role='tab']:has-text('{text}')").first
        if tab.count() > 0 and tab.is_visible():
            tab.click(timeout=1500)
            time.sleep(0.8)
            return True
    except Exception:
        pass

    return False


def click_tab_todos_generico(page):
    try:
        todos = page.locator("[id$='-todos-chile']")
        if todos.count() > 0 and todos.first.is_visible():
            todos.first.click(timeout=1200)
            time.sleep(0.2)
            return
    except Exception:
        pass

    try:
        btn = page.locator("button[role='tab']:has-text('Todos')")
        if btn.count() > 0 and btn.first.is_visible():
            btn.first.click(timeout=1200)
            time.sleep(0.2)
            return
    except Exception:
        pass

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
    for i in range(max_tries):
        if page.locator('#listing-collections a#collection-card').count() > 0:
            return True

        if page.locator("#listing-collections a[href*='-chile']").count() > 0:
            return True

        page.evaluate("(y)=>window.scrollTo(0,y)", (i + 1) * 800)
        time.sleep(0.5)

    return page.locator('#listing-collections a#collection-card').count() > 0


def build_savecar_payload(a):
    tiposprecio = [
        'Crédito inteligente',
        'Crédito convencional',
        'Todo medio de pago',
        'Precio de lista'
    ]

    precio = [
        a.get('precio_credito_inteligente_int'),
        a.get('precio_credito_convencional_int'),
        a.get('precio_todo_medio_pago_int'),
        a.get('precio_lista_int')
    ]

    return {
        'modelo': a.get('modelo'),
        'marca': a.get('marca'),
        'modelDetail': a.get('version'),
        'tiposprecio': tiposprecio,
        'precio': precio
    }


def deduplicar_modelos_por_url(modelos):
    dedup = {}

    for m in modelos:
        url = m.get("url_modelo")
        if not url:
            continue
        dedup[url] = m

    return list(dedup.values())


def extraer_modelos_jetour(page):
    """
    Jetour no tiene tab 'Todos'.
    Por eso se recorren explícitamente SUVs e Híbridos.
    """

    modelos = []

    print("[INFO] Jetour detectado: recorriendo tabs SUVs e Híbridos")

    for tab_name in ["SUVs", "Híbridos"]:
        print(f"[INFO] Jetour tab: {tab_name}")

        clicked = click_tab_by_text(page, tab_name)

        if not clicked:
            print(f"[WARN] No pude hacer click en tab Jetour: {tab_name}")
            continue

        ok = wait_grid_with_scroll(page, max_tries=10)

        if not ok:
            print(f"[WARN] No cargó grilla para Jetour tab: {tab_name}")
            continue

        try:
            modelos_tab = extraer_modelos(page)
        except Exception as e:
            print(f"[WARN] Error extrayendo modelos Jetour tab {tab_name}: {e}")
            modelos_tab = []

        print(f"[INFO] Jetour {tab_name}: {len(modelos_tab)} modelos")

        modelos.extend(modelos_tab)

    modelos = deduplicar_modelos_por_url(modelos)

    print(f"[INFO] Jetour modelos únicos: {len(modelos)}")

    return modelos


# ============ SCRAPER POR MARCA ============
def scrape_brand_flat_json(brand_url, nombre_marca="marca", headless=False):
    ensure_dir()

    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    all_versions = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--window-size=1366,900"]
        )

        ctx = browser.new_context(
            locale="es-CL",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome Safari"
            ),
            viewport={"width": 1366, "height": 900},
        )

        page = ctx.new_page()

        page.goto(brand_url, wait_until="domcontentloaded", timeout=45000)
        close_cookies_if_any(page)

        if nombre_marca.lower() == "jetour":
            modelos = extraer_modelos_jetour(page)
        else:
            click_tab_todos_generico(page)

            ok = wait_grid_with_scroll(page, max_tries=10)

            if not ok:
                for _ in range(3):
                    page.mouse.wheel(0, 1000)
                    time.sleep(0.3)

                page.mouse.wheel(0, -3000)
                time.sleep(0.3)

            modelos = extraer_modelos(page)

            if not modelos:
                click_tab_todos_generico(page)
                ok = wait_grid_with_scroll(page, max_tries=10)
                modelos = extraer_modelos(page) if ok else []

        stats["models_found"] = len(modelos)

        print(f"[INFO] Modelos encontrados en {nombre_marca}: {len(modelos)}")

        for m in modelos:
            try:
                print(f"[RUN] {nombre_marca} - {m.get('modelo')}")

                page.goto(m["url_modelo"], wait_until="domcontentloaded", timeout=45000)
                scroll_suave(page)
                close_cookies_if_any(page)

                vers = extraer_versiones(page, m["modelo"], nombre_marca)

                stats["models_processed"] += 1
                stats["versions_found"] += len([v for v in vers if not v.get("_error")])
                stats["version_errors"] += len([v for v in vers if v.get("_error")])

                all_versions.extend(vers)

                print(f"[OK] {m.get('modelo')}: {len(vers)} versiones")

            except Exception as e:
                stats["model_errors"] += 1
                print(f"[WARN] Error procesando modelo {m.get('modelo')} ({m.get('url_modelo')}): {e}")
                traceback.print_exc()

        ctx.close()
        browser.close()

    path = os.path.join("salida_modelos", f"{nombre_marca}_versiones.json")

    for a in all_versions:
        try:
            if a.get("_error"):
                stats["save_errors"] += 1
                continue

            datos = build_savecar_payload(a)

            if datos["marca"] and datos["modelo"]:
                saveCar(a['marca'], datos, 'www.difor.cl')
                stats["saved_ok"] += 1
                print(datos)
                print("-" * 100)
            else:
                stats["save_errors"] += 1

        except Exception as e:
            stats["save_errors"] += 1
            print(f"[ERROR] saveCar falló para {a}: {e}")
            traceback.print_exc()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_versions, f, ensure_ascii=False, indent=2)

    print(f"[OK] JSON plano creado: {path} ({len(all_versions)} versiones)")

    return path, all_versions, stats


# ============ CLI / ORQUESTADOR ============
def main():
    ensure_dir()

    global_stats = {
        "brands_total": len(marcas_difor),
        "brands_processed": 0,
        "brand_errors": 0,
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    all_rows = []

    try:
        for m in marcas_difor:
            url_brand = m["url"]
            nombre_marca = m["brand"]

            print(f"\n=== MARCA: {nombre_marca} ===")

            try:
                path, rows, stats = scrape_brand_flat_json(
                    url_brand,
                    nombre_marca,
                    headless=HEADLESS
                )

                global_stats["brands_processed"] += 1
                global_stats["models_found"] += stats["models_found"]
                global_stats["models_processed"] += stats["models_processed"]
                global_stats["model_errors"] += stats["model_errors"]
                global_stats["versions_found"] += stats["versions_found"]
                global_stats["version_errors"] += stats["version_errors"]
                global_stats["saved_ok"] += stats["saved_ok"]
                global_stats["save_errors"] += stats["save_errors"]

                all_rows.extend(rows)

            except Exception as e:
                global_stats["brand_errors"] += 1
                print(f"[WARN] Error en marca {nombre_marca}: {e}")
                traceback.print_exc()

        global_path = os.path.join("salida_modelos", "difor_all_versiones.json")

        with open(global_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)

        summary = {
            "status": "success",
            "source": "www.difor.cl",
            **global_stats
        }

        if global_stats["brands_processed"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se pudo procesar ninguna marca")
            sys.exit(1)

        if global_stats["models_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se encontraron modelos")
            sys.exit(1)

        if global_stats["models_processed"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se pudo procesar ningún modelo")
            sys.exit(1)

        if global_stats["versions_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se encontraron versiones")
            sys.exit(1)

        if global_stats["saved_ok"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se guardó ningún registro en Firebase")
            sys.exit(1)

        if global_stats["brands_total"] > 0:
            brand_error_ratio = global_stats["brand_errors"] / global_stats["brands_total"]
            summary["brand_error_ratio"] = round(brand_error_ratio, 4)

        if global_stats["models_found"] > 0:
            model_error_ratio = global_stats["model_errors"] / global_stats["models_found"]
            summary["model_error_ratio"] = round(model_error_ratio, 4)

            if model_error_ratio >= 0.5:
                summary["status"] = "error"
                print(json.dumps(summary, ensure_ascii=False))
                print(
                    f"[ERROR] Demasiados errores de modelo: "
                    f"{global_stats['model_errors']} de {global_stats['models_found']}"
                )
                sys.exit(1)

        print("RUN_OK")
        print(json.dumps(summary, ensure_ascii=False))
        print(f"[OK] JSON global creado: {global_path} ({len(all_rows)} registros)")
        sys.exit(0)

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()