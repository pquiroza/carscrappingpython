from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urljoin
import re
import json
import os
import sys
import traceback

from utils import saveCar
from utils import to_title_custom

BASE_URL = "https://www.lynkco.cl"
BRAND = "LYNK & CO"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

# =========================
# SELECTORES FLEXIBLES
# =========================
VERSION_SELECTORS = [
    "div.centro-abs div.btn_version",
    "div.btn_version",
    ".btn_version",
    "[id_version]",
    "[data-version]",
    "button:has-text('Hyper')",
    "button:has-text('Plus')",
    "button:has-text('Pro')",
    "button:has-text('Halo')",
]

# =========================
# UTILIDADES
# =========================
def limpiar_monto(texto):
    if not texto:
        return None
    solo_numeros = re.sub(r"[^\d]", "", str(texto))
    return int(solo_numeros) if solo_numeros else None


def ir_a_pagina(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)


def safe_text(locator, timeout=2000):
    try:
        if locator.count() == 0:
            return ""
        return (locator.first.inner_text(timeout=timeout) or "").strip()
    except Exception:
        return ""


def safe_subtract(a, b):
    if a in (None, 0):
        return None if a is None else a
    return a - (b or 0)


# =========================
# VERSIONES (ROBUSTO)
# =========================
def wait_version_buttons(page):
    last_error = None

    for sel in VERSION_SELECTORS:
        try:
            page.wait_for_selector(sel, timeout=5000)
            loc = page.locator(sel)
            if loc.count() > 0:
                print(f"[OK] Versiones detectadas con selector: {sel}")
                return loc
        except Exception as e:
            last_error = e

    # DEBUG
    try:
        with open("debug_lynkco.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print("[DEBUG] HTML guardado en debug_lynkco.html")
    except:
        pass

    raise RuntimeError(f"No se encontraron versiones: {last_error}")


def obtener_versiones(page, url_modelo):
    ir_a_pagina(page, url_modelo)

    botones = wait_version_buttons(page)
    total = botones.count()

    versiones = []

    for i in range(total):
        boton = botones.nth(i)

        nombre = safe_text(boton.locator("span"))
        if not nombre:
            nombre = safe_text(boton)

        if nombre:
            versiones.append(nombre.strip())

    # quitar duplicados
    return list(dict.fromkeys(versiones))


def click_version(page, version_texto):
    botones = wait_version_buttons(page)

    for i in range(botones.count()):
        btn = botones.nth(i)
        txt = safe_text(btn.locator("span")) or safe_text(btn)

        if txt.strip() != version_texto:
            continue

        btn.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        try:
            btn.click(force=True)
        except:
            page.evaluate(
                """(txt) => {
                    const btns = [...document.querySelectorAll('div,button')];
                    const b = btns.find(x => x.innerText.trim() === txt);
                    if (b) b.click();
                }""",
                version_texto
            )

        page.wait_for_timeout(1500)
        return

    raise RuntimeError(f"No se pudo clickear versión {version_texto}")


# =========================
# PRECIOS
# =========================
def extraer_info_precio(page):
    texto = page.content()

    match = re.search(r"\$[\d\.]+", texto)
    precio_desde = limpiar_monto(match.group(0)) if match else None

    return {
        "precio_desde": precio_desde,
        "precio_lista": precio_desde,
        "bono_directo": 0,
        "bono_financiamiento": 0
    }


# =========================
# MODELOS
# =========================
def obtener_modelos(page):
    ir_a_pagina(page, BASE_URL)

    modelos = [
        {"model": "06", "url": f"{BASE_URL}/lynkco06"},
        {"model": "08", "url": f"{BASE_URL}/lynkco08"},
        {"model": "09", "url": f"{BASE_URL}/lynkco09"},
    ]

    return modelos


# =========================
# MAIN SCRAPER
# =========================
def scrap_lynkco():
    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        modelos = obtener_modelos(page)

        stats["models_found"] = len(modelos)

        print("Modelos encontrados:")
        for m in modelos:
            print(f" - {m['model']} -> {m['url']}")

        resultado = []

        for m in modelos:
            try:
                versiones = obtener_versiones(page, m["url"])

                for v in versiones:
                    try:
                        ir_a_pagina(page, m["url"])
                        click_version(page, v)

                        info = extraer_info_precio(page)

                        resultado.append({
                            "brand": BRAND,
                            "model": m["model"],
                            "version": v,
                            **info
                        })

                        stats["versions_found"] += 1

                    except Exception as e:
                        stats["version_errors"] += 1
                        print(f"[WARN] Error versión {v}: {e}")

                stats["models_processed"] += 1

            except Exception as e:
                stats["model_errors"] += 1
                print(f"Error modelo {m['model']}: {e}")

        browser.close()
        return resultado, stats


# =========================
# MAIN
# =========================
def main():
    try:
        data, stats = scrap_lynkco()

        with open("lynkco.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        for a in data:
            try:
                tiposprecio = [
                    'Crédito inteligente',
                    'Crédito convencional',
                    'Todo medio de pago',
                    'Precio de lista'
                ]

                precio = [
                    a.get("precio_desde"),
                    a.get("precio_lista"),
                    a.get("precio_lista"),
                    a.get("precio_lista")
                ]

                datos = {
                    "marca": to_title_custom(a["brand"]),
                    "modelo": a["model"],
                    "modelDetail": a["version"],
                    "tiposprecio": tiposprecio,
                    "precio": precio
                }

                saveCar("Lynk & Co", datos, 'www.lynkco.cl')
                stats["saved_ok"] += 1

            except Exception:
                stats["save_errors"] += 1

        print("RUN_OK")
        print(json.dumps(stats, ensure_ascii=False))

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()