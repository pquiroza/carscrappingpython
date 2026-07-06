import asyncio
import json
import re
import sys
import os
import traceback
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from utils import saveCar
from utils import to_title_custom

BASE = "https://www.bmw.cl"

DEBUG_DIR = Path(__file__).resolve().parent / "debug_bmw"
DEBUG_DIR.mkdir(exist_ok=True)


# -----------------------------
# Utilidades
# -----------------------------
def parse_clp_number(text: str):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


async def safe_inner_text(locator, default=None):
    try:
        return (await locator.inner_text()).strip()
    except Exception:
        return default


async def safe_attr(locator, name: str, default=None):
    try:
        return await locator.get_attribute(name)
    except Exception:
        return default


async def save_debug(page, prefix: str):
    try:
        html = await page.content()
        with open(DEBUG_DIR / f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

    try:
        await page.screenshot(path=str(DEBUG_DIR / f"{prefix}.png"), full_page=True)
    except Exception:
        pass


async def try_close_cookie_banner(page):
    candidates = [
        "button#onetrust-accept-btn-handler",
        "button:has-text('Aceptar')",
        "button:has-text('Acepto')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count():
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass


async def click_force(locator):
    await locator.scroll_into_view_if_needed()
    try:
        await locator.click(timeout=1500)
    except Exception:
        await locator.click(timeout=3000, force=True)


# -----------------------------
# Paso 1: categorías -> modelos
# -----------------------------
async def open_dropdown(page, cat):
    name = (await cat.inner_text()).strip()
    li = cat.locator("xpath=ancestor::li[contains(@class,'button-dropdown')]")
    menu = li.locator("div.dropdown-menu")

    try:
        await cat.hover()
    except Exception:
        pass

    try:
        await menu.wait_for(state="visible", timeout=1200)
        return name, menu
    except PWTimeoutError:
        try:
            await cat.click()
        except Exception:
            pass

    try:
        await menu.wait_for(state="visible", timeout=2500)
        return name, menu
    except PWTimeoutError:
        return name, None


async def extract_models_from_menu(menu):
    items = menu.locator("h2.car-name").locator("xpath=ancestor::li[1]")
    count = await items.count()

    models = []
    for i in range(count):
        it = items.nth(i)

        a_model = it.locator("h2.car-name a")
        name = await safe_inner_text(a_model, default="")
        href = await safe_attr(a_model, "href")
        modelos_url = urljoin(BASE, href) if href else None

        # filtro básico para evitar links basura o vacíos
        if not modelos_url or "/modelos/" not in modelos_url:
            continue

        models.append({
            "model_name": name,
            "modelos_url": modelos_url,
        })

    return models


async def get_categories_locator(page):
    selectors = [
        "li.button-dropdown.car-nav > a.dropdown-toggle",
        "li.button-dropdown a.dropdown-toggle",
        "header a.dropdown-toggle",
        "a.dropdown-toggle",
    ]

    for sel in selectors:
        try:
            await page.wait_for_selector(sel, state="attached", timeout=10000)
            loc = page.locator(sel)
            count = await loc.count()
            if count > 0:
                print(f"[INFO] Selector categorías detectado: {sel} ({count})")
                return loc, sel
        except Exception:
            pass

    return None, None


async def scrape_categories_and_models(page):
    await page.goto(BASE, wait_until="domcontentloaded", timeout=45000)
    await page.wait_for_timeout(3000)

    await try_close_cookie_banner(page)
    await page.wait_for_timeout(1500)

    cats, used_selector = await get_categories_locator(page)

    if cats is None:
        await save_debug(page, "bmw_home_no_categories")
        raise RuntimeError("No se encontraron categorías del menú BMW con ningún selector")

    n = await cats.count()
    if n == 0:
        await save_debug(page, "bmw_home_zero_categories")
        raise RuntimeError("El locator de categorías existe, pero no devolvió elementos")

    results = []
    seen_urls = set()

    for i in range(n):
        cat = cats.nth(i)
        category_name, menu = await open_dropdown(page, cat)

        if not menu:
            results.append({"category": category_name, "models": []})
            continue

        models = await extract_models_from_menu(menu)

        # deduplicar por URL dentro de cada categoría
        filtered_models = []
        for m in models:
            u = m.get("modelos_url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                filtered_models.append(m)

        results.append({"category": category_name, "models": filtered_models})

        try:
            await cat.click()
        except Exception:
            pass

    return results


# -----------------------------
# Paso 2: modelo -> versiones + precios por versión
# -----------------------------
async def find_versions_tabs_nav(page):
    navs = page.locator("nav.cont-tabs")
    count = await navs.count()
    if count == 0:
        return None

    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator('button#version-button, button[id="version-button"]').count():
            return nav

    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator("button.tablink[value]").count():
            return nav

    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator("button.tablink").count():
            return nav

    return None


async def extract_prices_for_version(page, version_value: str):
    model_box = page.locator(f"#model-{version_value}")

    try:
        await model_box.wait_for(state="attached", timeout=8000)
    except PWTimeoutError:
        return {
            "version_title": None,
            "precio_lista_text": None,
            "precio_lista": None,
            "bono_mes_text": None,
            "bono_mes": None,
            "prices_error": f"#model-{version_value} no apareció",
        }

    version_title = await safe_inner_text(
        model_box.locator("div.cont-tit h3.tit"),
        default=None
    )

    precio_lista_loc = model_box.locator(
        'div.price:has(p.pref:has-text("PRECIO DE LISTA")) p.number'
    )
    precio_lista_text = await safe_inner_text(precio_lista_loc, default=None)
    precio_lista = parse_clp_number(precio_lista_text or "")

    bono_mes_loc = model_box.locator(
        'div.price:has(p.pref:has-text("BONO DEL MES")) p.number'
    )
    bono_mes_text = await safe_inner_text(bono_mes_loc, default=None)
    bono_mes = parse_clp_number(bono_mes_text or "")

    return {
        "version_title": version_title,
        "precio_lista_text": precio_lista_text,
        "precio_lista": precio_lista,
        "bono_mes_text": bono_mes_text,
        "bono_mes": bono_mes,
    }


async def get_versions_with_prices(page, model_url: str):
    try:
        await page.goto(model_url, wait_until="domcontentloaded", timeout=45000)
    except PWTimeoutError:
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", model_url.split("/")[-1] or "goto_timeout")
        await save_debug(page, f"bmw_model_goto_timeout_{safe_name}")
        raise

    await page.wait_for_timeout(2500)
    await try_close_cookie_banner(page)
    await page.wait_for_timeout(1200)

    try:
        await page.wait_for_selector("nav.cont-tabs", state="attached", timeout=20000)
    except PWTimeoutError:
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", model_url.split("/")[-1] or "no_tabs")
        await save_debug(page, f"bmw_model_no_tabs_{safe_name}")
        return []

    tabs_nav = await find_versions_tabs_nav(page)
    if not tabs_nav:
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", model_url.split("/")[-1] or "tabs_nav_not_found")
        await save_debug(page, f"bmw_model_tabs_nav_not_found_{safe_name}")
        return []

    btns = tabs_nav.locator("button.tablink[value]")
    n = await btns.count()

    if n == 0:
        btns = tabs_nav.locator("button.tablink")
        n = await btns.count()
        if n == 0:
            return []

    versions = []
    seen_version_keys = set()

    for i in range(n):
        b = btns.nth(i)
        version_name = (await safe_inner_text(b, default="")).strip()
        version_value = await safe_attr(b, "value")

        key = (version_name, version_value)
        if key in seen_version_keys:
            continue
        seen_version_keys.add(key)

        versions.append({
            "version_name": version_name,
            "version_value": version_value,
        })

    out = []
    for v in versions:
        val = v["version_value"]

        if not val:
            out.append({**v, "active_confirmed": False, "prices_error": "sin value"})
            continue

        btn = tabs_nav.locator(f'button.tablink[value="{val}"]')

        try:
            await click_force(btn)
            await page.wait_for_timeout(800)

            await page.wait_for_function(
                f"""
                () => {{
                    const b = document.querySelector(
                        'nav.cont-tabs button.tablink[value="{val}"]'
                    );
                    return !!(b && b.classList.contains('active'));
                }}
                """,
                timeout=8000,
            )

            prices = await extract_prices_for_version(page, val)

            out.append({
                **v,
                "active_confirmed": True,
                **prices,
            })

        except Exception as e:
            out.append({
                **v,
                "active_confirmed": False,
                "error": str(e),
            })

    return out


# -----------------------------
# Orquestación
# -----------------------------
async def main():
    out_path = "bmw_cl_modelos_versiones_precios.json"

    stats = {
        "categories_found": 0,
        "models_found": 0,
        "models_processed": 0,
        "versions_found": 0,
        "versions_with_price": 0,
        "saved_ok": 0,
        "model_errors": 0,
        "save_errors": 0,
    }

    browser = None
    context = None

    try:
        async with async_playwright() as p:
            # BMW por defecto visible
            headless = os.getenv("HEADLESS", "false").lower() == "true"

            browser = await p.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"]
            )

            context = await browser.new_context(
                locale="es-CL",
                viewport={"width": 1440, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )

            page = await context.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(45000)

            categories = await scrape_categories_and_models(page)
            stats["categories_found"] = len(categories)

            for cat in categories:
                stats["models_found"] += len(cat["models"])

                for m in cat["models"]:
                    url = m.get("modelos_url")
                    if not url:
                        m["versions"] = []
                        continue

                    try:
                        versions = await get_versions_with_prices(page, url)
                        m["versions"] = versions
                        stats["models_processed"] += 1
                        stats["versions_found"] += len(versions)

                        for v in versions:
                            if v.get("precio_lista") is not None:
                                stats["versions_with_price"] += 1

                    except Exception as e:
                        stats["model_errors"] += 1
                        m["versions"] = []
                        print(f"[WARN] Error procesando modelo {m.get('model_name')} ({url}): {e}")
                        traceback.print_exc()

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(categories, f, ensure_ascii=False, indent=2)

            for c in categories:
                for m in c["models"]:
                    for v in m.get("versions", []):
                        try:
                            tiposprecio = [
                                "Crédito inteligente",
                                "Crédito convencional",
                                "Todo medio de pago",
                                "Precio de lista"
                            ]

                            # Mantengo tu lógica actual
                            precio = [
                                v.get("precio_lista"),
                                v.get("precio_lista"),
                                v.get("precio_lista"),
                                v.get("precio_lista")
                            ]

                            datos = {
                                "modelo": m.get("model_name"),
                                "marca": "BMW",
                                "modelDetail": v.get("version_name"),
                                "tiposprecio": tiposprecio,
                                "precio": precio
                            }

                            print(datos)
                            saveCar("BMW", datos, "www.bmw.cl")
                            stats["saved_ok"] += 1

                        except Exception as e:
                            stats["save_errors"] += 1
                            print(f"[ERROR] saveCar falló para BMW {m.get('model_name')} {v.get('version_name')}: {e}")
                            traceback.print_exc()

    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass

        if browser:
            try:
                await browser.close()
            except Exception:
                pass

    summary = {
        "status": "success",
        "source": "www.bmw.cl",
        "output_file": out_path,
        **stats
    }

    total_versions = stats["versions_found"]

    if stats["models_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron modelos")
        sys.exit(1)

    if stats["models_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar ningún modelo")
        sys.exit(1)

    if total_versions == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron versiones")
        sys.exit(1)

    if stats["saved_ok"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se guardó ningún registro en Firebase")
        sys.exit(1)

    # Regla importante para el orquestador:
    # si falla demasiada proporción de modelos, debe marcar error
    if stats["models_found"] > 0:
        error_ratio = stats["model_errors"] / stats["models_found"]
        summary["error_ratio"] = round(error_ratio, 4)

        if error_ratio >= 0.5:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print(f"[ERROR] Demasiados errores de modelo: {stats['model_errors']} de {stats['models_found']}")
            sys.exit(1)

    print(json.dumps(summary, ensure_ascii=False))
    print(f"OK -> {out_path}")
    print("RUN_OK")
    sys.exit(0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)