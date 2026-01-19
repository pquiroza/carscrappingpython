import asyncio
import json
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from utils import saveCar
from utils import to_title_custom
BASE = "https://www.bmw.cl"


# -----------------------------
# Utilidades
# -----------------------------
def parse_clp_number(text: str):
    # " $40.900.000 " -> 40900000
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


async def safe_inner_text(locator, default=None):
    try:
        return (await locator.inner_text()).strip()
    except:
        return default


async def safe_attr(locator, name: str, default=None):
    try:
        return await locator.get_attribute(name)
    except:
        return default


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
                await btn.first.click(timeout=1200)
                break
        except:
            pass


async def click_force(locator):
    await locator.scroll_into_view_if_needed()
    try:
        await locator.click(timeout=1500)
    except:
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
    except:
        pass

    try:
        await menu.wait_for(state="visible", timeout=900)
        return name, menu
    except PWTimeoutError:
        try:
            await cat.click()
        except:
            pass

    try:
        await menu.wait_for(state="visible", timeout=1800)
        return name, menu
    except PWTimeoutError:
        return name, None


async def extract_models_from_menu(menu):
    """
    Dropdown:
      - h2.car-name a (link /modelos/...)
    """
    items = menu.locator("h2.car-name").locator("xpath=ancestor::li[1]")
    count = await items.count()

    models = []
    for i in range(count):
        it = items.nth(i)

        a_model = it.locator("h2.car-name a")
        name = await safe_inner_text(a_model, default="")
        href = await safe_attr(a_model, "href")
        modelos_url = urljoin(BASE, href) if href else None

        models.append({
            "model_name": name,
            "modelos_url": modelos_url,
        })

    return models


async def scrape_categories_and_models(page):
    await page.goto(BASE, wait_until="domcontentloaded")
    await try_close_cookie_banner(page)

    await page.wait_for_selector("li.button-dropdown.car-nav > a.dropdown-toggle", timeout=15000)
    cats = page.locator("li.button-dropdown.car-nav > a.dropdown-toggle")
    n = await cats.count()

    results = []
    for i in range(n):
        cat = cats.nth(i)
        category_name, menu = await open_dropdown(page, cat)

        if not menu:
            results.append({"category": category_name, "models": []})
            continue

        models = await extract_models_from_menu(menu)
        results.append({"category": category_name, "models": models})

        try:
            await cat.click()
        except:
            pass

    return results


# -----------------------------
# Paso 2: modelo -> versiones + precios por versión
# -----------------------------
async def find_versions_tabs_nav(page):
    """
    BMW tiene 2 nav.cont-tabs (uno de versiones y otro de ConnectedDrive).
    Elegimos el que contiene botones de versión.
    """
    navs = page.locator("nav.cont-tabs")
    count = await navs.count()
    if count == 0:
        return None

    # Preferir nav que tiene id version-button (aunque se repita, es señal)
    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator('button#version-button, button[id="version-button"]').count():
            return nav

    # Fallback: nav con tablink con value
    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator("button.tablink[value]").count():
            return nav

    # Último fallback
    for i in range(count):
        nav = navs.nth(i)
        if await nav.locator("button.tablink").count():
            return nav

    return None


async def extract_prices_for_version(page, version_value: str):
    """
    Lee:
      <div class="model" id="model-1"> ... </div>
    y extrae:
      - version_title: h3.tit
      - PRECIO DE LISTA -> p.number
      - BONO DEL MES -> p.number
    """
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

    version_title = await safe_inner_text(model_box.locator("div.cont-tit h3.tit"), default=None)

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
    await page.goto(model_url, wait_until="domcontentloaded")
    await try_close_cookie_banner(page)

    try:
        await page.wait_for_selector("nav.cont-tabs", timeout=15000)
    except PWTimeoutError:
        return []

    tabs_nav = await find_versions_tabs_nav(page)
    if not tabs_nav:
        return []

    # Botones de versión: idealmente con value
    btns = tabs_nav.locator("button.tablink[value]")
    n = await btns.count()

    # fallback
    if n == 0:
        btns = tabs_nav.locator("button.tablink")
        n = await btns.count()
        if n == 0:
            return []

    # Capturar lista estable antes del click
    versions = []
    for i in range(n):
        b = btns.nth(i)
        versions.append({
            "version_name": (await safe_inner_text(b, default="")).strip(),
            "version_value": await safe_attr(b, "value"),
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

            # ✅ FIX: wait_for_function sin args posicionales extra (interpolamos val)
            await page.wait_for_function(
                f"""
                () => {{
                    const b = document.querySelector(
                        'nav.cont-tabs button.tablink[value="{val}"]'
                    );
                    return b && b.classList.contains('active');
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
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        categories = await scrape_categories_and_models(page)

        for cat in categories:
            for m in cat["models"]:
                url = m.get("modelos_url")
                if not url:
                    m["versions"] = []
                    continue

                m["versions"] = await get_versions_with_prices(page, url)

        await browser.close()

        out_path = "bmw_cl_modelos_versiones_precios.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False, indent=2)
        for c in categories:
            for m in c["models"]:
                for v in m["versions"]:
                    tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
                    precio = [v['precio_lista'],v['precio_lista'],v['precio_lista'],v['precio_lista']]
                    datos = {
                        'modelo': m['model_name'],
                        'marca': 'BMW',
                        'modelDetail': v['version_name'],    
                        'tiposprecio': tiposprecio,
                        'precio': precio
                    }
                    print(datos)
                    saveCar('BMW',datos,'www.bmw.cl')
        print(f"OK -> {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
