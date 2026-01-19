# dfsk.py
# Scraper DFSK (Chile) - PLP: modelos (dropdown) -> versiones/precios (cards)
# Requiere: pip install playwright && playwright install

import asyncio
import json
import re
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from utils import saveCar

START_URL = "https://www.dfsk.cl/product-list-page"
BASE_URL = "https://www.dfsk.cl"


# -----------------------------
# Helpers
# -----------------------------
def norm(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip()
    return s if s else None


def money_to_int(s: str | None) -> int | None:
    """Convierte '$16.690.000 + IVA' -> 16690000. Retorna None si no hay número."""
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None


# -----------------------------
# UI / Overlays / Filtros
# -----------------------------
async def dismiss_overlays(page):
    candidates = [
        page.get_by_role("button", name=re.compile(r"acept", re.I)),
        page.get_by_role("button", name=re.compile(r"accept", re.I)),
        page.get_by_role("button", name=re.compile(r"entendido", re.I)),
        page.get_by_role("button", name=re.compile(r"continuar", re.I)),
        page.locator("button:has-text('Aceptar')"),
        page.locator("button:has-text('ACEPTAR')"),
    ]
    for btn in candidates:
        try:
            if await btn.first.is_visible(timeout=800):
                await btn.first.click()
                await page.wait_for_timeout(250)
        except Exception:
            pass

    close_candidates = [
        page.get_by_role("button", name=re.compile(r"cerrar|close|×|x", re.I)),
        page.locator('[aria-label="close"], [aria-label="cerrar"], .close, .modal-close'),
    ]
    for c in close_candidates:
        try:
            if await c.first.is_visible(timeout=800):
                await c.first.click()
                await page.wait_for_timeout(250)
        except Exception:
            pass


async def open_filters_panel_if_needed(page):
    triggers = [
        page.get_by_role("button", name=re.compile(r"filtros|filtrar", re.I)),
        page.locator("button:has-text('Filtros'), button:has-text('Filtrar')"),
        page.locator("a:has-text('Filtros'), a:has-text('Filtrar')"),
    ]
    for t in triggers:
        try:
            if await t.first.is_visible(timeout=800):
                await t.first.click()
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass


async def open_modelos_dropdown(page):
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(500)

    await dismiss_overlays(page)
    await open_filters_panel_if_needed(page)

    try:
        await page.locator(".plp_filter__answer, .plp_filter").first.wait_for(state="attached", timeout=30000)
    except PWTimeoutError:
        pass

    header_candidates = [
        page.locator("h3.plp_filter__title__list", has_text=re.compile(r"modelos?", re.I)).first,
        page.locator(
            "xpath=//h3[contains(@class,'plp_filter__title__list')][contains(translate(normalize-space(.),'MODELOS','modelos'),'modelos')]"
        ).first,
        page.locator("xpath=//h3[contains(., 'Modelos') or contains(., 'MODELOS')]").first,
    ]

    header = None
    for cand in header_candidates:
        try:
            await cand.wait_for(state="attached", timeout=8000)
            header = cand
            break
        except PWTimeoutError:
            continue

    if header is None:
        await page.screenshot(path="dfsk_no_header.png", full_page=True)
        html = await page.content()
        with open("dfsk_no_header.html", "w", encoding="utf-8") as f:
            f.write(html)
        raise RuntimeError("No encontré el header 'Modelos' en el DOM (dfsk_no_header.* generado)")

    await header.scroll_into_view_if_needed()
    await dismiss_overlays(page)

    try:
        await header.click(timeout=5000)
    except Exception:
        await header.click(force=True, timeout=5000)

    ul = page.locator("ul#plp_list__Modelo")
    try:
        await ul.wait_for(state="visible", timeout=20000)
    except PWTimeoutError:
        await ul.wait_for(state="attached", timeout=10000)

    return ul


async def get_modelos_items(page):
    ul = await open_modelos_dropdown(page)
    items = ul.locator("li.plp_items__Modelo")
    n = await items.count()

    modelos = []
    for i in range(n):
        li = items.nth(i)
        input_el = li.locator("input.plp_input__checkbox").first
        label_el = li.locator("label.plp_label__checkbox").first
        value = await input_el.get_attribute("value")
        label = await label_el.inner_text()
        modelos.append({"label": (label or "").strip(), "value": (value or "").strip()})

    dedup = {}
    for m in modelos:
        k = m["label"].upper()
        if k and k not in dedup:
            dedup[k] = m
    return list(dedup.values())


async def clear_all_model_filters(page):
    ul = page.locator("ul#plp_list__Modelo")
    await ul.wait_for(state="attached", timeout=15000)

    inputs = ul.locator("input.plp_input__checkbox[filter='model']")
    n = await inputs.count()

    for i in range(n):
        cb = inputs.nth(i)
        try:
            if await cb.is_checked():
                cb_id = await cb.get_attribute("id")
                if cb_id:
                    label = ul.locator(f"label.plp_label__checkbox[for='{cb_id}']").first
                    await label.scroll_into_view_if_needed()
                    await label.click()
                    await page.wait_for_timeout(150)
        except Exception:
            pass


async def apply_model_filter(page, model_value: str):
    ul = page.locator("ul#plp_list__Modelo")
    await ul.wait_for(state="attached", timeout=15000)

    cb = ul.locator(f"input.plp_input__checkbox[filter='model'][value=\"{model_value}\"]").first
    await cb.wait_for(state="attached", timeout=15000)

    cb_id = await cb.get_attribute("id")
    if not cb_id:
        li = cb.locator("xpath=ancestor::li[1]")
        await li.scroll_into_view_if_needed()
        await li.click(force=True)
        return

    label = ul.locator(f"label.plp_label__checkbox[for='{cb_id}']").first
    await label.scroll_into_view_if_needed()

    try:
        await label.click(timeout=5000)
    except Exception:
        await label.click(force=True, timeout=5000)


# -----------------------------
# Extracción de Cards
# -----------------------------
async def wait_grid_ready(page):
    grid = page.locator("section.plp_grid__wrapper")
    await grid.wait_for(state="attached", timeout=25000)

    cards = page.locator("div.plp_vehicles_grid__content__card.plp_grid_card")
    await cards.first.wait_for(state="visible", timeout=25000)


async def safe_get_first_attr(locator, attr: str) -> str | None:
    """
    Si el locator no existe, retorna None sin esperar timeouts largos.
    """
    try:
        if await locator.count() == 0:
            return None
        # No esperamos "visible": solo leemos atributo si está en DOM.
        return await locator.first.get_attribute(attr)
    except Exception:
        return None


async def extract_cards(page, modelo_label: str | None = None):
    await wait_grid_ready(page)

    cards = page.locator("div.plp_vehicles_grid__content__card.plp_grid_card")
    count = await cards.count()

    rows = []
    for i in range(count):
        card = cards.nth(i)

        brand = norm(await card.locator("h5.plp_grid_card__content__h5").first.inner_text())
        model = norm(await card.locator("h3.plp_grid_card__content__h3").first.inner_text())
        version = norm(await card.locator("h5.plp_grid_card__content__h5__fit").first.inner_text())

        precio_desde_texto = norm(await card.locator("h2.plp_grid_card__content__h2").first.inner_text())
        precio_desde = money_to_int(precio_desde_texto)

        detalle = {}
        p_rows = card.locator("p.plp_grid_card__content__p")
        pr_count = await p_rows.count()
        for j in range(pr_count):
            p = p_rows.nth(j)
            span = p.locator("span")
            strong = p.locator("strong.plp_grid_card__content__p__strong__price")
            if await span.count() and await strong.count():
                k = norm(await span.first.inner_text())
                v = norm(await strong.first.inner_text())
                if k:
                    detalle[k] = v

        precio_lista_texto = detalle.get("Precio de Campaña")
        precio_lista = money_to_int(precio_lista_texto)

        bono_directo = money_to_int(detalle.get("Bono Directo"))
        bono_fin = money_to_int(detalle.get("Bono Financiamiento"))

        # ✅ Selector ampliado y NO bloqueante
        cotizar_rel = await safe_get_first_attr(
            card.locator('a[href*="/formulario/cotizacion/"]'),
            "href",
        )
        cotizar_url = urljoin(BASE_URL, cotizar_rel) if cotizar_rel else None

        id_model = None
        id_version = None
        if cotizar_url:
            qs = parse_qs(urlparse(cotizar_url).query)
            id_model = qs.get("id_model", [None])[0]
            id_version = qs.get("id_version", [None])[0]

        row = {
            "brand": brand,
            "model": model or modelo_label,
            "version": version,
            "precio_desde_texto": precio_desde_texto,
            "precio_desde": precio_desde,
            "precio_lista_texto": precio_lista_texto,
            "precio_lista": precio_lista,
            "bono_directo": bono_directo,
            "bono_financiamiento": bono_fin,
            "cotizar_url": cotizar_url,
            "id_model": id_model,
            "id_version": id_version,
            "modelo_filtro": modelo_label or model,
        }

        # Log corto si falta cotizar (para debug, pero no rompe)
        if not cotizar_url:
            # print(f"   [WARN] Sin cotizar_url en card: model={row['model']} version={row['version']}")
            pass

        rows.append(row)

    return rows


# -----------------------------
# Main
# -----------------------------
async def main():
    async with async_playwright() as p:
        # Debug visual:
        # browser = await p.chromium.launch(headless=False, slow_mo=150)
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="es-CL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        page.set_default_timeout(30000)

        print(f"Abriendo: {START_URL}")
        await page.goto(START_URL, wait_until="domcontentloaded")
        await dismiss_overlays(page)
        await open_filters_panel_if_needed(page)

        modelos = await get_modelos_items(page)
        print(f"Modelos encontrados en dropdown: {len(modelos)}")

        all_rows = []

        for idx, m in enumerate(modelos, start=1):
            label = m["label"]
            value = m["value"]
            print(f"\n[{idx}/{len(modelos)}] Modelo: {label}")

            try:
                await open_modelos_dropdown(page)
            except Exception:
                await open_filters_panel_if_needed(page)
                await open_modelos_dropdown(page)

            await clear_all_model_filters(page)
            await apply_model_filter(page, value)

            rows = await extract_cards(page, modelo_label=label)

            print(f"   Versiones (cards) encontradas: {len(rows)}")
            for r in rows:
                print(f"   - {r.get('version')} | desde: {r.get('precio_desde_texto')} | id_version: {r.get('id_version')}")

            all_rows.extend(rows)

        out_file = "dfsk_versiones_precios.json"


        for r in all_rows:
            tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
            precio = [r['precio_desde']-r['bono_financiamiento'],r['precio_lista']-r['bono_directo'],r['precio_lista'],r['precio_lista']]
            datos = {
                'modelo': r['model'],
                'marca': r['brand'],
                'modelDetail': r['version'],
                'tiposprecio': tiposprecio,
                'precio': precio,
              
            }
            saveCar('DFSK',datos,"https://www.dfsk.cl/product-list-page")

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)

        print(f"\nOK -> {out_file} (total registros: {len(all_rows)})")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
