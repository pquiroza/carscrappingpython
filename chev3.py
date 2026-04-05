import asyncio
import json
import re
from urllib.parse import urljoin, urlparse
from utils import saveCar, to_title_custom
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE = "https://www.coseche.com"
START_URL = f"{BASE}/marcas/chevrolet/nuevo"
BRAND = "CHEVROLET"


# ----------------------------
# Helpers
# ----------------------------
def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def money_to_int(text: str):
    """
    "$22.490.000" -> 22490000
    """
    if not text:
        return None
    t = text.strip()
    t = t.replace("$", "").replace(" ", "")
    t = t.replace(".", "").replace(",", "")
    m = re.search(r"\d+", t)
    return int(m.group(0)) if m else None


def extract_money(text: str) -> str:
    """
    Encuentra un monto estilo $22.490.000 dentro de un texto.
    """
    if not text:
        return ""
    m = re.search(r"\$\s*[\d\.\,]+", text)
    return clean_text(m.group(0)).replace(" ", "") if m else clean_text(text)


def ensure_abs_url(href: str) -> str:
    if not href:
        return None
    return urljoin(BASE, href)


def slug_from_model_url(model_url: str) -> str:
    """
    https://www.coseche.com/marcas/chevrolet/nuevo/chevrolet-bolt-euv -> chevrolet-bolt-euv
    """
    if not model_url:
        return ""
    path = urlparse(model_url).path.rstrip("/")
    return path.split("/")[-1]


def slug_to_model(slug: str) -> str:
    """
    'chevrolet-bolt-euv' -> 'BOLT EUV'
    """
    if not slug:
        return None
    s = slug.lower()
    # quita prefijo marca si viene incluido
    s = s.replace("chevrolet-", "")
    parts = [p for p in s.split("-") if p]
    return " ".join(p.upper() for p in parts) if parts else None


async def safe_goto(page, url: str, timeout_ms: int = 45000):
    # domcontentloaded suele ser más rápido y estable; luego complementamos con red idle.
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeoutError:
        pass


async def get_models_from_listing(page) -> list[dict]:
    """
    Devuelve lista de dicts: {model_name, model_url}
    """
    await safe_goto(page, START_URL)

    # El listado de modelos suele estar en anchors a /marcas/chevrolet/nuevo/<slug>
    # Tomamos todos los links que calcen ese patrón y filtramos duplicados.
    anchors = page.locator('a[href^="/marcas/chevrolet/nuevo/"]')
    count = await anchors.count()

    seen = set()
    models = []

    for i in range(count):
        href = await anchors.nth(i).get_attribute("href")
        if not href:
            continue
        if href.count("/") < 4:
            continue

        # descartar links con query o rutas no-modelo
        absu = ensure_abs_url(href.split("?")[0])
        if not absu or absu.endswith("/nuevo"):
            continue

        # solo rutas tipo .../nuevo/<slug> (sin /formulario, etc.)
        path = urlparse(absu).path.strip("/")
        parts = path.split("/")
        if len(parts) < 4:
            continue
        # ejemplo: marcas/chevrolet/nuevo/chevrolet-groove
        if parts[:3] != ["marcas", "chevrolet", "nuevo"]:
            continue
        if len(parts) != 4:
            continue

        if absu in seen:
            continue
        seen.add(absu)

        # nombre: intenta leer texto del link; si no, usa slug humanizado
        txt = clean_text(await anchors.nth(i).inner_text())
        slug = parts[-1]
        model_name = txt if txt else slug_to_model(slug) or slug

        models.append({"model_name": model_name, "model_url": absu})

    # Si por algún motivo el selector no agarró nada, intentamos con cards (fallback)
    if not models:
        cards = page.locator('a[href*="/marcas/chevrolet/nuevo/"]')
        c2 = await cards.count()
        for i in range(c2):
            href = await cards.nth(i).get_attribute("href")
            absu = ensure_abs_url((href or "").split("?")[0])
            if not absu or absu.endswith("/nuevo"):
                continue
            path = urlparse(absu).path.strip("/")
            parts = path.split("/")
            if len(parts) == 4 and parts[:3] == ["marcas", "chevrolet", "nuevo"]:
                if absu in seen:
                    continue
                seen.add(absu)
                txt = clean_text(await cards.nth(i).inner_text())
                slug = parts[-1]
                model_name = txt if txt else slug_to_model(slug) or slug
                models.append({"model_name": model_name, "model_url": absu})

    return models


async def extract_versions_from_model(page, model_url: str) -> list[dict]:
    """
    En la página de modelo, extrae las tarjetas del carrusel (swiper-slide) con:
    - version_name
    - desde_text / desde
    - ver_version_url / version_id
    - cotizar_url
    - reservar_url
    - credito_inteligente / credito_convencional / todo_medio_pago
    """
    await safe_goto(page, model_url)

    # Espera a que exista al menos 1 slide/tarjeta en DOM.
    # IMPORTANT: no uses "visible" porque swiper-wrapper puede estar hidden.
    # attached es suficiente para scrapear.
    await page.wait_for_selector(".swiper-slide article", state="attached", timeout=25000)

    # A veces renderiza más al scrollear
    try:
        await page.mouse.wheel(0, 1200)
    except Exception:
        pass

    slides = page.locator(".swiper-slide article")
    n = await slides.count()
    if n == 0:
        return []

    rows = []
    for i in range(n):
        card = slides.nth(i)

        # Nombre de versión
        version_name = ""
        h3 = card.locator("h3")
        if await h3.count():
            version_name = clean_text(await h3.first.inner_text())

        # "Desde" principal
        # Busca el h4 grande del precio
        desde_text = ""
        h4 = card.locator("h4")
        if await h4.count():
            desde_text = extract_money(await h4.first.inner_text())

        # URLs: ver versión / cotizar / reservar
        ver_version_href = None
        cotizar_href = None
        reservar_href = None

        # En tu HTML: "VER VERSIÓN" es un <a> en el footer
        ver_a = card.locator('a:has-text("VER VERSIÓN")')
        if await ver_a.count():
            ver_version_href = await ver_a.first.get_attribute("href")

        cot_a = card.locator('a:has-text("COTIZAR")')
        if await cot_a.count():
            cotizar_href = await cot_a.first.get_attribute("href")

        res_a = card.locator('a:has-text("Reservar")')
        if await res_a.count():
            reservar_href = await res_a.first.get_attribute("href")

        ver_version_url = ensure_abs_url(ver_version_href)
        cotizar_url = ensure_abs_url(cotizar_href)
        reservar_url = ensure_abs_url(reservar_href)

        # version_id viene en la url /.../<id> o /reservation-payment/<id>
        version_id = None
        if ver_version_url:
            m = re.search(r"/(\d+)\s*$", ver_version_url)
            if m:
                version_id = m.group(1)
        if not version_id and reservar_url:
            m = re.search(r"/reservation-payment/(\d+)", reservar_url)
            if m:
                version_id = m.group(1)

        # Precios del tab "Precios"
        # Son pares label -> value en spans
        credito_inteligente_text = ""
        credito_convencional_text = ""
        todo_medio_pago_text = ""

        # buscamos dentro del card el bloque que contiene esos labels
        # (No dependemos de clases exactas)
        price_rows = card.locator("div:has-text('Con Crédito Inteligente')").locator("..")
        if await price_rows.count() == 0:
            # fallback: busca todos los divs dentro del panel que tenga esos labels
            panel = card.locator('div[role="tabpanel"]')
            if await panel.count():
                candidates = panel.first.locator("div")
                ccount = await candidates.count()
                for j in range(ccount):
                    txt = clean_text(await candidates.nth(j).inner_text())
                    if "Con Crédito Inteligente" in txt:
                        # busca el último $ en ese bloque
                        credito_inteligente_text = extract_money(txt)
                    elif "Con Crédito Convencional" in txt:
                        credito_convencional_text = extract_money(txt)
                    elif "Con todo medio de pago" in txt:
                        todo_medio_pago_text = extract_money(txt)
        else:
            # si encontramos row, intentamos extraer $ desde la misma fila
            txt = clean_text(await price_rows.first.inner_text())
            credito_inteligente_text = extract_money(txt)

        # Si faltan, buscamos en el panel completo
        panel2 = card.locator('div[role="tabpanel"]')
        if await panel2.count():
            full_panel_text = clean_text(await panel2.first.inner_text())
            if not credito_inteligente_text and "Con Crédito Inteligente" in full_panel_text:
                # extrae línea más cercana: fallback (igual útil)
                m = re.search(r"Con Crédito Inteligente.*?(\$\s*[\d\.\,]+)", full_panel_text)
                if m:
                    credito_inteligente_text = clean_text(m.group(1)).replace(" ", "")
            if not credito_convencional_text and "Con Crédito Convencional" in full_panel_text:
                m = re.search(r"Con Crédito Convencional.*?(\$\s*[\d\.\,]+)", full_panel_text)
                if m:
                    credito_convencional_text = clean_text(m.group(1)).replace(" ", "")
            if not todo_medio_pago_text and "Con todo medio de pago" in full_panel_text:
                m = re.search(r"Con todo medio de pago.*?(\$\s*[\d\.\,]+)", full_panel_text)
                if m:
                    todo_medio_pago_text = clean_text(m.group(1)).replace(" ", "")

        row = {
            "version_name": version_name,
            "desde_text": desde_text,
            "desde": money_to_int(desde_text),
            "ver_version_url": ver_version_url,
            "version_id": version_id,
            "cotizar_url": cotizar_url,
            "reservar_url": reservar_url,
            "credito_inteligente_text": credito_inteligente_text,
            "credito_inteligente": money_to_int(credito_inteligente_text),
            "credito_convencional_text": credito_convencional_text,
            "credito_convencional": money_to_int(credito_convencional_text),
            "todo_medio_pago_text": todo_medio_pago_text,
            "todo_medio_pago": money_to_int(todo_medio_pago_text),
        }
        rows.append(row)

    # De-dup por version_id o por (version_name, desde)
    dedup = []
    seen = set()
    for r in rows:
        key = r.get("version_id") or (r.get("version_name"), r.get("desde"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    return dedup


async def scrape():
    all_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="es-CL",
        )
        page = await context.new_page()

        print(f"[INFO] Abriendo listado: {START_URL}")
        models = await get_models_from_listing(page)
        print(f"[INFO] Modelos encontrados: {len(models)}")

        for idx, m in enumerate(models, start=1):
            model_url = m["model_url"]
            model_slug = slug_from_model_url(model_url)
            modelo = slug_to_model(model_slug) or m.get("model_name") or model_slug

            retries = 3
            last_err = None
            versions = []

            for attempt in range(1, retries + 1):
                try:
                    print(f"[INFO] Modelo: {model_url} (intento {attempt}/{retries})")
                    versions = await extract_versions_from_model(page, model_url)
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    print(f"[WARN] fallo en {model_url}: {repr(e)}")
                    # pequeño backoff
                    await asyncio.sleep(1.2 * attempt)

            if last_err:
                print(f"[ERR] No se pudo extraer {model_url} -> {repr(last_err)}")
                versions = []

            # Armar JSON final por versión: marca, modelo, version
            for v in versions:
                row = {
                    "marca": BRAND,
                    "modelo": modelo,
                    "version": v.get("version_name"),

                    "desde_text": v.get("desde_text"),
                    "desde": v.get("desde"),

                    "credito_inteligente_text": v.get("credito_inteligente_text"),
                    "credito_inteligente": v.get("credito_inteligente"),

                    "credito_convencional_text": v.get("credito_convencional_text"),
                    "credito_convencional": v.get("credito_convencional"),

                    "todo_medio_pago_text": v.get("todo_medio_pago_text"),
                    "todo_medio_pago": v.get("todo_medio_pago"),

                    "version_id": v.get("version_id"),
                    "ver_version_url": v.get("ver_version_url"),
                    "cotizar_url": v.get("cotizar_url"),
                    "reservar_url": v.get("reservar_url"),

                    # extras útiles
                    "model_url": model_url,
                    "model_slug": model_slug,
                }
                all_rows.append(row)

            print(f"[INFO] {idx}/{len(models)} -> versiones: {len(versions)}")

        await browser.close()

    return all_rows


def main():
    rows = asyncio.run(scrape())

    out_file = "coseche_chevrolet_versions.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    
    for r in rows:
        print(r)
        tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
        precios = [
            r['credito_inteligente'],
            r['credito_convencional'],
            r['todo_medio_pago'],
            r['todo_medio_pago'],
        ]
        datos = {
            'modelo': to_title_custom(r.get('modelo')),
            'marca': to_title_custom(r.get('marca')),
            'modelDetail': to_title_custom(r.get('version')),
            'tiposprecio': tiposprecio,
            'precio': precios
        }
        print(datos)
        saveCar("Chevrolet",datos,'www.coseche.com')
    print(f"[OK] Guardado: {out_file} | filas: {len(rows)}")


if __name__ == "__main__":
    main()
