import json
import re
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils import saveCar
from utils import saveCarDate

BASE_URL = "https://www.jacautoschile.cl/modelos/"


def limpiar_texto(texto):
    if not texto:
        return ""
    return re.sub(r"\s+", " ", texto).strip()


def normalizar_monto(texto):
    if not texto:
        return 0

    match = re.search(r"\$\s*([\d\.]+)", texto)
    if not match:
        return 0

    numero = match.group(1).replace(".", "")
    try:
        return int(numero)
    except ValueError:
        return 0


def formatear_precio_texto_solo_monto(texto):
    """
    Convierte:
    'Desde: $41.490.000 + IVA*'
    en:
    '$41.490.000'
    """
    if not texto:
        return ""

    match = re.search(r"\$\s*[\d\.]+", texto)
    if not match:
        return ""

    return match.group(0).replace("$ ", "$")


def extraer_cards_listado(page):
    page.wait_for_selector("article.card--car", timeout=20000)

    cards = page.locator("article.card--car")
    total = cards.count()
    resultados = []

    for i in range(total):
        card = cards.nth(i)

        nombre = ""
        categoria = ""
        precio_texto = ""
        url_modelo = ""

        title_locator = card.locator("h3.card__title")
        if title_locator.count() > 0:
            nombre = limpiar_texto(title_locator.first.inner_text())

        categoria_locator = card.locator(".card__category")
        if categoria_locator.count() > 0:
            categoria = limpiar_texto(categoria_locator.first.inner_text())

        precio_locator = card.locator(".card__price-info")
        if precio_locator.count() > 0:
            precio_texto = limpiar_texto(precio_locator.first.inner_text())

        modelo_link = card.locator("a.card__model-link")
        if modelo_link.count() > 0:
            href = modelo_link.first.get_attribute("href")
            if href:
                url_modelo = urljoin(BASE_URL, href)

        resultados.append({
            "nombre": nombre,
            "categoria": categoria,
            "precio_listado_texto": precio_texto,
            "precio_listado": normalizar_monto(precio_texto),
            "url_modelo": url_modelo,
        })

    return resultados


def ir_a_pagina_listado(page, numero_pagina):
    page.wait_for_selector("ul.pagination", timeout=15000)

    antes = page.locator("article.card--car h3.card__title").all_inner_texts()
    antes = [limpiar_texto(x) for x in antes]

    link_pagina = page.locator("ul.pagination a.page-link").filter(has_text=str(numero_pagina))

    if link_pagina.count() == 0:
        raise Exception(f"No se encontró el botón de la página {numero_pagina}")

    link_pagina.first.click(force=True)

    page.wait_for_function(
        """(params) => {
            const pagina = params.pagina;
            const anteriores = params.anteriores;

            const active = document.querySelector('ul.pagination li.page-item.active a.page-link');
            const titulos = Array.from(document.querySelectorAll('article.card--car h3.card__title'))
                .map(x => (x.textContent || '').trim());

            const activoOk = active && active.textContent.trim() === String(pagina);
            const cambioListado = JSON.stringify(titulos) !== JSON.stringify(anteriores);

            return activoOk || cambioListado;
        }""",
        arg={
            "pagina": numero_pagina,
            "anteriores": antes
        },
        timeout=20000
    )

    page.wait_for_timeout(1500)


def listar_modelos_jac(page):
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("networkidle")

    todos = []

    pagina_1 = extraer_cards_listado(page)
    todos.extend(pagina_1)

    ir_a_pagina_listado(page, 2)
    pagina_2 = extraer_cards_listado(page)
    todos.extend(pagina_2)

    return todos


def obtener_texto(locator):
    try:
        if locator.count() > 0:
            return limpiar_texto(locator.first.inner_text())
    except Exception:
        pass
    return ""


def extraer_precio_desde_version_card(version_card):
    precio_desde_texto = ""
    precio_lista_texto = ""
    bono_directo_texto = ""
    bono_financiamiento_texto = ""

    spans = version_card.locator("span.card__price-info")
    total = spans.count()

    for i in range(total):
        texto = limpiar_texto(spans.nth(i).inner_text())
        texto_lower = texto.lower()

        if "desde:" in texto_lower:
            precio_desde_texto = texto
        elif "precio lista" in texto_lower:
            precio_lista_texto = texto
        elif "bono directo" in texto_lower:
            bono_directo_texto = texto
        elif "bono financiamiento" in texto_lower:
            bono_financiamiento_texto = texto

    return {
        "precio_desde_texto_completo": precio_desde_texto,
        "precio_desde_texto": formatear_precio_texto_solo_monto(precio_desde_texto),
        "precio_desde": normalizar_monto(precio_desde_texto),
        "precio_lista": normalizar_monto(precio_lista_texto),
        "bono_directo": normalizar_monto(bono_directo_texto),
        "bono_financiamiento": normalizar_monto(bono_financiamiento_texto),
    }


def extraer_versiones_modelo(page, brand, model, modelo_filtro):
    """
    SOLO toma versiones desde:
    article.card-model-version-container
    """
    page.wait_for_selector("article.card-model-version-container", timeout=20000)

    cards = page.locator("article.card-model-version-container")
    total = cards.count()
    resultados = []

    for i in range(total):
        card = cards.nth(i)

        if card.locator("span.card__price-info.card__price-info-from").count() == 0:
            continue

        version = obtener_texto(card.locator("h3.card__title"))
        if not version:
            continue

        precios = extraer_precio_desde_version_card(card)

        cotizar_url = None
        cotizar_link = card.locator("a.card__link.button--primary")
        if cotizar_link.count() > 0:
            href = cotizar_link.first.get_attribute("href")
            if href:
                cotizar_url = urljoin(BASE_URL, href)

        resultados.append({
            "brand": brand,
            "model": model,
            "version": version,
            "precio_desde_texto": precios["precio_desde_texto"],
            "precio_desde": precios["precio_desde"],
            "precio_lista": precios["precio_lista"],
            "bono_directo": precios["bono_directo"],
            "bono_financiamiento": precios["bono_financiamiento"],
            "cotizar_url": cotizar_url,
            "modelo_filtro": modelo_filtro
        })

    # deduplicación
    unicos = []
    vistos = set()

    for item in resultados:
        key = (
            item["brand"].strip().lower(),
            item["model"].strip().lower(),
            item["version"].strip().lower(),
            item["precio_desde"],
            item["precio_lista"]
        )
        if key not in vistos:
            vistos.add(key)
            unicos.append(item)

    return unicos


def extraer_detalle_modelo(page, modelo):
    url_modelo = modelo["url_modelo"]
    model = modelo["nombre"]
    brand = "JAC"
    modelo_filtro = f"{brand} {model}"

    page.goto(url_modelo, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    try:
        versiones = extraer_versiones_modelo(page, brand, model, modelo_filtro)
        return versiones
    except Exception as e:
        print(f"No se pudieron extraer versiones en {url_modelo}: {e}")
        return []


def scrapear_jac_versiones(headless=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            locale="es-CL"
        )

        page_listado = context.new_page()
        modelos = listar_modelos_jac(page_listado)

        page_detalle = context.new_page()

        todas_las_versiones = []

        for idx, modelo in enumerate(modelos, start=1):
            nombre = modelo.get("nombre", "")
            url_modelo = modelo.get("url_modelo", "")

            print(f"[{idx}/{len(modelos)}] Procesando modelo: {nombre} -> {url_modelo}")

            if not url_modelo:
                continue

            try:
                versiones = extraer_detalle_modelo(page_detalle, modelo)
                todas_las_versiones.extend(versiones)
            except PlaywrightTimeoutError as e:
                print(f"Timeout en {nombre}: {e}")
            except Exception as e:
                print(f"Error en {nombre}: {e}")

        browser.close()
        return todas_las_versiones


if __name__ == "__main__":
    data = scrapear_jac_versiones(headless=False)

    with open("jac_versiones.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        for r in data:
            tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
            precio = [r['precio_lista']-r['bono_financiamiento'],r['precio_lista']-r['bono_directo'], r["precio_lista"], r["precio_lista"]]
            datos = {
                'marca': r['brand'],
                'modelo': r['model'],
                'modelDetail': r['version'],
                'tiposprecio': tiposprecio,
                'precio': precio
            }
            print(datos)
            saveCar('JAC',datos,'www.jacautoschile.cl')

    print("\nArchivo guardado: jac_versiones.json")