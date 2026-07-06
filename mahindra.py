import json
import re
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils import saveCar

DETAIL_URL = "https://www.mahindra.cl/modelos/suv/xuv-3xo/#versiones"
BASE_URL = "https://www.mahindra.cl"


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

    try:
        return int(match.group(1).replace(".", ""))
    except ValueError:
        return 0


def extraer_solo_monto(texto):
    """
    Convierte:
    'Desde: $9.990.000*'
    en:
    '$9.990.000'
    """
    if not texto:
        return ""

    match = re.search(r"\$\s*[\d\.]+", texto)
    if not match:
        return ""

    return match.group(0).replace("$ ", "$")


def obtener_texto(locator):
    try:
        if locator.count() > 0:
            return limpiar_texto(locator.first.inner_text())
    except Exception:
        pass
    return ""


def extraer_datos_precio(card):
    precio_desde_texto = ""
    precio_lista_texto = ""
    bono_financiamiento_texto = ""
    bono_directo_texto = ""

    spans = card.locator("span.card__price-info")
    total = spans.count()

    for i in range(total):
        texto = limpiar_texto(spans.nth(i).inner_text())
        texto_lower = texto.lower()

        if "desde:" in texto_lower:
            precio_desde_texto = texto
        elif "precio lista" in texto_lower:
            precio_lista_texto = texto
        elif "bono financiamiento" in texto_lower:
            bono_financiamiento_texto = texto
        elif "bono directo" in texto_lower:
            bono_directo_texto = texto

    return {
        "precio_desde_texto": extraer_solo_monto(precio_desde_texto),
        "precio_desde": normalizar_monto(precio_desde_texto),
        "precio_lista": normalizar_monto(precio_lista_texto),
        "bono_directo": normalizar_monto(bono_directo_texto),
        "bono_financiamiento": normalizar_monto(bono_financiamiento_texto),
    }


def scrapear_versiones_mahindra(page):
    page.goto(DETAIL_URL, wait_until="load", timeout=60000)

    # Esperar la sección real en vez de usar networkidle
    try:
        page.wait_for_selector("article.card-model-version-container", state="visible", timeout=30000)
    except PlaywrightTimeoutError:
        # respaldo por si la sección está adjunta pero no visible aún
        page.wait_for_timeout(3000)
        page.wait_for_selector("article.card-model-version-container", state="attached", timeout=30000)

    cards = page.locator("article.card-model-version-container")
    total = cards.count()

    resultados = []

    for i in range(total):
        card = cards.nth(i)

        # Solo cards de versiones reales con precio "Desde"
        if card.locator("span.card__price-info.card__price-info-from").count() == 0:
            continue

        version = obtener_texto(card.locator("h3.card__title"))
        if not version:
            continue

        precios = extraer_datos_precio(card)

        cotizar_url = None
        cotizar_link = card.locator("a.card__link.button--primary")
        if cotizar_link.count() > 0:
            href = cotizar_link.first.get_attribute("href")
            if href:
                cotizar_url = urljoin(BASE_URL, href)

        resultados.append({
            "brand": "MAHINDRA",
            "model": "XUV 3XO",
            "version": version,
            "precio_desde_texto": precios["precio_desde_texto"],
            "precio_desde": precios["precio_desde"],
            "precio_lista": precios["precio_lista"],
            "bono_directo": precios["bono_directo"],
            "bono_financiamiento": precios["bono_financiamiento"],
            "cotizar_url": cotizar_url,
            "modelo_filtro": "MAHINDRA XUV 3XO"
        })

    # deduplicación por seguridad
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


if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox"
            ]
        )

        page = browser.new_page(viewport={"width": 1440, "height": 2200})

        data = scrapear_versiones_mahindra(page)

        print(f"Total versiones encontradas: {len(data)}")
        for item in data:
            print(
                f"{item['version']} | "
                f"{item['precio_desde_texto']} | "
                f"Lista: {item['precio_lista']} | "
                f"Bono fin.: {item['bono_financiamiento']}"
            )

        with open("mahindra_xuv3xo_versiones.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        for r in data:
            tiposprecio = [
                'Crédito inteligente',
                'Crédito convencional',
                'Todo medio de pago',
                'Precio de lista'
            ]
            precio = [
                r['precio_lista'] - r['bono_financiamiento'],
                r['precio_lista'] - r['bono_directo'],
                r["precio_lista"],
                r["precio_lista"]
            ]
            datos = {
                'marca': r['brand'],
                'modelo': r['model'],
                'modelDetail': r['version'],
                'tiposprecio': tiposprecio,
                'precio': precio
            }
            print(datos)
            saveCar('Mahindra', datos, 'www.mahindra.cl')

        print("\nArchivo guardado: mahindra_xuv3xo_versiones.json")
        browser.close()