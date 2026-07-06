from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urljoin
import re
import json
from utils import saveCar
from utils import to_title_custom

BASE_URL = "https://www.lynkco.cl"
BRAND = "LYNK & CO"


def limpiar_monto(texto):
    if not texto:
        return None
    solo_numeros = re.sub(r"[^\d]", "", str(texto))
    return int(solo_numeros) if solo_numeros else None


def ir_a_pagina(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2500)


def normalizar_modelo_texto(nombre):
    nombre = (nombre or "").upper().strip()
    nombre = nombre.replace("LYNK & CO", "").strip()
    return nombre


def modelo_filtro(brand, model):
    return f"{brand} {model}".upper().strip()


def obtener_modelos(page, url_base):
    ir_a_pagina(page, url_base)

    enlaces = page.locator("a[href]")
    total = enlaces.count()

    vistos = set()
    modelos = []

    for i in range(total):
        a = enlaces.nth(i)
        href = a.get_attribute("href")
        texto = a.inner_text().strip() if a.count() else ""

        if not href:
            continue

        href_lower = href.lower().strip()
        if href_lower not in ["/lynkco06", "/lynkco08", "/lynkco09"]:
            continue

        url_modelo = urljoin(url_base, href)
        if url_modelo in vistos:
            continue

        vistos.add(url_modelo)

        model = normalizar_modelo_texto(texto)
        if not model:
            if href_lower == "/lynkco06":
                model = "06"
            elif href_lower == "/lynkco08":
                model = "08"
            elif href_lower == "/lynkco09":
                model = "09"

        modelos.append({
            "model": model,
            "url": url_modelo
        })

    modelos.sort(key=lambda x: x["url"])
    return modelos


def obtener_versiones(page, url_modelo):
    ir_a_pagina(page, url_modelo)
    page.wait_for_selector("div.centro-abs div.btn_version", timeout=20000)

    botones = page.locator("div.centro-abs div.btn_version")
    total = botones.count()

    versiones = []
    for i in range(total):
        boton = botones.nth(i)
        nombre = boton.locator("span").inner_text().strip()
        versiones.append(nombre)

    return versiones


def esperar_version_activa(page, version_texto):
    page.wait_for_function(
        """
        (versionEsperada) => {
            const botones = [...document.querySelectorAll('div.centro-abs div.btn_version')];
            return botones.some(btn => {
                const span = btn.querySelector('span');
                const texto = span ? span.textContent.trim() : '';
                return btn.classList.contains('active') && texto === versionEsperada;
            });
        }
        """,
        arg=version_texto,
        timeout=10000
    )


def click_version(page, version_texto):
    botones = page.locator("div.centro-abs div.btn_version")
    total = botones.count()

    encontrado = False

    for i in range(total):
        boton = botones.nth(i)
        span = boton.locator("span")
        texto = span.inner_text().strip()

        if texto != version_texto:
            continue

        boton.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        # intento 1
        try:
            boton.click(force=True, timeout=5000)
            encontrado = True
        except Exception:
            pass

        # intento 2
        if not encontrado:
            try:
                span.click(force=True, timeout=5000)
                encontrado = True
            except Exception:
                pass

        # intento 3
        if not encontrado:
            try:
                page.evaluate(
                    """
                    (versionEsperada) => {
                        const botones = [...document.querySelectorAll('div.centro-abs div.btn_version')];
                        const btn = botones.find(b => {
                            const span = b.querySelector('span');
                            return span && span.textContent.trim() === versionEsperada;
                        });
                        if (!btn) throw new Error("No se encontró la versión: " + versionEsperada);
                        btn.click();
                    }
                    """,
                    version_texto
                )
                encontrado = True
            except Exception:
                pass

        if not encontrado:
            raise RuntimeError(f"No se pudo hacer click en la versión {version_texto}")

        esperar_version_activa(page, version_texto)
        page.wait_for_timeout(1800)
        return

    raise RuntimeError(f"No se encontró la versión {version_texto}")


def extraer_bloque_precio(page):
    """
    Busca el bloque que contiene 'Precio desde'.
    """
    bloques = page.locator("div.col-6.col-md-4")
    total = bloques.count()

    for i in range(total):
        bloque = bloques.nth(i)
        texto = bloque.inner_text().strip()

        if "Precio desde" in texto:
            return bloque

    # fallback más abierto
    bloques = page.locator("div")
    total = min(bloques.count(), 300)

    for i in range(total):
        bloque = bloques.nth(i)
        try:
            texto = bloque.inner_text().strip()
        except Exception:
            continue

        if "Precio desde" in texto and "$" in texto:
            return bloque

    return None


def extraer_info_precio(page):
    bloque = extraer_bloque_precio(page)

    if bloque is None:
        return {
            "precio_desde_texto": None,
            "precio_desde": None,
            "precio_lista": None,
            "bono_directo": None,
            "bono_financiamiento": None
        }

    texto_bloque = bloque.inner_text().strip()

    # Precio desde
    precio_desde_texto = None
    p_tags = bloque.locator("p")
    for i in range(p_tags.count()):
        txt = p_tags.nth(i).inner_text().strip()
        if "$" in txt:
            precio_desde_texto = txt
            break

    if not precio_desde_texto:
        m = re.search(r"\$[\d\.\,]+", texto_bloque)
        if m:
            precio_desde_texto = m.group(0)

    precio_desde = limpiar_monto(precio_desde_texto)

    # Descuentos
    bono_directo = 0
    bono_financiamiento = 0

    lineas = bloque.locator("div.lh-120")
    for i in range(lineas.count()):
        item = lineas.nth(i)
        clase = (item.get_attribute("class") or "").lower()
        if "d-none" in clase:
            continue

        strong = item.locator("strong")
        span = item.locator("span")

        nombre = strong.first.inner_text().strip() if strong.count() > 0 else ""
        valor = span.first.inner_text().strip() if span.count() > 0 else ""

        nombre_lower = nombre.replace(":", "").strip().lower()
        monto = limpiar_monto(valor)

        if monto is None:
            monto = 0

        if "marca" in nombre_lower:
            bono_directo = monto
        elif "credi" in nombre_lower or "financ" in nombre_lower or "tattersall" in nombre_lower:
            bono_financiamiento = monto

    precio_lista = None
    if precio_desde is not None:
        precio_lista = precio_desde + bono_directo + bono_financiamiento

    return {
        "precio_desde_texto": precio_desde_texto,
        "precio_desde": precio_desde,
        "precio_lista": precio_lista,
        "bono_directo": bono_directo,
        "bono_financiamiento": bono_financiamiento
    }


def obtener_cotizar_url(page):
    links = page.locator("a[href*='cotizar']")
    total = links.count()

    for i in range(total):
        href = links.nth(i).get_attribute("href")
        if href:
            return urljoin(BASE_URL, href)

    return None


def obtener_versiones_y_precios(page, url_modelo, brand, model):
    versiones = obtener_versiones(page, url_modelo)
    resultados = []

    for version in versiones:
        ir_a_pagina(page, url_modelo)
        page.wait_for_selector("div.centro-abs div.btn_version", timeout=20000)

        click_version(page, version)
        info = extraer_info_precio(page)
        cotizar_url = obtener_cotizar_url(page)

        print({
            "model": model,
            "version": version,
            "precio_desde_texto": info["precio_desde_texto"],
            "precio_desde": info["precio_desde"],
            "precio_lista": info["precio_lista"],
            "bono_directo": info["bono_directo"],
            "bono_financiamiento": info["bono_financiamiento"]
        })

        resultados.append({
            "brand": brand,
            "model": model,
            "version": version.upper().strip(),
            "precio_desde_texto": info["precio_desde_texto"],
            "precio_desde": info["precio_desde"],
            "precio_lista": info["precio_lista"],
            "bono_directo": info["bono_directo"],
            "bono_financiamiento": info["bono_financiamiento"],
            "cotizar_url": cotizar_url,
            "modelo_filtro": modelo_filtro(brand, model)
        })

    return resultados


def scrap_lynkco():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            slow_mo=400
        )

        page = browser.new_page(
            viewport={"width": 1440, "height": 2200}
        )
        page.set_default_timeout(60000)

        try:
            modelos = obtener_modelos(page, BASE_URL)
        except PlaywrightTimeoutError as e:
            browser.close()
            raise RuntimeError(f"No se pudieron obtener los modelos. Detalle: {e}")

        if not modelos:
            browser.close()
            raise RuntimeError("No se encontraron modelos en lynkco.cl")

        print("Modelos encontrados:")
        for m in modelos:
            print(f" - {m['model']} -> {m['url']}")

        resultado_final = []

        for item_modelo in modelos:
            model = item_modelo["model"]
            url_modelo = item_modelo["url"]

            try:
                registros = obtener_versiones_y_precios(
                    page=page,
                    url_modelo=url_modelo,
                    brand=BRAND,
                    model=model
                )
                resultado_final.extend(registros)
            except Exception as e:
                print(f"Error procesando modelo {model} ({url_modelo}): {e}")

        browser.close()
        return resultado_final


if __name__ == "__main__":
    data = scrap_lynkco()

    with open("lynkco_modelos_versiones_precios.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        for a in data:
            print("-"*100)
            tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
            precio = [a["precio_desde"], a["precio_lista"]-a["bono_directo"], a["precio_lista"]-a["bono_directo"], a["precio_lista"]]
            datos = {
                "marca": to_title_custom(a["brand"]),
                "modelo": a["model"],
                "modelDetail": a["version"],
               'tiposprecio': tiposprecio,
                 'precio': precio
            }
            print(datos)
            saveCar("Lynk & Co",datos,'www.lynkco.cl')

    print(json.dumps(data, ensure_ascii=False, indent=2))
    print("\nArchivo guardado: lynkco_modelos_versiones_precios.json")