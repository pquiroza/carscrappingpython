import json
import re
import os
import sys
import traceback
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils import saveCar

DETAIL_URL = "https://www.mahindra.cl/modelos/suv/xuv-3xo/#versiones"
BASE_URL = "https://www.mahindra.cl"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"


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


def safe_subtract(a, b):
    if a in (None, 0):
        return None if a is None else a
    return a - (b or 0)


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

    try:
        page.wait_for_selector("article.card-model-version-container", state="visible", timeout=30000)
    except PlaywrightTimeoutError:
        page.wait_for_timeout(3000)
        page.wait_for_selector("article.card-model-version-container", state="attached", timeout=30000)

    cards = page.locator("article.card-model-version-container")
    total = cards.count()

    resultados = []

    for i in range(total):
        try:
            card = cards.nth(i)

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

        except Exception as e:
            resultados.append({
                "_error": str(e),
                "brand": "MAHINDRA",
                "model": "XUV 3XO",
                "modelo_filtro": "MAHINDRA XUV 3XO"
            })

    unicos = []
    vistos = set()

    for item in resultados:
        if item.get("_error"):
            unicos.append(item)
            continue

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


def main():
    stats = {
        "models_found": 1,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    browser = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox"
                ]
            )

            page = browser.new_page(viewport={"width": 1440, "height": 2200})
            page.set_default_timeout(60000)

            data = scrapear_versiones_mahindra(page)
            stats["models_processed"] = 1
            stats["versions_found"] = len([x for x in data if not x.get("_error")])
            stats["version_errors"] = len([x for x in data if x.get("_error")])

            print(f"Total versiones encontradas: {len(data)}")
            for item in data:
                if item.get("_error"):
                    print(f"[WARN] fila con error: {item['_error']}")
                    continue

                print(
                    f"{item['version']} | "
                    f"{item['precio_desde_texto']} | "
                    f"Lista: {item['precio_lista']} | "
                    f"Bono fin.: {item['bono_financiamiento']}"
                )

            with open("mahindra_xuv3xo_versiones.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            for r in data:
                try:
                    if r.get("_error"):
                        stats["save_errors"] += 1
                        continue

                    tiposprecio = [
                        'Crédito inteligente',
                        'Crédito convencional',
                        'Todo medio de pago',
                        'Precio de lista'
                    ]

                    precio = [
                        safe_subtract(r.get('precio_lista'), r.get('bono_financiamiento')),
                        safe_subtract(r.get('precio_lista'), r.get('bono_directo')),
                        r.get("precio_lista"),
                        r.get("precio_lista")
                    ]

                    datos = {
                        'marca': r.get('brand'),
                        'modelo': r.get('model'),
                        'modelDetail': r.get('version'),
                        'tiposprecio': tiposprecio,
                        'precio': precio
                    }

                    if datos["marca"] and datos["modelo"] and datos["modelDetail"]:
                        print(datos)
                        saveCar('Mahindra', datos, 'www.mahindra.cl')
                        stats["saved_ok"] += 1
                    else:
                        stats["save_errors"] += 1

                except Exception as e:
                    stats["save_errors"] += 1
                    print(f"[ERROR] saveCar falló para fila {r}: {e}")
                    traceback.print_exc()

    except PlaywrightTimeoutError as e:
        stats["model_errors"] += 1
        print(f"[FATAL] Timeout general en Mahindra: {e}")
        traceback.print_exc()
        summary = {
            "status": "error",
            "source": "www.mahindra.cl",
            **stats
        }
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    except Exception as e:
        stats["model_errors"] += 1
        print(f"[FATAL] {e}")
        traceback.print_exc()
        summary = {
            "status": "error",
            "source": "www.mahindra.cl",
            **stats
        }
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(1)

    finally:
        if browser:
            try:
                browser.close()
            except Exception:
                pass

    summary = {
        "status": "success",
        "source": "www.mahindra.cl",
        **stats
    }

    if stats["models_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron modelos")
        sys.exit(1)

    if stats["models_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar el modelo")
        sys.exit(1)

    if stats["versions_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron versiones")
        sys.exit(1)

    if stats["saved_ok"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se guardó ningún registro en Firebase")
        sys.exit(1)

    if stats["models_found"] > 0:
        error_ratio = stats["model_errors"] / stats["models_found"]
        summary["error_ratio"] = round(error_ratio, 4)

        if error_ratio >= 0.5:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print(f"[ERROR] Demasiados errores de modelo: {stats['model_errors']} de {stats['models_found']}")
            sys.exit(1)

    print("\nArchivo guardado: mahindra_xuv3xo_versiones.json")
    print("RUN_OK")
    print(json.dumps(summary, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()