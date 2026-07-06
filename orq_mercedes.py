import asyncio
import json
import re
import os
import sys
import traceback
from urllib.parse import urljoin
from playwright.async_api import async_playwright
from utils import saveCar
from utils import to_title_custom

LIST_URL = "https://www.kaufmann.cl/automoviles/mercedes-benz/nuestros-vehiculos"
BRAND = "Mercedez Benz"
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"


# -------------------------
# Helpers parseo precios
# -------------------------
def parse_clp(text: str | None):
    if not text:
        return None
    m = re.search(r"\$\s*([\d\.]+)", text)
    if not m:
        return None
    return int(m.group(1).replace(".", ""))


def parse_usd(text: str | None):
    if not text:
        return None
    m = re.search(r"USD\s*([\d\.]+)", text, re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1).replace(".", ""))


# -------------------------
# Listado de modelos
# -------------------------
async def extraer_modelos_desde_listado(page):
    await page.goto(LIST_URL, wait_until="domcontentloaded")
    await page.wait_for_selector("div.modelCar", timeout=20000)

    modelos = await page.evaluate(
        """
    () => {
      const out = [];
      document.querySelectorAll("div.identificador-auto").forEach(cat => {
        const category = cat.getAttribute("data-category");

        cat.querySelectorAll("div.modelCar").forEach(card => {
          const model_id = card.getAttribute("data-modelid");
          const electrico = card.getAttribute("data-electrico") === "si";

          const h3 = card.querySelector("h3");
          const model = h3 ? h3.textContent.replace(/\\s+/g, " ").trim() : null;

          const link = card.querySelector("a.btn-card");
          const href = link ? link.getAttribute("href") : null;
          if (!href) return;

          out.push({ category, model, model_id, electrico, href });
        });
      });
      return out;
    }
    """
    )

    base = page.url
    for m in modelos:
        m["brand"] = BRAND
        m["detalle_url"] = urljoin(base, m.pop("href"))

    return modelos


# -------------------------
# Versiones por modelo
# -------------------------
async def extraer_versiones(page):
    try:
        await page.wait_for_selector("#contentSelector", state="attached", timeout=7000)
    except Exception:
        return []

    select = page.locator("#contentSelector").first
    await select.scroll_into_view_if_needed()

    try:
        await page.wait_for_selector(
            "#contentSelector option", state="attached", timeout=7000
        )
    except Exception:
        return []

    options = page.locator("#contentSelector option")
    count = await options.count()
    if count == 0:
        return []

    base = page.url
    versiones = []
    for i in range(count):
        opt = options.nth(i)
        version_txt = (await opt.inner_text()).strip()
        value = await opt.get_attribute("value")
        ficha = await opt.get_attribute("data-ficha")

        versiones.append(
            {
                "version": " ".join(version_txt.split()),
                "value": value,
                "ficha_url": urljoin(base, ficha) if ficha else None,
            }
        )

    return versiones


async def seleccionar_version(page, value: str):
    prev_href = None
    if await page.locator("#btnDescargarFicha").count() > 0:
        prev_href = await page.locator("#btnDescargarFicha").get_attribute("href")

    await page.select_option("#contentSelector", value=value)

    if prev_href is not None:
        try:
            await page.wait_for_function(
                """(prev) => {
                    const a = document.querySelector('#btnDescargarFicha');
                    return a && a.getAttribute('href') && a.getAttribute('href') !== prev;
                }""",
                prev_href,
                timeout=7000,
            )
        except Exception:
            await page.wait_for_timeout(600)
    else:
        await page.wait_for_timeout(600)


# -------------------------
# Precios por versión (accordion Precio)
# -------------------------
async def abrir_precio_accordion_si_corresponde(page):
    precio_btn = page.locator(
        ".accordion-item:has(.accordion-header:has-text('Precio')) .accordion-header"
    ).first

    if await precio_btn.count() == 0:
        return False

    try:
        await precio_btn.scroll_into_view_if_needed(timeout=5000)
        await page.wait_for_timeout(150)
    except Exception:
        pass

    cls = (await precio_btn.get_attribute("class")) or ""
    if "active" in cls:
        return True

    try:
        await precio_btn.click(force=True, timeout=5000)
        await page.wait_for_timeout(250)
    except Exception:
        pass

    cls = (await precio_btn.get_attribute("class")) or ""
    if "active" in cls:
        return True

    try:
        await page.evaluate(
            """
        () => {
          const all = Array.from(document.querySelectorAll(".accordion-item .accordion-header"));
          const precio = all.find(b => (b.textContent || "").toLowerCase().includes("precio"));
          if (precio) precio.click();
        }
        """
        )
        await page.wait_for_timeout(300)
    except Exception:
        pass

    cls = (await precio_btn.get_attribute("class")) or ""
    return "active" in cls


async def extraer_precios_version_actual(page):
    await abrir_precio_accordion_si_corresponde(page)

    try:
        await page.locator(".caracteristicas_body--content, .accordion").first.scroll_into_view_if_needed(timeout=5000)
        await page.wait_for_timeout(250)
    except Exception:
        pass

    try:
        await page.wait_for_selector(
            ".accordion-item:has(.accordion-header:has-text('Precio')) "
            ".caracteristicas-accordion_body__item",
            state="attached",
            timeout=7000,
        )
    except Exception:
        return {
            "precio_desde_texto": None,
            "precio_desde": None,
            "precio_lista_texto": None,
            "precio_lista_usd": None,
        }

    items = page.locator(
        ".accordion-item:has(.accordion-header:has-text('Precio')) "
        ".caracteristicas-accordion_body__item"
    )
    n = await items.count()

    precio_desde_texto = None
    precio_lista_texto = None

    for i in range(n):
        it = items.nth(i)

        titulo = await it.locator(".caracteristicas-accordion_body__item-titulo").first.inner_text()
        titulo = " ".join((titulo or "").split()).strip()

        label = await it.locator(".caracteristicas-accordion_body__item-text").first.inner_text()
        label = " ".join((label or "").split()).strip().lower()

        if "oportunidad" in label and "desde" in label:
            precio_desde_texto = titulo
        elif "precio lista" in label:
            precio_lista_texto = titulo

    return {
        "precio_desde_texto": precio_desde_texto,
        "precio_desde": parse_clp(precio_desde_texto),
        "precio_lista_texto": precio_lista_texto,
        "precio_lista_usd": parse_usd(precio_lista_texto),
    }


# -------------------------
# Main scrape
# -------------------------
async def scrape():
    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        print("Abriendo listado:", LIST_URL)
        modelos = await extraer_modelos_desde_listado(page)
        stats["models_found"] = len(modelos)
        print("Modelos encontrados:", len(modelos))

        resultados = []

        for i, m in enumerate(modelos, start=1):
            print(f"[{i}/{len(modelos)}] {m['model']} -> {m['detalle_url']}")

            try:
                await page.goto(m["detalle_url"], wait_until="domcontentloaded")
                await page.wait_for_timeout(700)

                versiones = await extraer_versiones(page)
                print(f"   versiones detectadas: {len(versiones)}")

                if not versiones:
                    precio_base = await extraer_precios_version_actual(page)
                    resultados.append({**m, "versions": [], "precio_base": precio_base})
                    stats["models_processed"] += 1
                    continue

                for v in versiones:
                    if not v.get("value"):
                        v["precios"] = {
                            "precio_desde_texto": None,
                            "precio_desde": None,
                            "precio_lista_texto": None,
                            "precio_lista_usd": None,
                        }
                        continue

                    await seleccionar_version(page, v["value"])

                    try:
                        await page.locator(".caracteristicas-select__right, .caracteristicas_body--content, .accordion").first.scroll_into_view_if_needed(timeout=5000)
                        await page.wait_for_timeout(200)
                    except Exception:
                        pass

                    v["precios"] = await extraer_precios_version_actual(page)

                resultados.append({**m, "versions": versiones})
                stats["models_processed"] += 1
                stats["versions_found"] += len(versiones)

            except Exception as e:
                stats["model_errors"] += 1
                print(f"   ❌ error: {e}")

        out_file = "mercedes_modelos_versiones_precios.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)

        await browser.close()

    return resultados, stats


def main():
    try:
        resultados, stats = asyncio.run(scrape())

        for r in resultados:
            for vs in r.get("versions", []):
                try:
                    precio_desde = vs.get("precios", {}).get("precio_desde")
                    if precio_desde is None:
                        stats["save_errors"] += 1
                        continue

                    tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
                    precio = [precio_desde, precio_desde, precio_desde, precio_desde]

                    datos = {
                        'modelo': vs.get('version'),
                        'marca': to_title_custom(BRAND),
                        'modelDetail': vs.get('version'),
                        'tiposprecio': tiposprecio,
                        'precio': precio
                    }

                    print(datos)
                    saveCar('Mercedes Benz', datos, 'https://www.kaufmann.cl/automoviles/mercedes-benz/nuestros-vehiculos')
                    stats["saved_ok"] += 1

                except Exception as e:
                    stats["save_errors"] += 1
                    print(f"[ERROR] saveCar falló: {e}")
                    traceback.print_exc()

        summary = {
            "status": "success",
            "source": "https://www.kaufmann.cl/automoviles/mercedes-benz/nuestros-vehiculos",
            **stats
        }

        if stats["models_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            sys.exit(1)

        if stats["models_processed"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            sys.exit(1)

        if stats["saved_ok"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            sys.exit(1)

        if stats["models_found"] > 0:
            error_ratio = stats["model_errors"] / stats["models_found"]
            summary["error_ratio"] = round(error_ratio, 4)

            if error_ratio >= 0.5:
                summary["status"] = "error"
                print(json.dumps(summary, ensure_ascii=False))
                sys.exit(1)

        print("✔ Listo: mercedes_modelos_versiones_precios.json")
        print("RUN_OK")
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()