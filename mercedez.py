import asyncio
import json
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright
from utils import saveCar
from utils import to_title_custom
LIST_URL = "https://www.kaufmann.cl/automoviles/mercedes-benz/nuestros-vehiculos"
BRAND = "Mercedez Benz"


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

          // Primer botón: "Más información"
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
    """
    Extrae versiones desde #contentSelector si existe.
    Para modelos que no lo tienen, retorna [].
    """
    try:
        await page.wait_for_selector("#contentSelector", state="attached", timeout=7000)
    except:
        return []

    select = page.locator("#contentSelector").first
    await select.scroll_into_view_if_needed()

    try:
        await page.wait_for_selector(
            "#contentSelector option", state="attached", timeout=7000
        )
    except:
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
    """
    Selecciona una versión en el <select id="contentSelector"> y espera a que la UI se actualice.
    Señal más estable: cambio de href del botón #btnDescargarFicha (si existe).
    """
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
        except:
            await page.wait_for_timeout(600)
    else:
        await page.wait_for_timeout(600)


# -------------------------
# Precios por versión (accordion Precio)
# -------------------------
async def abrir_precio_accordion_si_corresponde(page):
    """
    Asegura que el accordion 'Precio' esté abierto.
    Robusto: scroll + click force + fallback JS.
    """
    precio_btn = page.locator(
        ".accordion-item:has(.accordion-header:has-text('Precio')) .accordion-header"
    ).first

    if await precio_btn.count() == 0:
        return False

    # Scroll a la zona del accordion para hacerlo "clickeable"
    try:
        await precio_btn.scroll_into_view_if_needed(timeout=5000)
        await page.wait_for_timeout(150)
    except:
        pass

    # Si ya está activo, listo
    cls = (await precio_btn.get_attribute("class")) or ""
    if "active" in cls:
        return True

    # Intento 1: click forzado (ignora visibilidad)
    try:
        await precio_btn.click(force=True, timeout=5000)
        await page.wait_for_timeout(250)
    except:
        pass

    cls = (await precio_btn.get_attribute("class")) or ""
    if "active" in cls:
        return True

    # Intento 2: click vía JS
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
    except:
        pass

    cls = (await precio_btn.get_attribute("class")) or ""
    return "active" in cls


async def extraer_precios_version_actual(page):
    """
    Lee el bloque 'Precio' del accordion para la versión seleccionada actualmente.
    """
    # Mejor esfuerzo: abrir accordion
    await abrir_precio_accordion_si_corresponde(page)

    # Asegurar que el área esté en viewport (ayuda mucho)
    try:
        await page.locator(".caracteristicas_body--content, .accordion").first.scroll_into_view_if_needed(timeout=5000)
        await page.wait_for_timeout(250)
    except:
        pass

    # Esperar el contenido (attached, no necesariamente visible)
    try:
        await page.wait_for_selector(
            ".accordion-item:has(.accordion-header:has-text('Precio')) "
            ".caracteristicas-accordion_body__item",
            state="attached",
            timeout=7000,
        )
    except:
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
# Main
# -------------------------
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
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
        print("Modelos encontrados:", len(modelos))

        resultados = []

        for i, m in enumerate(modelos, start=1):
            print(f"[{i}/{len(modelos)}] {m['model']} -> {m['detalle_url']}")
            await page.goto(m["detalle_url"], wait_until="domcontentloaded")
            await page.wait_for_timeout(700)

            versiones = await extraer_versiones(page)
            print(f"   versiones detectadas: {len(versiones)}")

            # Si el modelo no tiene selector de versiones, intentar precio base
            if not versiones:
                precio_base = await extraer_precios_version_actual(page)
                resultados.append({**m, "versions": [], "precio_base": precio_base})
                continue

            # Para cada versión: seleccionar + extraer precios
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

                # (Opcional) scroll a la zona de características para evitar "no visible"
                try:
                    await page.locator(".caracteristicas-select__right, .caracteristicas_body--content, .accordion").first.scroll_into_view_if_needed(timeout=5000)
                    await page.wait_for_timeout(200)
                except:
                    pass

                v["precios"] = await extraer_precios_version_actual(page)

            resultados.append({**m, "versions": versiones})

        out_file = "mercedes_modelos_versiones_precios.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
            for r in resultados:
                for vs in r['versions']:
                    tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
                    precio = [vs['precios']['precio_desde'],vs['precios']['precio_desde'],vs['precios']['precio_desde'],vs['precios']['precio_desde']]
                    datos = {
                        'modelo': vs['version'],
                        'marca': to_title_custom(BRAND),
                        'modelDetail': vs['version'],
                        'tiposprecio': tiposprecio,
                        'precio': precio
                    }            
                    print(datos)
                    saveCar('Mercedes Benz',datos,'https://www.kaufmann.cl/automoviles/mercedes-benz/nuestros-vehiculos')

        print("✔ Listo:", out_file)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
