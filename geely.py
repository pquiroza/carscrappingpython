# geely_scrape_versiones_json.py
# pip install playwright
# playwright install
# python geely_scrape_versiones_json.py

import asyncio
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

URL_LISTA_MODELOS = "https://geely.cl/modelos/"
MODELOS_JSON = Path("geely_modelos.json")
SALIDA_JSON = Path("geely_versiones_formato.json")


# ---------- Utilidades ----------
def limpiar_precio(texto: Optional[str]) -> Optional[int]:
    if not texto:
        return None
    t = str(texto).strip().lower().replace("desde", "")
    m = re.search(r"(\d{1,3}(?:[.\s]\d{3})+|\d+)", t)
    if not m:
        return None
    num = re.sub(r"[.\s,]", "", m.group(1))
    try:
        return int(num)
    except ValueError:
        return None

def norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()

def strip_accents_lower(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_price_by_regex(text: str, pattern: str) -> Optional[int]:
    m = re.search(pattern, strip_accents_lower(text))
    if not m:
        return None
    # La captura puede ser sólo número o incluir $ con miles; tomemos todo y limpiemos
    raw = m.group(1) if m.lastindex else m.group(0)
    # reconstruir desde el texto original si es necesario:
    # pero normalmente bastará:
    return limpiar_precio(raw)

def extract_label_value_from_li_text(li_text: str) -> Optional[Dict[str, str]]:
    """
    Intenta partir 'Label ... $12.345.678' en (label, valor_texto)
    """
    txt = norm_text(li_text or "") or ""
    # buscar último monto en la línea como valor
    m = re.search(r"(\$\s*\d[\d.\s]*)", txt)
    if m:
        valor = m.group(1)
        label = txt[:m.start()].strip(":– -").strip()
        return {"label": label, "valor_texto": valor}
    return None


# ---------- Modelos (para href/nombre) ----------
async def scrape_modelos(ctx) -> List[Dict[str, Any]]:
    page = await ctx.new_page()
    page.set_default_timeout(45000)
    await page.goto(URL_LISTA_MODELOS, wait_until="domcontentloaded")
    await page.wait_for_selector("ul.model-tabs__content__cards a.card-car__content")

    for _ in range(4):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(250)

    anchors = await page.query_selector_all("ul.model-tabs__content__cards a.card-car__content")
    items: List[Dict[str, Any]] = []
    for a in anchors:
        href = await a.get_attribute("href")
        h3 = await a.query_selector(".card-car__content__details h3")
        nombre = norm_text(await h3.inner_text()) if h3 else None
        if nombre and href:
            items.append({"nombre": nombre, "href": href})
    await page.close()

    seen = set()
    out = []
    for it in items:
        key = (it["href"], it["nombre"])
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out


# ---------- Versiones por modelo ----------
async def scrape_versiones_de_modelo(ctx, model: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = model.get("href")
    nombre_modelo = model.get("nombre")  # ej: "Coolray"
    page = await ctx.new_page()
    page.set_default_timeout(45000)

    versiones_out: List[Dict[str, Any]] = []
    disclaimer_text: Optional[str] = None

    try:
        await page.goto(url, wait_until="domcontentloaded")
        for _ in range(6):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(200)

        try:
            disc = await page.query_selector(".models-show-versions__content__disclaimer")
            if disc:
                disclaimer_text = norm_text(await disc.inner_text())
        except Exception:
            pass

        await page.wait_for_selector(".card-version", timeout=30000)
        cards = await page.query_selector_all(".card-version")

        for card in cards:
            # versión
            h3 = await card.query_selector(".card-version__title")
            version_title = norm_text(await h3.inner_text()) if h3 else None

            # precio desde (principal)
            price = await card.query_selector(".card-version__price")
            precio_desde_texto = norm_text(await price.inner_text()) if price else None
            precio_desde = limpiar_precio(precio_desde_texto)

            # contenedor detalles
            detalles_node = await card.query_selector(".card-version__details")
            detalles_texto = norm_text(await detalles_node.inner_text()) if detalles_node else ""

            # parse de detalles
            precio_lista = None
            bono_financiamiento = None
            bono_candidatos: List[int] = []

            # 1) Camino estructurado por <li>
            if detalles_node:
                lis = await detalles_node.query_selector_all("li")
                for li in lis:
                    # label (= primer hijo) y valor (= .card-version__details__bold | strong | b | último hijo)
                    label_node = await li.query_selector(":scope > div:nth-of-type(1), :scope > span:nth-of-type(1), :scope > *:first-child")
                    valor_node = await li.query_selector(".card-version__details__bold, strong, b, :scope > *:last-child")
                    label_txt = norm_text(await label_node.inner_text()) if label_node else None
                    valor_txt = norm_text(await valor_node.inner_text()) if valor_node else None

                    if not (label_txt and valor_txt):
                        # fallback: todo el texto del li
                        li_full = norm_text(await li.inner_text())
                        pair = extract_label_value_from_li_text(li_full or "")
                        if pair:
                            label_txt = pair["label"]
                            valor_txt = pair["valor_texto"]

                    if not (label_txt and valor_txt):
                        continue

                    lbl_norm = strip_accents_lower(label_txt)
                    if "precio" in lbl_norm and "lista" in lbl_norm:
                        precio_lista = limpiar_precio(valor_txt)
                    elif "bono" in lbl_norm and "financiamient" in lbl_norm:
                        val = limpiar_precio(valor_txt)
                        if val:
                            bono_candidatos.append(val)

            # 2) Regex sobre el texto del bloque de detalles (por si el HTML es distinto)
            if precio_lista is None and detalles_texto:
                m = re.search(r"precio\s*de\s*lista.*?\$([\d.\s]+)", strip_accents_lower(detalles_texto))
                if m:
                    precio_lista = limpiar_precio(m.group(1))

            if not bono_candidatos and detalles_texto:
                for m in re.finditer(r"bono.*?financiamient.*?\$([\d.\s]+)", strip_accents_lower(detalles_texto)):
                    val = limpiar_precio(m.group(1))
                    if val:
                        bono_candidatos.append(val)

            # 3) Fallback diferencia lista - desde
            if bono_financiamiento is None and precio_lista and precio_desde and precio_lista > precio_desde:
                diff = precio_lista - precio_desde
                if diff >= 100000:
                    bono_candidatos.append(diff)

            # 4) Fallback disclaimer
            if not bono_candidatos and disclaimer_text:
                for m in re.finditer(r"bono\s+de\s+\$([\d.\s]+)", strip_accents_lower(disclaimer_text)):
                    val = limpiar_precio(m.group(1))
                    if val:
                        bono_candidatos.append(val)

            # escoger bono (primero válido)
            if bono_candidatos:
                bono_financiamiento = next((x for x in bono_candidatos if isinstance(x, int) and x > 0), None)

            # cotizar url
            cotizar_url = None
            for a in await card.query_selector_all(".card-version__buttons a"):
                title = (await a.get_attribute("title")) or ""
                if "cotizar" in strip_accents_lower(title):
                    cotizar_url = await a.get_attribute("href")

            versiones_out.append({
                "brand": "GEELY",
                "model": (nombre_modelo or "").upper(),         # <-- MAYÚSCULAS como pediste
                "version": version_title,
                "precio_desde_texto": precio_desde_texto,
                "precio_desde": precio_desde,
                "precio_lista": precio_lista,
                "bono_directo": None,
                "bono_financiamiento": bono_financiamiento,
                "cotizar_url": cotizar_url,
                "modelo_filtro": f"GEELY {(nombre_modelo or '').upper()}".strip()
            })

    except PWTimeout:
        print(f"[WARN] Timeout en modelo: {nombre_modelo} ({url})")
    finally:
        await page.close()

    # filtrar versiones sin version o sin precio
    versiones_out = [v for v in versiones_out if v.get("version") and v.get("precio_desde")]
    return versiones_out


# ---------- Main ----------
async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        ctx = await browser.new_context(
            locale="es-CL",
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
        )

        async def route_handler(route):
            url = route.request.url
            if any(x in url for x in ["google-analytics", "gtm", "hotjar", "doubleclick"]):
                return await route.abort()
            return await route.continue_()
        await ctx.route("**/*", route_handler)

        # modelos (cache o scrape)
        if MODELOS_JSON.exists():
            modelos = json.loads(MODELOS_JSON.read_text(encoding="utf-8"))
            print(f"[INFO] Modelos cache: {len(modelos)}")
        else:
            print("[INFO] Scrapeando modelos…")
            modelos = await scrape_modelos(ctx)
            MODELOS_JSON.write_text(json.dumps(modelos, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[OK] {MODELOS_JSON.resolve()}")

        # versiones
        versiones_formato: List[Dict[str, Any]] = []
        for m in modelos:
            nombre = m.get("nombre")
            href = m.get("href")
            if not href:
                continue
            print(f"[PW] Versiones -> {nombre} ({href})")
            versiones_formato.extend(await scrape_versiones_de_modelo(ctx, m))

        await ctx.close()
        await browser.close()

    SALIDA_JSON.write_text(json.dumps(versiones_formato, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] JSON -> {SALIDA_JSON.resolve()}")
    print(f"[INFO] Total versiones: {len(versiones_formato)}")


if __name__ == "__main__":
    asyncio.run(main())
