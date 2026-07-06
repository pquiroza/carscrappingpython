import asyncio
import json
import re
import sys
import traceback
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
    if not text:
        return None
    t = text.replace("$", "").replace(" ", "").replace(".", "").replace(",", "")
    m = re.search(r"\d+", t)
    return int(m.group(0)) if m else None


def extract_money(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\$\s*[\d\.\,]+", text)
    return clean_text(m.group(0)).replace(" ", "") if m else ""


def ensure_abs_url(href: str) -> str:
    return urljoin(BASE, href) if href else None


def slug_from_model_url(model_url: str) -> str:
    path = urlparse(model_url).path.rstrip("/")
    return path.split("/")[-1]


def slug_to_model(slug: str) -> str:
    if not slug:
        return None
    s = slug.lower().replace("chevrolet-", "")
    return " ".join(p.upper() for p in s.split("-") if p)


async def safe_text(locator, timeout=2000):
    try:
        if await locator.count() > 0:
            return clean_text(await locator.first.inner_text(timeout=timeout))
    except:
        return ""
    return ""


# ----------------------------
# NAVIGATION
# ----------------------------
async def safe_goto(page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except:
        pass

    await page.wait_for_timeout(1500)


# ----------------------------
# SCRAPING
# ----------------------------
async def get_models_from_listing(page):
    await safe_goto(page, START_URL)

    anchors = page.locator('a[href^="/marcas/chevrolet/nuevo/"]')
    count = await anchors.count()

    models = []
    seen = set()

    for i in range(count):
        href = await anchors.nth(i).get_attribute("href")
        absu = ensure_abs_url((href or "").split("?")[0])

        if not absu or absu.endswith("/nuevo"):
            continue

        path = urlparse(absu).path.strip("/").split("/")
        if len(path) != 4:
            continue

        if absu in seen:
            continue

        seen.add(absu)

        txt = clean_text(await anchors.nth(i).inner_text())
        slug = path[-1]
        model_name = txt if txt else slug_to_model(slug)

        models.append({"model_name": model_name, "model_url": absu})

    return models


async def extract_versions_from_model(page, model_url: str):
    await safe_goto(page, model_url)

    # 👇 FORZAR RENDER DE SWIPER
    for _ in range(3):
        await page.mouse.wheel(0, 1500)
        await page.wait_for_timeout(500)

    await page.wait_for_selector(".swiper-slide article", state="attached", timeout=25000)

    slides = page.locator(".swiper-slide article")
    n = await slides.count()

    rows = []

    for i in range(n):
        card = slides.nth(i)

        # SAFE TEXT (evita timeout)
        version_name = await safe_text(card.locator("h3"))
        desde_text = await safe_text(card.locator("h4"))

        credito_text = ""
        credito_conv_text = ""
        todo_text = ""

        panel = card.locator('div[role="tabpanel"]')

        if await panel.count():
            try:
                txt = clean_text(await panel.first.inner_text(timeout=2000))

                m = re.search(r"Crédito Inteligente.*?(\$\s*[\d\.\,]+)", txt)
                if m:
                    credito_text = m.group(1)

                m = re.search(r"Crédito Convencional.*?(\$\s*[\d\.\,]+)", txt)
                if m:
                    credito_conv_text = m.group(1)

                m = re.search(r"todo medio de pago.*?(\$\s*[\d\.\,]+)", txt, re.IGNORECASE)
                if m:
                    todo_text = m.group(1)

            except:
                pass

        # 👇 FILTRO CLAVE
        if not version_name and not desde_text:
            continue

        rows.append({
            "version_name": version_name,
            "desde": money_to_int(desde_text),
            "credito_inteligente": money_to_int(credito_text),
            "credito_convencional": money_to_int(credito_conv_text),
            "todo_medio_pago": money_to_int(todo_text),
        })

    return rows


# ----------------------------
# MAIN SCRAPER
# ----------------------------
async def scrape():
    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    all_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1400, "height": 900})
        page = await context.new_page()

        models = await get_models_from_listing(page)
        stats["models_found"] = len(models)

        print(f"[INFO] Modelos encontrados: {len(models)}")

        for idx, m in enumerate(models, 1):
            model_url = m["model_url"]
            modelo = slug_to_model(slug_from_model_url(model_url))

            try:
                versions = await extract_versions_from_model(page, model_url)

                stats["models_processed"] += 1
                stats["versions_found"] += len(versions)

                for v in versions:
                    row = {
                        "marca": BRAND,
                        "modelo": modelo,
                        "version": v["version_name"],
                        "credito_inteligente": v["credito_inteligente"],
                        "credito_convencional": v["credito_convencional"],
                        "todo_medio_pago": v["todo_medio_pago"],
                    }
                    all_rows.append(row)

                print(f"[INFO] {idx}/{len(models)} -> versiones: {len(versions)}")

            except Exception as e:
                stats["model_errors"] += 1
                print(f"[WARN] error en {model_url}: {e}")
                traceback.print_exc()

        await browser.close()

    return all_rows, stats


# ----------------------------
# ORQUESTADOR ENTRYPOINT
# ----------------------------
def main():
    try:
        rows, stats = asyncio.run(scrape())

        for r in rows:
            try:
                tiposprecio = [
                    'Crédito inteligente',
                    'Crédito convencional',
                    'Todo medio de pago',
                    'Precio de lista'
                ]

                precios = [
                    r['credito_inteligente'],
                    r['credito_convencional'],
                    r['todo_medio_pago'],
                    r['todo_medio_pago'],
                ]

                datos = {
                    'modelo': to_title_custom(r['modelo']),
                    'marca': to_title_custom(r['marca']),
                    'modelDetail': to_title_custom(r['version']),
                    'tiposprecio': tiposprecio,
                    'precio': precios
                }

                if datos['precio'][3] is not None:
                    saveCar("Chevrolet", datos, 'www.coseche.com')
                    stats["saved_ok"] += 1
                else:
                    stats["save_errors"] += 1

            except Exception as e:
                stats["save_errors"] += 1
                print("[ERROR] saveCar:", e)

        summary = {
            "status": "success",
            "source": "www.coseche.com",
            **stats
        }

        # VALIDACIONES
        if stats["models_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary))
            sys.exit(1)

        if stats["models_processed"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary))
            sys.exit(1)

        if stats["versions_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary))
            sys.exit(1)

        if stats["saved_ok"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary))
            sys.exit(1)

        if stats["models_found"] > 0:
            error_ratio = stats["model_errors"] / stats["models_found"]
            summary["error_ratio"] = round(error_ratio, 4)

            if error_ratio >= 0.5:
                summary["status"] = "error"
                print(json.dumps(summary))
                sys.exit(1)

        print(json.dumps(summary))
        print("RUN_OK")
        sys.exit(0)

    except Exception as e:
        print("[FATAL]", e)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()