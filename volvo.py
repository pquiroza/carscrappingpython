import asyncio
import re
import json
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from utils import saveCar
from utils import to_title_custom

BASE = "https://www.salazarisrael.cl"
START_URL = f"{BASE}/marcas/volvo/nuevo"
DETAIL_BTN_TEXT = "VER MÁS DETALLES DEL AUTO"
BRAND = "VOLVO"


def extract_money_text(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\$\s*[\d\.]+", text)
    return m.group(0).replace(" ", "") if m else text.strip()


def money_to_int(text: str):
    if not text:
        return None
    m = re.search(r"\$\s*([\d\.]+)", text)
    if not m:
        return None
    digits = m.group(1).replace(".", "")
    try:
        return int(digits)
    except ValueError:
        return None


async def scroll_until_stable(page, selector: str, max_rounds: int = 30, wait_ms: int = 900):
    stable_rounds = 0
    last_count = -1

    for _ in range(max_rounds):
        count = await page.locator(selector).count()
        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = count

        if stable_rounds >= 2:
            break

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(wait_ms)


async def maybe_close_popups(page):
    candidates = [
        'button:has-text("Aceptar")',
        'button:has-text("ACEPTAR")',
        'button:has-text("Entendido")',
        'button:has-text("ENTENDIDO")',
        'button:has-text("Cerrar")',
        'button:has-text("CERRAR")',
        '[aria-label="close"]',
        '[aria-label="Close"]',
        '.close',
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=1200)
                await page.wait_for_timeout(250)
                break
        except Exception:
            pass


async def listar_modelos(page):
    await page.wait_for_selector("ul.grid", timeout=30000)
    details_selector = f'a:has-text("{DETAIL_BTN_TEXT}")'
    await scroll_until_stable(page, details_selector)
    await maybe_close_popups(page)

    raw_items = await page.eval_on_selector_all(
        details_selector,
        """
        (links) => links.map(a => {
          const href = a.getAttribute("href") || "";
          const article = a.closest("article");
          const model =
            article?.querySelector('span.text-xl.text-SI-primary-dark.font-medium')?.textContent?.trim() ||
            article?.querySelector('span.text-xl')?.textContent?.trim() ||
            article?.querySelector('h2, h3')?.textContent?.trim() ||
            "";
          return { model, href };
        }).filter(x => x.href)
        """
    )

    uniq = {}
    for it in raw_items:
        full_url = it["href"] if it["href"].startswith("http") else urljoin(BASE, it["href"])
        if full_url not in uniq:
            uniq[full_url] = {"model": it["model"] or "(sin nombre)", "url": full_url}
    return list(uniq.values())


async def listar_versiones_en_detalle_modelo(page):
    # CLAVE: no esperes "visible", espera "attached"
    await page.wait_for_selector("div.swiper-wrapper", state="attached", timeout=45000)

    # Elegir el swiper-wrapper "bueno" (el que realmente tiene slides con h3/link)
    wrapper_handle = await page.evaluate_handle(
        """
        () => {
          const wrappers = Array.from(document.querySelectorAll("div.swiper-wrapper"));
          if (!wrappers.length) return null;

          const scoreWrapper = (w) => {
            const slides = w.querySelectorAll(".swiper-slide");
            let score = slides.length;

            // suma puntos si encuentra h3 con texto y/o links
            for (const s of slides) {
              const h3 = s.querySelector("h3");
              if (h3 && (h3.textContent || "").trim().length > 0) score += 10;
              if (s.querySelector("a[href]")) score += 10;
              if (s.querySelector("[data-href]")) score += 5;
              if (s.querySelector("[onclick]")) score += 2;
            }

            // castigo si está dentro de algo oculto
            const style = window.getComputedStyle(w);
            if (style && (style.display === "none" || style.visibility === "hidden")) score -= 1000;

            return score;
          };

          wrappers.sort((a, b) => scoreWrapper(b) - scoreWrapper(a));
          return wrappers[0];
        }
        """
    )

    if not wrapper_handle or await wrapper_handle.evaluate("(w) => w === null"):
        return []

    # Extrae versiones SOLO dentro del wrapper elegido (evitas wrappers clonados/ocultos)
    versiones = await page.evaluate(
        """
        (wrapper) => {
          const out = [];
          const slides = wrapper.querySelectorAll(".swiper-slide");

          for (const s of slides) {
            const h3 = s.querySelector("h3");
            const version =
              h3?.textContent?.trim() ||
              h3?.getAttribute("title")?.trim() ||
              s.querySelector("[title]")?.getAttribute("title")?.trim() ||
              "";

            const precio_card =
              s.querySelector('section[aria-label="Información de precio"] p')?.textContent?.trim() ||
              s.querySelector("p")?.textContent?.trim() ||
              "";

            let href =
              s.querySelector("a[href]")?.getAttribute("href") ||
              s.querySelector("[data-href]")?.getAttribute("data-href") ||
              "";

            if (!href) {
              const oc = s.querySelector("[onclick]")?.getAttribute("onclick") || "";
              const m = oc.match(/https?:\\/\\/[^'"]+|\\/[^'"]+/);
              if (m) href = m[0];
            }

            if (version && href) out.push({ version, precio_card, href });
          }

          return out;
        }
        """,
        wrapper_handle
    )

    uniq = {}
    for v in versiones:
        full_url = v["href"] if v["href"].startswith("http") else urljoin(BASE, v["href"])
        key = f"{v['version']}|{full_url}"
        if key not in uniq:
            uniq[key] = {
                "version": v["version"],
                "precio_card": extract_money_text(v.get("precio_card", "")),
                "url": full_url
            }

    return list(uniq.values())


async def _pick_best_price_section(page):
    await page.wait_for_selector("#price-section", state="attached", timeout=30000)
    handle = await page.evaluate_handle(
        """
        () => {
          const els = Array.from(document.querySelectorAll("#price-section"));
          if (!els.length) return null;

          const scored = els.map(el => {
            const t = (el.innerText || "");
            const hasMoney = /\\$\\s*[0-9\\.]+/.test(t);
            const score = (hasMoney ? 10000 : 0) + t.length;
            return { el, score };
          }).sort((a,b) => b.score - a.score);

          return scored[0].el;
        }
        """
    )
    return handle


async def capturar_precios_y_cotizar(page):
    try:
        await page.wait_for_load_state("networkidle", timeout=25000)
    except PlaywrightTimeoutError:
        pass

    await maybe_close_popups(page)

    price_section = await _pick_best_price_section(page)
    if not price_section:
        return {
            "precio_desde_texto": "",
            "credito_inteligente_texto": "",
            "todo_medio_texto": "",
            "ref": "",
            "cotizar_url": None,
            "opciones_raw": {}
        }

    try:
        await price_section.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass

    try:
        await page.wait_for_function(
            """(el) => el && /\\$\\s*[0-9\\.]+/.test(el.innerText || "")""",
            arg=price_section,
            timeout=20000
        )
    except PlaywrightTimeoutError:
        pass

    try:
        toggle = await page.evaluate_handle(
            "(el) => el.querySelector('.payment-options > div > div')",
            price_section
        )
        if await toggle.evaluate("(t) => !!t"):
            txt = await toggle.evaluate("(t) => (t.innerText || '').toLowerCase()")
            if "ver más" in txt and "opciones de pago" in txt:
                await toggle.click()
                await page.wait_for_timeout(400)
    except Exception:
        pass

    data = await page.evaluate(
        """
        (el) => {
          const out = { desde: "", opciones: [], ref: "" };

          const m = (el.innerText || "").match(/\\$\\s*[0-9\\.]+/);
          out.desde = m ? m[0].replace(/\\s+/g, "") : "";

          const lis = el.querySelectorAll(".payment-options li");
          out.opciones = Array.from(lis).map(li => {
            const label = li.querySelector(".label")?.textContent?.trim() || "";
            const price = li.querySelector(".price")?.textContent?.trim() || "";
            return { label, price };
          }).filter(x => x.label && x.price);

          const containerText = (el.parentElement?.innerText || el.innerText || "");
          const rm = containerText.match(/REF:\\s*([^\\n\\r]+)/);
          out.ref = rm ? rm[1].trim() : "";

          return out;
        }
        """,
        price_section
    )

    opciones_raw = {}
    credito_txt = ""
    todo_txt = ""

    for row in data.get("opciones", []):
        label = row["label"]
        price_txt = extract_money_text(row["price"])
        opciones_raw[label] = price_txt

        lk = label.lower()
        if "crédito inteligente" in lk or "credito inteligente" in lk:
            credito_txt = price_txt
        if "todo medio de pago" in lk:
            todo_txt = price_txt

    cotizar_url = None
    try:
        a = page.locator('a:has-text("Cotizar")').first
        if await a.count() > 0:
            href = await a.get_attribute("href")
            if href:
                cotizar_url = href if href.startswith("http") else urljoin(BASE, href)
    except Exception:
        cotizar_url = None

    return {
        "precio_desde_texto": extract_money_text(data.get("desde", "")),
        "credito_inteligente_texto": credito_txt,
        "todo_medio_texto": todo_txt,
        "ref": data.get("ref", ""),
        "cotizar_url": cotizar_url,
        "opciones_raw": opciones_raw
    }


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        print("Abriendo START_URL:", START_URL)
        await page.goto(START_URL, wait_until="domcontentloaded")
        await maybe_close_popups(page)

        modelos = await listar_modelos(page)
        print("Modelos encontrados:", len(modelos))

        resultados = []

        for i, m in enumerate(modelos, 1):
            print(f"\n[{i}/{len(modelos)}] Modelo:", m["model"], "->", m["url"])
            await page.goto(m["url"], wait_until="domcontentloaded")
            await maybe_close_popups(page)

            try:
                versiones = await listar_versiones_en_detalle_modelo(page)
                print("  Versiones encontradas:", len(versiones))
            except Exception as e:
                print("  ERROR listando versiones:", repr(e))
                continue

            for j, v in enumerate(versiones, 1):
                print(f"    [{j}/{len(versiones)}] Version:", v["version"], "->", v["url"])
                await page.goto(v["url"], wait_until="domcontentloaded")
                await maybe_close_popups(page)

                info = await capturar_precios_y_cotizar(page)

                precio_desde_texto = info["precio_desde_texto"]
                precio_desde = money_to_int(precio_desde_texto)

                precio_credito = money_to_int(info["credito_inteligente_texto"])
                precio_lista = money_to_int(info["todo_medio_texto"])

                bono_financiamiento = None
                if precio_lista is not None and precio_credito is not None:
                    bono_financiamiento = max(precio_lista - precio_credito, 0)

                item = {
                    "brand": BRAND,
                    "model": m["model"],
                    "version": v["version"],
                    "precio_desde_texto": precio_desde_texto,
                    "precio_desde": precio_desde,
                    "precio_lista": precio_lista,
                    "bono_directo": None,
                    "bono_financiamiento": bono_financiamiento,
                    "cotizar_url": info["cotizar_url"],
                    "modelo_filtro": f"{BRAND} {m['model']}".strip()
                }

                print("      precio_desde:", item["precio_desde"], "| precio_lista:", item["precio_lista"])
                resultados.append(item)

        await browser.close()

        print("\nTOTAL resultados:", len(resultados))
        print(json.dumps(resultados, ensure_ascii=False, indent=2))

        for r in resultados:
            tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
            precio = [r['precio_desde'], r['precio_lista'], r['precio_lista'], r['precio_lista']]

            datos = {
                'modelo': to_title_custom(r['model']),
                'marca': to_title_custom(r['brand']),
                'modelDetail': r['version'],
                'tiposprecio': tiposprecio,
                'precio': precio
            }

            saveCar('Volvo', datos, 'www.https://www.salazarisrael.cl/')


if __name__ == "__main__":
    asyncio.run(main())
