#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
from urllib.parse import quote
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.chileautos.cl/vehiculos/"


def build_url(brand: str, model: str) -> str:
    brand = brand.strip()
    model = model.strip()
    q = f"(And.Marca.{brand}._.CarAll.keyword({model}).)"
    return f"{BASE_URL}?q={quote(q, safe='')}&variant=merlin"


def clean_text(value):
    if not value:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def extract_digits(value):
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def normalize_url(url):
    if not url:
        return None
    if url.startswith("//"):
        return "https:" + url
    return url


def detect_year(title):
    if not title:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return m.group(1) if m else None


def detect_condition_from_km(km):
    if km is None:
        return None
    if km == 0:
        return "nuevo"
    if km > 0:
        return "usado"
    return None


def scrape_first_page(brand: str, model: str, year: str | None = None, headed: bool = False, slow_mo: int = 0):
    url = build_url(brand, model)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed, slow_mo=slow_mo)
        context = browser.new_context(
            locale="es-CL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2400},
        )

        page = context.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3500)
        except PlaywrightTimeoutError:
            context.close()
            browser.close()
            raise RuntimeError("Timeout cargando la página de Chileautos")

        for selector in [
            'button:has-text("Aceptar")',
            'button:has-text("Acepto")',
            'button:has-text("Entendido")',
            'button:has-text("Continuar")',
        ]:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=1200):
                    btn.click(timeout=1200)
                    page.wait_for_timeout(800)
                    break
            except Exception:
                pass

        raw_items = page.evaluate(
            """
            () => {
                const anchors = Array.from(document.querySelectorAll('a[href*="/vehiculos/detalles/"]'));
                const seen = new Set();
                const items = [];

                for (const a of anchors) {
                    const href = a.href;
                    if (!href || seen.has(href)) continue;
                    seen.add(href);

                    let card = a;
                    for (let i = 0; i < 8; i++) {
                        if (card.parentElement) {
                            card = card.parentElement;
                        }
                    }

                    const title = (a.innerText || a.textContent || '').replace(/\\s+/g, ' ').trim();

                    const fullText = (card.innerText || card.textContent || '')
                        .replace(/\\s+/g, ' ')
                        .trim();

                    let priceText = null;
                    const priceMatch = fullText.match(/\\$\\s*[\\d\\.,]+/);
                    if (priceMatch) {
                        priceText = priceMatch[0];
                    }

                    let kmText = null;
                    const kmMatch = fullText.match(/(\\d[\\d\\.,]*)\\s*km\\b/i);
                    if (kmMatch) {
                        kmText = kmMatch[1] + ' km';
                    }

                    let version = null;
                    const versionNode = card.querySelector('div.iompba0.iompba3._1lalutr15u._1lalutr330 span');
                    if (versionNode) {
                        version = (versionNode.textContent || '').replace(/\\s+/g, ' ').trim();
                    }

                    let seller = null;
                    const sellerSection = card.querySelector('[data-testid="seller-section"]');
                    if (sellerSection) {
                        const firstSpan = sellerSection.querySelector('span');
                        if (firstSpan) {
                            seller = (firstSpan.textContent || '').replace(/\\s+/g, ' ').trim();
                        }
                    }

                    let thumbnail = null;
                    const img = card.querySelector('img');
                    if (img) {
                        thumbnail =
                            img.getAttribute('src') ||
                            img.getAttribute('data-src') ||
                            null;
                    }

                    items.push({
                        title,
                        priceText,
                        kmText,
                        version,
                        seller,
                        thumbnail,
                        url: href
                    });
                }

                return items;
            }
            """
        )

        context.close()
        browser.close()

    vehicles = []
    seen_urls = set()

    for item in raw_items:
        item_url = item.get("url")
        if not item_url or item_url in seen_urls:
            continue
        seen_urls.add(item_url)

        title = clean_text(item.get("title"))
        price = extract_digits(item.get("priceText"))
        km = extract_digits(item.get("kmText"))
        version = clean_text(item.get("version"))
        seller = clean_text(item.get("seller"))
        thumbnail = normalize_url(clean_text(item.get("thumbnail")))

        detected_year = detect_year(title)

        # solo filtra por año si el usuario lo pasó
        if year and detected_year and str(detected_year) != str(year):
            continue

        condition = detect_condition_from_km(km)

        vehicles.append({
            "title": title,
            "price": price,
            "url": item_url,
            "condition": condition,
            "brand": brand,
            "model": model,
            "version": version,
            "year": str(detected_year or year) if (detected_year or year) else None,
            "km": km,
            "seller": seller,
            "thumbnail": thumbnail
        })

    payload = {
        "brand": brand,
        "model": model,
        "year": str(year) if year is not None else None,
        "condition": None,
        "source": "chileautos",
        "vehicles": vehicles
    }

    return payload


def main():
    parser = argparse.ArgumentParser(description="Chileautos scraper primera página")
    parser.add_argument("--brand", required=True, help="Marca. Ej: Geely")
    parser.add_argument("--model", required=True, help="Modelo. Ej: Coolray")
    parser.add_argument("--year", required=False, help="Año opcional. Ej: 2024")
    parser.add_argument("--output", default="chileautos_result.json", help="Archivo JSON de salida")
    parser.add_argument("--headed", action="store_true", help="Abrir navegador visible")
    parser.add_argument("--slow-mo", type=int, default=0, help="Retardo entre acciones en ms")
    args = parser.parse_args()

    data = scrape_first_page(
        brand=args.brand,
        model=args.model,
        year=args.year,
        headed=args.headed,
        slow_mo=args.slow_mo,
    )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[OK] Archivo generado: {args.output}")
    print(f"[OK] Vehículos encontrados: {len(data['vehicles'])}")


if __name__ == "__main__":
    main()