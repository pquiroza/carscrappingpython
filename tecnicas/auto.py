import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://www.auto.cl"
START_URL = "https://www.auto.cl/usados"

DATA_DIR = Path("data")
SEEN_IDS_FILE = DATA_DIR / "auto_cl_seen_ids.json"
OUTPUT_FILE = DATA_DIR / "auto_cl_usados.json"

MARCAS_CONOCIDAS = sorted([
    "Land Rover",
    "Mercedes-Benz",
    "Mercedes Benz",
    "Alfa Romeo",
    "Aston Martin",
    "Great Wall",
    "BMW",
    "BYD",
    "Changan",
    "Chery",
    "Chevrolet",
    "Citroën",
    "Citroen",
    "Cupra",
    "DFSK",
    "Fiat",
    "Ford",
    "Foton",
    "GAC",
    "Geely",
    "GWM",
    "Honda",
    "Hyundai",
    "Infiniti",
    "JAC",
    "Jeep",
    "Kaiyi",
    "Karry",
    "Kia",
    "KGM",
    "Lexus",
    "Mahindra",
    "Mazda",
    "MG",
    "Mini",
    "Mitsubishi",
    "Nissan",
    "Omoda",
    "Opel",
    "Peugeot",
    "RAM",
    "Renault",
    "Seat",
    "Skoda",
    "SsangYong",
    "Subaru",
    "Suzuki",
    "Toyota",
    "Volkswagen",
    "Volvo",
    "Maxus",
    "Jetour",
    "Deepal",
    "JMC",
    "Audi",
    "Porsche",
    "Jaguar",
    "Lifan",
    "Maserati",
    "Ferrari",
], key=len, reverse=True)


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def normalize_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def extract_numeric_value(text: str):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def extract_year(text: str):
    if not text:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", text)
    return int(match.group(0)) if match else None


def remove_year_from_version(text: str):
    if not text:
        return None
    clean = re.sub(r"\b(19|20)\d{2}\b", "", text)
    clean = normalize_text(clean)
    return clean or None


def extract_id_from_href(href: str):
    if not href:
        return None
    clean_href = href.split("?")[0].rstrip("/")
    last_segment = clean_href.split("/")[-1]
    if not last_segment:
        return None
    parts = last_segment.split("-")
    return parts[-1] if parts else last_segment


def split_marca_modelo(title: str):
    normalized = normalize_text(title)

    for marca in MARCAS_CONOCIDAS:
        pattern = rf"^{re.escape(marca)}\b"
        if re.match(pattern, normalized, flags=re.IGNORECASE):
            modelo = re.sub(pattern, "", normalized, count=1, flags=re.IGNORECASE).strip()
            return marca, (modelo if modelo else None)

    parts = normalized.split(" ", 1)
    marca = parts[0] if parts else None
    modelo = parts[1].strip() if len(parts) > 1 else None
    return marca, modelo


def extract_kilometraje_from_card(card):
    props = card.locator(".content-used-properties > div")
    count = props.count()

    for i in range(count):
        text = normalize_text(props.nth(i).inner_text())
        if "km" in text.lower():
            return extract_numeric_value(text)

    return None


def extract_cards_from_page(page, seen_ids: set):
    page.wait_for_selector("app-card-used-car", timeout=30000)

    cards = page.locator("app-card-used-car")
    total = cards.count()
    results = []

    for i in range(total):
        card = cards.nth(i)

        try:
            href = card.locator('a[href^="/usados/"]').first.get_attribute("href")
            if not href:
                continue

            item_id = extract_id_from_href(href)
            if not item_id or item_id in seen_ids:
                continue

            url = urljoin(BASE_URL, href)

            title = normalize_text(
                card.locator("h2.car-title").first.inner_text()
            )

            version_raw = normalize_text(
                card.locator("p.label-version").first.inner_text()
            )

            year_from_version = extract_year(version_raw)

            year = None
            year_locator = card.locator(".content-year b")
            if year_locator.count() > 0:
                year_text = normalize_text(year_locator.first.inner_text())
                year = extract_year(year_text)

            if year is None:
                year = year_from_version

            version = remove_year_from_version(version_raw)
            kilometraje = extract_kilometraje_from_card(card)
            marca, modelo = split_marca_modelo(title)

            results.append({
                "id": item_id,
                "source": "auto.cl",
                "url": url,
                "marca": marca,
                "modelo": modelo,
                "version": version,
                "año": year,
                "kilometraje": kilometraje,
                "titulo": title,
                "version_raw": version_raw,
                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S")
            })

        except Exception as e:
            print(f"Error procesando card {i}: {e}")

    return results


def go_to_next_page(page):
    next_button = page.locator("button", has_text="Pagina Siguiente").first

    if next_button.count() == 0:
        return False

    try:
        disabled = next_button.is_disabled()
    except Exception:
        return False

    if disabled:
        return False

    try:
        next_button.click()
        page.wait_for_load_state("networkidle", timeout=30000)
        page.wait_for_timeout(1500)
        return True
    except Exception:
        return False


def main():
    ensure_data_dir()

    seen_ids_list = load_json(SEEN_IDS_FILE, [])
    seen_ids = set(seen_ids_list)

    existing_data = load_json(OUTPUT_FILE, [])

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        try:
            print(f"Abriendo {START_URL}")
            page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            page_number = 1
            total_new = 0

            while True:
                print(f"Procesando página {page_number}...")

                items = extract_cards_from_page(page, seen_ids)

                if items:
                    for item in items:
                        existing_data.append(item)
                        seen_ids.add(item["id"])
                        total_new += 1

                    print(f"Página {page_number}: {len(items)} nuevos registros")
                else:
                    print(f"Página {page_number}: sin nuevos registros")

                has_next = go_to_next_page(page)
                if not has_next:
                    break

                page_number += 1

            save_json(SEEN_IDS_FILE, sorted(list(seen_ids)))
            save_json(OUTPUT_FILE, existing_data)

            print("=" * 50)
            print("Scraping finalizado")
            print(f"Páginas recorridas: {page_number}")
            print(f"Nuevos registros: {total_new}")
            print(f"Total ids guardados: {len(seen_ids)}")
            print(f"Datos: {OUTPUT_FILE}")
            print(f"IDs: {SEEN_IDS_FILE}")
            print("=" * 50)

        except PlaywrightTimeoutError as e:
            print(f"Timeout durante el scraping: {e}")
        except Exception as e:
            print(f"Error general: {e}")
        finally:
            browser.close()


if __name__ == "__main__":
    main()