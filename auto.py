import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils import guarda_autocl

BASE_URL = "https://www.auto.cl"
START_URL = "https://www.auto.cl/usados"

DATA_DIR = Path("data")
SEEN_IDS_FILE = DATA_DIR / "auto_cl_seen_ids.json"
OUTPUT_FILE = DATA_DIR / "auto_cl_usados.json"
STATE_FILE = DATA_DIR / "auto_cl_estado.json"

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
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    temp_path.replace(path)


def save_state(page_number: int, total_records: int, total_seen_ids: int):
    state = {
        "ultima_pagina_procesada": page_number,
        "total_registros_guardados": total_records,
        "total_ids_guardados": total_seen_ids,
        "last_run_at": time.strftime("%Y-%m-%dT%H:%M:%S")
    }
    save_json(STATE_FILE, state)


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


def wait_for_cards(page, timeout=45000):
    page.wait_for_selector("app-card-used-car", timeout=timeout)

    for _ in range(10):
        count = page.locator("app-card-used-car").count()
        if count > 0:
            return count
        page.wait_for_timeout(1000)

    return 0


def force_lazy_render(page):
    for _ in range(4):
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(1200)


def dismiss_possible_popups(page):
    texts = ["Aceptar", "Entendido", "Aceptar cookies", "Continuar"]
    for text in texts:
        try:
            btn = page.locator("button", has_text=text).first
            if btn.count() > 0:
                btn.click(timeout=1500)
                page.wait_for_timeout(500)
        except Exception:
            pass


def extract_used_properties(card):
    result = {
        "kilometraje": None,
        "transmision": None,
        "combustible": None,
        "ubicacion": None
    }

    props = card.locator(".content-used-properties > div")
    count = props.count()

    for i in range(count):
        try:
            item = props.nth(i)
            text = normalize_text(item.inner_text())

            img_locator = item.locator("img").first
            alt = ""
            if img_locator.count() > 0:
                alt = normalize_text(img_locator.get_attribute("alt") or "").lower()

            if not text:
                continue

            if "kilometraje" in alt or "km" in text.lower():
                result["kilometraje"] = extract_numeric_value(text)

            elif "traccion" in alt:
                result["transmision"] = text

            elif "bencina" in alt:
                result["combustible"] = text

            elif "ubicacion" in alt:
                result["ubicacion"] = text

        except Exception:
            continue

    return result


def extract_precio_from_card(card):
    """
    Extrae el precio desde .content-price b y lo normaliza como int.
    '$8.100.000' -> 8100000
    """
    try:
        price_locator = card.locator(".content-price b").first
        if price_locator.count() == 0:
            return None

        price_text = normalize_text(price_locator.inner_text())
        return extract_numeric_value(price_text)
    except Exception:
        return None


def extract_cards_from_page(page, seen_ids: set):
    card_count = wait_for_cards(page)
    print(f"Cards detectadas: {card_count}")

    cards = page.locator("app-card-used-car")
    total = cards.count()
    results = []

    for i in range(total):
        card = cards.nth(i)

        try:
            href_locator = card.locator('a[href*="/usados/"]').first
            href = href_locator.get_attribute("href")
            if not href:
                continue

            item_id = extract_id_from_href(href)
            if not item_id or item_id in seen_ids:
                continue

            url = urljoin(BASE_URL, href)

            title = ""
            title_locator = card.locator("h2.car-title").first
            if title_locator.count() > 0:
                title = normalize_text(title_locator.inner_text())

            version_raw = ""
            version_locator = card.locator("p.label-version").first
            if version_locator.count() > 0:
                version_raw = normalize_text(version_locator.inner_text())

            year = None
            year_locator = card.locator(".content-year b").first
            if year_locator.count() > 0:
                year_text = normalize_text(year_locator.inner_text())
                year = extract_year(year_text)

            if year is None:
                year = extract_year(version_raw)

            version = remove_year_from_version(version_raw)
            precio = extract_precio_from_card(card)

            props = extract_used_properties(card)

            kilometraje = props["kilometraje"]
            transmision = props["transmision"]
            combustible = props["combustible"]
            ubicacion = props["ubicacion"]

            marca, modelo = split_marca_modelo(title)

            item = {
                "id": item_id,
                "source": "auto.cl",
                "url": url,
                "marca": marca,
                "modelo": modelo,
                "version": version,
                "año": year,
                "precio": precio,
                "kilometraje": kilometraje,
                "transmision": transmision,
                "combustible": combustible,
                "ubicacion": ubicacion,
                "titulo": title,
                "version_raw": version_raw,
                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S")
            }

            results.append(item)
            print(item)
            guarda_autocl(item)

        except Exception as e:
            print(f"Error procesando card {i}: {e}")

    return results


def go_to_next_page(page):
    next_button = page.locator("button", has_text="Pagina Siguiente").first

    if next_button.count() == 0:
        print("No se encontró botón de siguiente página")
        return False

    try:
        if next_button.is_disabled():
            print("Botón siguiente deshabilitado")
            return False
    except Exception:
        return False

    try:
        next_button.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        next_button.click()
        page.wait_for_timeout(2500)
        wait_for_cards(page, timeout=30000)
        return True
    except Exception as e:
        print(f"No se pudo avanzar a la siguiente página: {e}")
        return False


def main():
    ensure_data_dir()

    seen_ids_list = load_json(SEEN_IDS_FILE, [])
    seen_ids = set(seen_ids_list)

    existing_data = load_json(OUTPUT_FILE, [])

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            slow_mo=300
        )

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
            page.wait_for_timeout(5000)

            dismiss_possible_popups(page)
            force_lazy_render(page)
            wait_for_cards(page, timeout=45000)

            page_number = 1
            total_new = 0

            while True:
                print(f"\nProcesando página {page_number}...")

                items = extract_cards_from_page(page, seen_ids)

                if items:
                    for item in items:
                        existing_data.append(item)
                        seen_ids.add(item["id"])
                        total_new += 1

                    print(f"Página {page_number}: {len(items)} nuevos registros")
                else:
                    print(f"Página {page_number}: sin nuevos registros")

                save_json(OUTPUT_FILE, existing_data)
                save_json(SEEN_IDS_FILE, sorted(list(seen_ids)))
                save_state(
                    page_number=page_number,
                    total_records=len(existing_data),
                    total_seen_ids=len(seen_ids)
                )

                print(
                    f"Guardado parcial OK -> página {page_number} | "
                    f"total registros: {len(existing_data)} | total ids: {len(seen_ids)}"
                )

                has_next = go_to_next_page(page)
                if not has_next:
                    break

                dismiss_possible_popups(page)
                force_lazy_render(page)
                page_number += 1

            print("\n" + "=" * 60)
            print("Scraping finalizado")
            print(f"Páginas recorridas: {page_number}")
            print(f"Nuevos registros: {total_new}")
            print(f"Total ids guardados: {len(seen_ids)}")
            print(f"Datos: {OUTPUT_FILE}")
            print(f"IDs: {SEEN_IDS_FILE}")
            print(f"Estado: {STATE_FILE}")
            print("=" * 60)

        except PlaywrightTimeoutError as e:
            print(f"Timeout durante el scraping: {e}")
            try:
                save_json(OUTPUT_FILE, existing_data)
                save_json(SEEN_IDS_FILE, sorted(list(seen_ids)))
                save_state(
                    page_number=page_number if 'page_number' in locals() else 0,
                    total_records=len(existing_data),
                    total_seen_ids=len(seen_ids)
                )
                print("Se guardó el progreso parcial tras timeout.")
            except Exception as save_error:
                print(f"No se pudo guardar el progreso tras timeout: {save_error}")

        except Exception as e:
            print(f"Error general: {e}")
            try:
                save_json(OUTPUT_FILE, existing_data)
                save_json(SEEN_IDS_FILE, sorted(list(seen_ids)))
                save_state(
                    page_number=page_number if 'page_number' in locals() else 0,
                    total_records=len(existing_data),
                    total_seen_ids=len(seen_ids)
                )
                print("Se guardó el progreso parcial tras error.")
            except Exception as save_error:
                print(f"No se pudo guardar el progreso tras error: {save_error}")

        finally:
            browser.close()


if __name__ == "__main__":
    main()