import asyncio
import json
import random
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from utils import guarda_usado
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

START_URL = "https://chileautos.cl/vehiculos/usado-tipo"
ITEM_LOCATOR = "div.listing-item"

# Pausas anti-bot
DELAY_BETWEEN_LISTINGS = (6.0, 14.0)
DELAY_BETWEEN_PAGES = (12.0, 25.0)
DELAY_AFTER_BACK = (2.5, 5.0)

# Pausa larga cada N avisos
LONG_BREAK_EVERY_N = 12
LONG_BREAK_RANGE = (60.0, 140.0)

# Señales de captcha/challenge (heurísticas)
BLOCK_SELECTORS = [
    "iframe[src*='turnstile']",
    "iframe[src*='hcaptcha']",
    "iframe[src*='captcha']",
    "div.cf-turnstile",
    "text=Verifying you are human",
    "text=Checking your browser",
    "text=Access denied",
    "text=Attention Required",
    "text=Just a moment",
]

STATE_DIR = Path("state")


# -----------------------------
# Helpers
# -----------------------------
def clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def parse_int_money_clp(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_int_km(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_year_from_vehicle(vehicle_text: str) -> Optional[int]:
    if not vehicle_text:
        return None
    m = re.match(r"^\s*((19|20)\d{2})\b", vehicle_text.strip())
    return int(m.group(1)) if m else None


def split_make_model(vehicle_text: str) -> Tuple[Optional[str], Optional[str]]:
    vt = clean_text(vehicle_text)
    if not vt:
        return None, None
    vt2 = re.sub(r"^(19|20)\d{2}\s+", "", vt).strip()
    parts = vt2.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    if len(parts) == 1:
        return parts[0], None
    return None, None


def absolutize_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://chileautos.cl" + href
    return "https://chileautos.cl/" + href


def get_run_date_str() -> str:
    return date.today().isoformat()


def state_paths(run_date: str) -> Tuple[Path, Path]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    visited_path = STATE_DIR / f"visited_{run_date}.jsonl"
    results_path = STATE_DIR / f"results_{run_date}.jsonl"
    return visited_path, results_path


def append_jsonl(path: Path, obj: Any):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_visited_ids(visited_path: Path) -> set[str]:
    visited = set()
    if not visited_path.exists():
        return visited
    with visited_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if isinstance(rec, str):
                    visited.add(rec)
                elif isinstance(rec, dict):
                    if "listing_id" in rec and rec["listing_id"]:
                        visited.add(str(rec["listing_id"]))
                    elif "id" in rec and rec["id"]:
                        visited.add(str(rec["id"]))
            except Exception:
                continue
    return visited


def extract_listing_id_from_url(detail_url: str) -> Optional[str]:
    if not detail_url:
        return None
    base = detail_url.split("?", 1)[0].rstrip("/")
    last = base.rsplit("/", 1)[-1]
    m = re.search(r"(\d{5,})", last)
    if m:
        return m.group(1)
    return last or None


async def human_pause(rng: Tuple[float, float], reason: str = ""):
    t = random.uniform(*rng)
    if reason:
        print(f"   ... pausa {t:.1f}s ({reason})", flush=True)
    await asyncio.sleep(t)


async def is_blocked(page) -> bool:
    for sel in BLOCK_SELECTORS:
        try:
            if await page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass

    try:
        title = (await page.title()) or ""
        if any(x in title.lower() for x in ["attention required", "access denied", "just a moment"]):
            return True
    except Exception:
        pass

    try:
        body = await page.evaluate("() => document.body ? document.body.innerText : ''")
        b = (body or "").lower()
        if any(x in b for x in ["checking your browser", "verify you are human", "access denied", "attention required"]):
            return True
    except Exception:
        pass

    return False


# -----------------------------
# Listing page (listado)
# -----------------------------
async def goto_list(page, page_num: int):
    url = START_URL if page_num == 1 else f"{START_URL}?page={page_num}"
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector(ITEM_LOCATOR, timeout=90000)
    await page.wait_for_timeout(800)


async def extract_basic_from_locator(item_loc) -> Dict[str, Any]:
    make = clean_text(await item_loc.get_attribute("data-webm-make") or "")
    model = clean_text(await item_loc.get_attribute("data-webm-model") or "")

    title = ""
    h3 = item_loc.locator("h3").first
    if await h3.count() > 0:
        title = clean_text(await h3.inner_text())

    href = ""
    a = item_loc.locator("a[href]").first
    if await a.count() > 0:
        href = await a.get_attribute("href") or ""
    detail_url = absolutize_url(href) if href else ""
    listing_id = extract_listing_id_from_url(detail_url) if detail_url else None

    blob = ""
    try:
        blob = clean_text(await item_loc.inner_text())
    except Exception:
        blob = ""

    price_text = ""
    mprice = re.search(r"\$\s?[\d\.\,]+", blob)
    if mprice:
        price_text = mprice.group(0)

    return {
        "listing_id": listing_id,
        "detail_url": detail_url or None,
        "make_list": make or None,
        "model_list": model or None,
        "title_list": title or None,
        "price_text_list": price_text or None,
        "price_list": parse_int_money_clp(price_text),
    }


# -----------------------------
# Detail page (detalle)
# -----------------------------
def _norm_key(s: str) -> str:
    return clean_text(s).rstrip(":").lower()


async def extract_features_rows(container_locator):
    rows = container_locator.locator("div.row.features-item")
    n = await rows.count()

    original = {}
    normalized = {}

    for i in range(n):
        row = rows.nth(i)
        key_el = row.locator("div.features-item-name span").first
        val_el = row.locator("div.features-item-value").first

        if await key_el.count() == 0 or await val_el.count() == 0:
            continue

        k = clean_text(await key_el.inner_text())
        v_raw = (await val_el.inner_text()) or ""
        parts = [p.strip() for p in re.split(r"\n+", v_raw) if p.strip()]

        if len(parts) >= 2 and len(set(parts)) == 1:
            v = parts[0]
        else:
            v = " | ".join(parts) if parts else clean_text(v_raw)

        if not k or not v:
            continue

        original[k] = v
        normalized[_norm_key(k)] = v

    return normalized, original


async def extract_detail(page) -> Dict[str, Any]:
    wrapper = page.locator("div.features-wrapper").first
    await wrapper.wait_for(timeout=60000)

    if await is_blocked(page):
        print("⚠️  Captcha/challenge detectado en DETALLE. Resuélvelo manualmente y presiona ENTER...", flush=True)
        input()
        await wrapper.wait_for(timeout=60000)

    details_tab = wrapper.locator("#details").first
    specs_tab = wrapper.locator("#specifications").first

    details_norm, details_orig = await extract_features_rows(details_tab)

    specs_norm, specs_orig = {}, {}
    try:
        if await specs_tab.locator("div.row.features-item").count() == 0:
            tab_btn = wrapper.locator("a#specifications-tab").first
            if await tab_btn.count() > 0:
                await tab_btn.click()
                await page.wait_for_timeout(600)
        specs_norm, specs_orig = await extract_features_rows(specs_tab)
    except Exception:
        specs_norm, specs_orig = {}, {}

    vehicle = details_norm.get("vehículo") or details_norm.get("vehiculo")
    price_text = details_norm.get("precio")
    km_text = details_norm.get("kilómetros") or details_norm.get("kilometros")
    color = details_norm.get("color")
    color_interior = details_norm.get("color interior")
    fuel = details_norm.get("combustible")
    region = details_norm.get("región") or details_norm.get("region")
    comuna = details_norm.get("comuna")

    version = specs_norm.get("versión") or specs_norm.get("version")
    puertas = specs_norm.get("puertas")
    transmision = specs_norm.get("transmisión") or specs_norm.get("transmision")
    carroceria = specs_norm.get("carrocería") or specs_norm.get("carroceria")
    unico_dueno = (
        specs_norm.get("único dueño")
        or specs_norm.get("unico dueño")
        or specs_norm.get("unico dueno")
    )
    tipo_categoria = specs_norm.get("tipo categoria")

    year = parse_year_from_vehicle(vehicle) if vehicle else None
    make_detail, model_detail = split_make_model(vehicle) if vehicle else (None, None)

    def to_bool(x):
        if x is None:
            return None
        s = str(x).strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
        return x

    return {
        "vehicle_text": vehicle,
        "year_detail": year,
        "make_detail": make_detail,
        "model_detail": model_detail,
        "price_text_detail": price_text,
        "price_detail": parse_int_money_clp(price_text) if price_text else None,
        "km_text_detail": km_text,
        "km_detail": parse_int_km(km_text) if km_text else None,
        "color_detail": color,
        "color_interior_detail": color_interior,
        "fuel_detail": fuel,
        "region_detail": region,
        "comuna_detail": comuna,

        "version_detail": version,
        "doors_detail": int(puertas) if puertas and str(puertas).isdigit() else puertas,
        "transmission_detail": transmision,
        "body_type_detail": carroceria,
        "single_owner_detail": to_bool(unico_dueno),
        "category_type_detail": tipo_categoria,

        "details_raw": details_orig,
        "specs_raw": specs_orig,

        "detail_page_url": page.url,
    }


def print_record(record: Dict[str, Any]):
    """
    Imprime inmediatamente cada auto procesado: resumen + JSON completo bonito.
    """
    guarda_usado(record)
    


# -----------------------------
# Main scraping loop with resume
# -----------------------------
async def scrape_all_pages_with_details_resume(
    headless: bool = False,
    start_page: int = 1,
    max_pages: Optional[int] = None,
):
    run_date = get_run_date_str()
    visited_path, results_path = state_paths(run_date)

    visited_ids = load_visited_ids(visited_path)
    print(f"[STATE] Fecha run: {run_date}", flush=True)
    print(f"[STATE] Visitados cargados: {len(visited_ids)}", flush=True)
    print(f"[STATE] visited_file: {visited_path}", flush=True)
    print(f"[STATE] results_file: {results_path}", flush=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            locale="es-CL",
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = await context.new_page()

        page_num = start_page
        processed_total = 0

        while True:
            if max_pages is not None and page_num > max_pages:
                print(f"[INFO] max_pages={max_pages} alcanzado. Corto.", flush=True)
                break

            print(f"\n=== LISTADO página {page_num} ===", flush=True)
            await goto_list(page, page_num)

            if await is_blocked(page):
                print("⚠️  Captcha/challenge detectado en LISTADO. Resuélvelo manualmente y presiona ENTER...", flush=True)
                input()
                await page.wait_for_selector(ITEM_LOCATOR, timeout=90000)

            items = page.locator(ITEM_LOCATOR)
            count = await items.count()
            if count == 0:
                print("[INFO] 0 items. Fin.", flush=True)
                break

            print(f"[OK] items en página {page_num}: {count}", flush=True)

            for idx in range(count):
                item_loc = items.nth(idx)

                # Leer item (con reintento si DOM re-renderiza)
                try:
                    basic = await extract_basic_from_locator(item_loc)
                except Exception as e:
                    print(f"[WARN] Lectura item {page_num}:{idx+1} falló (reintento). {type(e).__name__}: {e}", flush=True)
                    await page.wait_for_timeout(900)
                    items = page.locator(ITEM_LOCATOR)
                    if idx >= await items.count():
                        print("       índice ya no existe (DOM cambió). salto.", flush=True)
                        continue
                    basic = await extract_basic_from_locator(items.nth(idx))

                listing_id = basic.get("listing_id")
                detail_url = basic.get("detail_url")

                # fallback id si falta
                if not listing_id and detail_url:
                    listing_id = "url:" + detail_url

                if listing_id and listing_id in visited_ids:
                    print(f"  -> [{page_num}:{idx+1}] SKIP ya visitado hoy: {listing_id}", flush=True)
                    continue

                # Pausas anti-bot
                processed_total += 1
                await human_pause(DELAY_BETWEEN_LISTINGS, reason="antes de entrar al detalle")
                if LONG_BREAK_EVERY_N and processed_total % LONG_BREAK_EVERY_N == 0:
                    await human_pause(LONG_BREAK_RANGE, reason=f"pausa larga cada {LONG_BREAK_EVERY_N} avisos")

                print(
                    f"  -> [{page_num}:{idx+1}] VISIT id={listing_id} | "
                    f"{basic.get('make_list')} {basic.get('model_list')} | {basic.get('price_text_list')}",
                    flush=True,
                )

                if not detail_url:
                    print("     [WARN] Sin detail_url, salto.", flush=True)
                    continue

                # Entrar al detalle
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=90000)
                except PlaywrightTimeoutError:
                    print("     [WARN] Timeout entrando al detalle, salto.", flush=True)
                    await goto_list(page, page_num)
                    continue

                # Extraer detalle
                try:
                    detail = await extract_detail(page)
                except Exception as e:
                    print(f"     [ERROR] Falló extract_detail: {type(e).__name__}: {e}", flush=True)
                    await goto_list(page, page_num)
                    continue

                record = {
                    "run_date": run_date,
                    "listing_id": listing_id,
                    "page": page_num,
                    "pos_in_page": idx + 1,
                    **basic,
                    **detail,
                    "scraped_at": datetime.utcnow().isoformat() + "Z",
                }

                # PRINT INMEDIATO (lo que pediste)
                print_record(record)

                # Guardar resultado (streaming)
                append_jsonl(results_path, record)

                # Marcar como visitado (solo si guardó OK)
                append_jsonl(visited_path, {"listing_id": listing_id, "ts": record["scraped_at"]})
                visited_ids.add(listing_id)

                # Volver al listado
                await human_pause(DELAY_AFTER_BACK, reason="antes de volver al listado")
                await goto_list(page, page_num)

            # siguiente página
            page_num += 1
            await human_pause(DELAY_BETWEEN_PAGES, reason="entre páginas")

            # si la siguiente no carga, fin
            try:
                await goto_list(page, page_num)
                continue
            except Exception:
                print("[INFO] No pude cargar la siguiente página (probablemente no existe). Fin.", flush=True)
                break

        await browser.close()


async def main():
    await scrape_all_pages_with_details_resume(
        headless=False,   # según tu experiencia, así funciona
        start_page=1,
        max_pages=None,   # todas
    )


if __name__ == "__main__":
    asyncio.run(main())
