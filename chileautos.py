import asyncio
import json
import random
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

from utils import guarda_usado
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

HOME_URL = "https://www.chileautos.cl"
START_URL = "https://www.chileautos.cl/vehiculos/usado-tipo"
ITEM_LOCATOR = "div.listing-item"

STATE_DIR = Path("state")
PROFILE_DIR = Path("playwright_profile_chileautos")

# Señales de bloqueo
BLOCK_SELECTORS = [
    "iframe[src*='turnstile']",
    "iframe[src*='hcaptcha']",
    "iframe[src*='captcha']",
    "div.cf-turnstile",
    "input[name='cf-turnstile-response']",
    "text=Verifying you are human",
    "text=Checking your browser",
    "text=Access denied",
    "text=Attention Required",
    "text=Just a moment",
    "text=Please verify you are a human",
]

# Tiempos
DELAY_SHORT = (0.8, 2.2)
DELAY_MEDIUM = (2.0, 5.5)
DELAY_LONG = (8.0, 18.0)
DELAY_BETWEEN_PAGES = (7.0, 16.0)
DELAY_AFTER_HOME = (2.0, 5.0)
DELAY_DETAIL_STAY = (3.0, 8.0)
DELAY_AFTER_DETAIL_CLOSE = (1.0, 3.0)
LONG_BREAK_RANGE = (30.0, 90.0)

# Comportamiento "menos patrón"
MAX_PAGES_PER_RUN_DEFAULT = 4              # no muchas páginas por tanda
MIN_ITEMS_PER_PAGE = 1
MAX_ITEMS_PER_PAGE = 4                     # no abrir demasiados avisos de una misma página
PROB_SKIP_UNVISITED = 0.30                 # a veces saltar un aviso aunque no esté visitado
PROB_VIEW_ONLY_PAGE = 0.22                 # a veces "mirar" la página sin abrir avisos
PROB_GO_HOME_BETWEEN_PAGES = 0.30          # volver a home a veces
PROB_EXTRA_SCROLL = 0.45
PROB_LONG_BREAK = 0.18
PROB_RELOAD_SAME_PAGE = 0.12               # a veces recargar listado y seguir
MAX_BLOCK_EVENTS = 3

# Si tienes playwright-stealth instalado, lo usa
try:
    from playwright_stealth import stealth_async
    HAS_STEALTH = True
except Exception:
    HAS_STEALTH = False


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------
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
        return "https://www.chileautos.cl" + href
    return "https://www.chileautos.cl/" + href


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
                    if rec.get("listing_id"):
                        visited.add(str(rec["listing_id"]))
                    elif rec.get("id"):
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


def choose_subset_indices(total: int, min_n: int, max_n: int) -> List[int]:
    if total <= 0:
        return []

    n = random.randint(min_n, min(max_n, total))
    indices = list(range(total))
    random.shuffle(indices)
    selected = sorted(indices[:n])

    # a veces reordenar el orden de visita para que no sea ascendente
    if random.random() < 0.6:
        random.shuffle(selected)

    return selected


def maybe_bool_text(x):
    if x is None:
        return None
    s = str(x).strip().lower()
    if s == "true":
        return True
    if s == "false":
        return False
    return x


async def human_pause(rng: Tuple[float, float], reason: str = ""):
    t = random.uniform(*rng)
    if reason:
        print(f"   ... pausa {t:.1f}s ({reason})", flush=True)
    await asyncio.sleep(t)


async def human_mouse_move(page):
    try:
        await page.mouse.move(
            random.randint(80, 1250),
            random.randint(80, 780),
            steps=random.randint(6, 28),
        )
    except Exception:
        pass


async def human_scroll(page, min_scroll=200, max_scroll=1400):
    try:
        amount = random.randint(min_scroll, max_scroll)
        await page.mouse.wheel(0, amount)
        await page.wait_for_timeout(random.randint(500, 1800))
    except Exception:
        pass


async def micro_reading_pattern(page):
    await human_mouse_move(page)
    await page.wait_for_timeout(random.randint(300, 1200))
    if random.random() < 0.9:
        await human_scroll(page, 200, 900)
    if random.random() < PROB_EXTRA_SCROLL:
        await human_scroll(page, 150, 700)
    if random.random() < 0.25:
        await page.wait_for_timeout(random.randint(700, 2000))


async def safe_apply_stealth(page):
    if HAS_STEALTH:
        try:
            await stealth_async(page)
            print("[OK] stealth aplicado", flush=True)
        except Exception as e:
            print(f"[WARN] No pude aplicar stealth: {e}", flush=True)


async def route_handler(route):
    try:
        req = route.request
        resource_type = req.resource_type
        if resource_type in {"image", "media", "font"}:
            await route.abort()
            return
        await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass


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
        if any(x in b for x in [
            "checking your browser",
            "verify you are human",
            "access denied",
            "attention required",
            "please verify you are a human",
            "just a moment"
        ]):
            return True
    except Exception:
        pass

    return False


# ---------------------------------------------------
# Navegación
# ---------------------------------------------------
async def go_home(page):
    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(random.randint(1500, 3500))
    await micro_reading_pattern(page)


async def goto_list(page, page_num: int):
    url = START_URL if page_num == 1 else f"{START_URL}?page={page_num}"
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(random.randint(1200, 2600))
    await page.wait_for_selector(ITEM_LOCATOR, timeout=90000)
    await micro_reading_pattern(page)


async def maybe_manual_unblock(page, context: str) -> bool:
    if await is_blocked(page):
        print(f"⚠️  Bloqueo detectado en {context}. Resuélvelo manualmente y presiona ENTER...", flush=True)
        input()
        await page.wait_for_timeout(2000)
        return True
    return False


# ---------------------------------------------------
# Listado
# ---------------------------------------------------
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


async def snapshot_listing_page(page) -> List[Tuple[int, Dict[str, Any]]]:
    items = page.locator(ITEM_LOCATOR)
    count = await items.count()
    print(f"[OK] items visibles en listado: {count}", flush=True)

    rows = []
    for idx in range(count):
        item_loc = items.nth(idx)
        try:
            basic = await extract_basic_from_locator(item_loc)
            rows.append((idx + 1, basic))
        except Exception as e:
            print(f"[WARN] No pude leer item {idx+1}: {type(e).__name__}: {e}", flush=True)
    return rows


# ---------------------------------------------------
# Detalle
# ---------------------------------------------------
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

    details_tab = wrapper.locator("#details").first
    specs_tab = wrapper.locator("#specifications").first

    await micro_reading_pattern(page)

    details_norm, details_orig = await extract_features_rows(details_tab)

    specs_norm, specs_orig = {}, {}
    try:
        if await specs_tab.locator("div.row.features-item").count() == 0:
            tab_btn = wrapper.locator("a#specifications-tab").first
            if await tab_btn.count() > 0:
                await human_pause((0.8, 2.0), reason="antes de abrir specifications")
                await tab_btn.click()
                await page.wait_for_timeout(random.randint(700, 1800))
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
        "single_owner_detail": maybe_bool_text(unico_dueno),
        "category_type_detail": tipo_categoria,
        "details_raw": details_orig,
        "specs_raw": specs_orig,
        "detail_page_url": page.url,
    }


async def process_detail_in_new_tab(context, detail_url: str) -> Dict[str, Any]:
    detail_page = await context.new_page()
    await safe_apply_stealth(detail_page)

    try:
        await detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=90000)
        await detail_page.wait_for_timeout(random.randint(1300, 2600))

        if await maybe_manual_unblock(detail_page, "DETALLE"):
            await detail_page.wait_for_timeout(1000)

        # patrón menos perfecto: no siempre leer igual
        await micro_reading_pattern(detail_page)
        if random.random() < 0.65:
            await human_pause(DELAY_DETAIL_STAY, reason="leyendo detalle")

        detail = await extract_detail(detail_page)
        return detail

    finally:
        try:
            await human_pause((0.8, 2.0), reason="antes de cerrar pestaña detalle")
            await detail_page.close()
        except Exception:
            pass


def print_record(record: Dict[str, Any]):
    guarda_usado(record)


# ---------------------------------------------------
# Patrón de páginas no secuencial
# ---------------------------------------------------
def build_page_plan(start_page: int, max_pages_per_run: int) -> List[int]:
    """
    Genera un plan menos lineal.
    Ejemplos:
    [1,2,3,4]
    [1,3,2,4]
    [2,1,3]
    [1,2,4,3]
    """
    pages = list(range(start_page, start_page + max_pages_per_run))
    if len(pages) <= 1:
        return pages

    # mantener la primera relativamente cerca del inicio muchas veces
    if random.random() < 0.55:
        head = pages[:2]
        tail = pages[2:]
        random.shuffle(head)
        random.shuffle(tail)
        plan = head + tail
    else:
        random.shuffle(pages)
        plan = pages

    return plan


# ---------------------------------------------------
# Main
# ---------------------------------------------------
async def scrape_all_pages_with_details_resume(
    headless: bool = False,
    start_page: int = 1,
    max_pages_per_run: int = MAX_PAGES_PER_RUN_DEFAULT,
):
    run_date = get_run_date_str()
    visited_path, results_path = state_paths(run_date)

    visited_ids = load_visited_ids(visited_path)
    print(f"[STATE] Fecha run: {run_date}", flush=True)
    print(f"[STATE] Visitados cargados: {len(visited_ids)}", flush=True)
    print(f"[STATE] visited_file: {visited_path}", flush=True)
    print(f"[STATE] results_file: {results_path}", flush=True)

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    page_plan = build_page_plan(start_page, max_pages_per_run)
    print(f"[PLAN] páginas a visitar esta tanda: {page_plan}", flush=True)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            locale="es-CL",
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
        )

        try:
            await context.route("**/*", route_handler)
        except Exception as e:
            print(f"[WARN] No pude registrar route handler: {e}", flush=True)

        if context.pages:
            page = context.pages[0]
        else:
            page = await context.new_page()

        await safe_apply_stealth(page)

        try:
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
        except Exception as e:
            print(f"[WARN] No pude agregar init_script: {e}", flush=True)

        block_events = 0
        processed_total = 0

        print("[INFO] Abriendo home...", flush=True)
        await go_home(page)
        await human_pause(DELAY_AFTER_HOME, reason="después de home")

        for i, page_num in enumerate(page_plan, start=1):
            try:
                if i > 1 and random.random() < PROB_GO_HOME_BETWEEN_PAGES:
                    print("[INFO] vuelta intermedia a home", flush=True)
                    await go_home(page)
                    await human_pause(DELAY_AFTER_HOME, reason="después de home intermedio")

                print(f"\n=== LISTADO página {page_num} ===", flush=True)
                await goto_list(page, page_num)

                if await maybe_manual_unblock(page, f"LISTADO página {page_num}"):
                    block_events += 1

                if block_events >= MAX_BLOCK_EVENTS:
                    print("[INFO] demasiados eventos de bloqueo en esta tanda. Corto.", flush=True)
                    break

                rows = await snapshot_listing_page(page)
                if not rows:
                    print(f"[INFO] página {page_num} sin items legibles. Salto.", flush=True)
                    continue

                # A veces solo mirar la página y no abrir nada
                if random.random() < PROB_VIEW_ONLY_PAGE:
                    print(f"[INFO] solo observando página {page_num}, sin abrir avisos.", flush=True)
                    await micro_reading_pattern(page)
                    await human_pause(DELAY_BETWEEN_PAGES, reason="entre páginas")
                    continue

                selected_idx = choose_subset_indices(
                    total=len(rows),
                    min_n=MIN_ITEMS_PER_PAGE,
                    max_n=MAX_ITEMS_PER_PAGE,
                )

                print(f"[INFO] avisos elegidos en página {page_num}: {[x + 1 for x in selected_idx]}", flush=True)

                for sel in selected_idx:
                    pos_in_page, basic = rows[sel]
                    listing_id = basic.get("listing_id")
                    detail_url = basic.get("detail_url")

                    if not listing_id and detail_url:
                        listing_id = "url:" + detail_url

                    if listing_id and listing_id in visited_ids:
                        print(f"  -> [{page_num}:{pos_in_page}] SKIP ya visitado hoy: {listing_id}", flush=True)
                        continue

                    # a veces saltarlo igual para no verse tan sistemático
                    if random.random() < PROB_SKIP_UNVISITED:
                        print(f"  -> [{page_num}:{pos_in_page}] SKIP aleatorio de camuflaje: {listing_id}", flush=True)
                        continue

                    processed_total += 1

                    await micro_reading_pattern(page)
                    await human_pause(DELAY_MEDIUM, reason="antes de abrir detalle")

                    if random.random() < PROB_LONG_BREAK:
                        await human_pause(LONG_BREAK_RANGE, reason="pausa larga de camuflaje")

                    print(
                        f"  -> [{page_num}:{pos_in_page}] VISIT id={listing_id} | "
                        f"{basic.get('make_list')} {basic.get('model_list')} | {basic.get('price_text_list')}",
                        flush=True,
                    )

                    if not detail_url:
                        print("     [WARN] Sin detail_url, salto.", flush=True)
                        continue

                    try:
                        detail = await process_detail_in_new_tab(context, detail_url)
                    except PlaywrightTimeoutError:
                        print("     [WARN] Timeout en detalle, salto.", flush=True)
                        continue
                    except Exception as e:
                        print(f"     [ERROR] Falló detalle: {type(e).__name__}: {e}", flush=True)
                        continue

                    record = {
                        "run_date": run_date,
                        "listing_id": listing_id,
                        "page": page_num,
                        "pos_in_page": pos_in_page,
                        **basic,
                        **detail,
                        "scraped_at": datetime.utcnow().isoformat() + "Z",
                    }

                    print_record(record)
                    append_jsonl(results_path, record)

                    append_jsonl(visited_path, {"listing_id": listing_id, "ts": record["scraped_at"]})
                    visited_ids.add(listing_id)

                    await human_pause(DELAY_AFTER_DETAIL_CLOSE, reason="después de cerrar detalle")

                # a veces reabrir o recargar la misma página
                if random.random() < PROB_RELOAD_SAME_PAGE:
                    print(f"[INFO] recargando nuevamente página {page_num}", flush=True)
                    try:
                        await goto_list(page, page_num)
                        await micro_reading_pattern(page)
                        await human_pause(DELAY_SHORT, reason="tras recarga misma página")
                    except Exception:
                        pass

                await human_pause(DELAY_BETWEEN_PAGES, reason="entre páginas")

            except Exception as e:
                print(f"[ERROR] Fallo general en página {page_num}: {type(e).__name__}: {e}", flush=True)
                continue

        await context.close()


async def main():
    await scrape_all_pages_with_details_resume(
        headless=False,
        start_page=1,
        max_pages_per_run=4,   # importante: tandas pequeñas
    )


if __name__ == "__main__":
    asyncio.run(main())