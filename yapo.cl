# yapo_daily_ads.py
# Scraper diario (Opción A): solo procesa AVISOS nuevos (por ad_id) y corta temprano
# Incluye:
#  - Título completo (h1)
#  - Fecha de publicación (label "Publicado" / "Publicación" / "Fecha..." flexible)
#  - Marca, Modelo, Precio, Año, Kilómetros, Combustible, Transmisión (desde bloque insight dt/dd)
#  - Persistencia incremental por ad_id leyendo JSONL
#  - Corte temprano cuando encuentra muchos avisos seguidos ya vistos
#
# Requisitos:
#   pip install playwright
#   playwright install chromium

import asyncio
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, Optional, Set, List

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

START = "https://yapo.cl/autos-usados"

# Ajusta a gusto
HEADLESS = True
SLEEP_LIST = 0.6
SLEEP_DETAIL = 0.4

# Límites (útil para pruebas)
MAX_LIST_PAGES: Optional[int] = None   # None = todas (ojo: muchas)
MAX_ADS_TOTAL: Optional[int] = None    # None = sin límite

# Corte temprano: cuando ya encontramos muchos avisos seguidos ya guardados,
# asumimos que ya llegamos a “lo antiguo” y terminamos la corrida del día.
MAX_SEGUIDOS_YA_VISTOS = 40

# Meses ES (abreviados y completos)
_MONTHS_ES = {
    "ene": 1, "enero": 1,
    "feb": 2, "febrero": 2,
    "mar": 3, "marzo": 3,
    "abr": 4, "abril": 4,
    "may": 5, "mayo": 5,
    "jun": 6, "junio": 6,
    "jul": 7, "julio": 7,
    "ago": 8, "agosto": 8,
    "sep": 9, "sept": 9, "septiembre": 9,
    "oct": 10, "octubre": 10,
    "nov": 11, "noviembre": 11,
    "dic": 12, "diciembre": 12,
}


@dataclass
class AdData:
    ad_id: str
    url: str
    titulo: Optional[str] = None
    fecha_publicado_texto: Optional[str] = None
    fecha_publicado_iso: Optional[str] = None  # YYYY-MM-DD si se puede
    marca: Optional[str] = None
    modelo: Optional[str] = None
    transmision: Optional[str] = None
    precio_texto: Optional[str] = None
    precio: Optional[int] = None
    anio: Optional[int] = None
    kilometros_texto: Optional[str] = None
    kilometros: Optional[int] = None
    combustible: Optional[str] = None
    raw: Optional[Dict[str, str]] = None


def build_list_url(page: int) -> str:
    return START if page <= 1 else f"{START}.{page}"


def parse_int_digits(s: str) -> Optional[int]:
    if not s:
        return None
    d = re.sub(r"[^\d]", "", s)
    return int(d) if d else None


def normalize_transmision(t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    tt = re.sub(r"\s+", " ", t).strip().lower()
    if "auto" in tt:
        return "Automática"
    if "manual" in tt:
        return "Manual"
    return t.strip()


def parse_fecha_publicado_to_iso(texto: Optional[str]) -> Optional[str]:
    """
    Intenta convertir una fecha en español a YYYY-MM-DD.
    Soporta:
      - "02 ene 2026", "2 enero 2026"
      - "02/01/2026" o "02-01-2026"
      - Relativo simple: "hace 3 días", "hace 2 horas", "hace 1 semana" (aprox.)
    """
    if not texto:
        return None

    t = re.sub(r"\s+", " ", texto).strip().lower()

    # dd/mm/yyyy o dd-mm-yyyy
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
    if m:
        dd, mm, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(yy, mm, dd).date().isoformat()
        except ValueError:
            return None

    # "2 ene 2026" o "2 enero 2026"
    m = re.search(r"\b(\d{1,2})\s+([a-záéíóúñ]+)\s+(\d{4})\b", t)
    if m:
        dd = int(m.group(1))
        mon = m.group(2)
        yy = int(m.group(3))
        mon = mon.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        mm = _MONTHS_ES.get(mon)
        if mm:
            try:
                return datetime(yy, mm, dd).date().isoformat()
            except ValueError:
                return None

    # Relativo: "hace 3 días/horas/semanas/meses"
    m = re.search(r"\bhace\s+(\d+)\s+(minuto|minutos|hora|horas|día|dias|días|semana|semanas|mes|meses)\b", t)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        now = datetime.now()
        if unit.startswith("min"):
            dt = now - timedelta(minutes=n)
        elif unit.startswith("hora"):
            dt = now - timedelta(hours=n)
        elif unit in ("día", "dias", "días"):
            dt = now - timedelta(days=n)
        elif unit.startswith("semana"):
            dt = now - timedelta(days=7 * n)
        elif unit.startswith("mes"):
            dt = now - timedelta(days=30 * n)  # aproximación
        else:
            return None
        return dt.date().isoformat()

    # ISO dentro del texto
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t)
    if m:
        return m.group(1)

    return None


async def safe_goto(page, url: str, wait_css: str, timeout: int = 60000):
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    # "attached" = existe en DOM, no exige visible (evita timeouts por overlays/viewport)
    await page.wait_for_selector(wait_css, timeout=timeout, state="attached")


async def try_close_cookie_banner(page):
    # Best-effort
    for sel in [
        "button:has-text('Aceptar')",
        "button:has-text('Acepto')",
        "button:has-text('Entendido')",
        "button:has-text('OK')",
        "button:has-text('De acuerdo')",
        "[class*='accept']",
        "[id*='accept']",
    ]:
        try:
            loc = page.locator(sel)
            if await loc.count() and await loc.first.is_visible():
                await loc.first.click(timeout=1500)
                return
        except:
            pass


async def get_last_page(page) -> int:
    return await page.evaluate("""
        () => {
            const last = document.querySelector('a.d3-pagination__page--last[data-page]');
            return last ? parseInt(last.dataset.page) : 1;
        }
    """)


async def extract_ad_ids_from_list_page(page) -> List[str]:
    """
    En el listado existe ga4addata[<ID>] = {...}. Extraemos IDs desde scripts.
    """
    ids = await page.evaluate("""
      () => {
        const scripts = Array.from(document.querySelectorAll('script'));
        const out = [];
        for (const s of scripts) {
          const t = s.textContent || "";
          const m = t.match(/ga4addata\\[(\\d+)\\]\\s*=\\s*\\{/);
          if (m && m[1]) out.push(m[1]);
        }
        return out;
      }
    """)
    seen, ordered = set(), []
    for i in ids:
        i = str(i)
        if i not in seen:
            seen.add(i)
            ordered.append(i)
    return ordered


async def find_detail_url_for_ad(page, ad_id: str) -> Optional[str]:
    """
    Busca un <a href=".../ad_id"> dentro del listado. Esto suele existir.
    """
    return await page.evaluate(
        """(adid) => {
            const a = Array.from(document.querySelectorAll('a[href]'))
              .find(x => (x.getAttribute('href') || '').includes('/' + adid));
            if (!a) return null;
            const h = a.getAttribute('href');
            if (!h) return null;
            return h.startsWith('http') ? h : 'https://www.yapo.cl' + h;
        }""",
        ad_id
    )


async def extract_fecha_publicado_from_labels(page) -> Optional[str]:
    """
    Flexible: busca un <dt> cuyo texto contenga "public" o "fecha" (con o sin tildes),
    y retorna el <dd> asociado.
    """
    return await page.evaluate("""
      () => {
        const norm = (s) => (s || '')
          .toLowerCase()
          .normalize('NFD').replace(/[\\u0300-\\u036f]/g,'')  // quita tildes
          .replace(/\\s+/g,' ')
          .trim();

        const isWanted = (key) => {
          // flexible: "publicado", "publicación", "fecha de publicación", "publicado el", etc.
          return key.includes('public') || (key.includes('fecha') && key.includes('public'));
        };

        const dts = Array.from(document.querySelectorAll('dt'));
        for (const dt of dts) {
          const key = norm(dt.textContent);

          if (!isWanted(key)) continue;

          // Caso típico: dt y dd dentro del mismo <dl>
          const dl = dt.closest('dl');
          if (dl) {
            const dd = dl.querySelector('dd');
            if (dd) {
              const val = (dd.textContent || '').replace(/\\s+/g,' ').trim();
              if (val) return val;
            }
          }

          // Fallback: dd hermano
          const sib = dt.nextElementSibling;
          if (sib && sib.tagName && sib.tagName.toLowerCase() === 'dd') {
            const val = (sib.textContent || '').replace(/\\s+/g,' ').trim();
            if (val) return val;
          }
        }

        return null;
      }
    """)


async def scrape_detail(page, url: str, ad_id: str) -> AdData:
    await safe_goto(page, url, wait_css="div.d3-property__insight", timeout=60000)

    # KV del insight (Marca/Modelo/Precio/Año/Kilómetros/Combustible/Transmisión)
    data: Dict[str, str] = await page.evaluate("""
        () => {
            const out = {};
            const root = document.querySelector('div.d3-container.d3-property__insight');
            if (!root) return out;

            root.querySelectorAll('dl').forEach(dl => {
                const dt = dl.querySelector('dt');
                const dd = dl.querySelector('dd');
                if (dt && dd) {
                    out[dt.textContent.trim()] = dd.textContent.replace(/\\s+/g,' ').trim();
                }
            });
            return out;
        }
    """)

    # Título (robusto)
    titulo = await page.evaluate("""
        () => {
            const h1 = document.querySelector('h1');
            return h1 ? h1.textContent.replace(/\\s+/g,' ').trim() : null;
        }
    """)

    # Fecha publicación (desde label en el HTML)
    fecha_texto = await extract_fecha_publicado_from_labels(page)
    fecha_iso = parse_fecha_publicado_to_iso(fecha_texto)

    ad = AdData(
        ad_id=str(ad_id),
        url=url,
        titulo=titulo,
        fecha_publicado_texto=fecha_texto,
        fecha_publicado_iso=fecha_iso,
        raw=data
    )

    ad.marca = data.get("Marca")
    ad.modelo = data.get("Modelo")
    ad.transmision = normalize_transmision(data.get("Transmisión"))
    ad.precio_texto = data.get("Precio")
    ad.precio = parse_int_digits(ad.precio_texto or "")
    ad.anio = parse_int_digits(data.get("Año", "") or "")
    ad.kilometros_texto = data.get("Kilómetros")
    ad.kilometros = parse_int_digits(ad.kilometros_texto or "")
    ad.combustible = data.get("Combustible")

    return ad


def load_seen_from_jsonl(path: str) -> Set[str]:
    seen: Set[str] = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if "ad_id" in obj:
                        seen.add(str(obj["ad_id"]))
                except:
                    pass
    except FileNotFoundError:
        pass
    return seen


def append_jsonl(path: str, ad: AdData):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(ad), ensure_ascii=False) + "\n")


def append_csv(path: str, ad: AdData):
    fields = [
        "ad_id", "url",
        "titulo",
        "fecha_publicado_texto", "fecha_publicado_iso",
        "marca", "modelo", "transmision",
        "precio_texto", "precio",
        "anio",
        "kilometros_texto", "kilometros",
        "combustible",
    ]
    write_header = False
    try:
        with open(path, "r", encoding="utf-8"):
            pass
    except FileNotFoundError:
        write_header = True

    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow({k: getattr(ad, k) for k in fields})


async def main():
    out_jsonl = "yapo_autos_usados.jsonl"
    out_csv = "yapo_autos_usados.csv"

    seen = load_seen_from_jsonl(out_jsonl)
    print(f"🧠 Avisos ya vistos (según {out_jsonl}): {len(seen)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        )

        # Bloquear pesados (reduce requests y mejora estabilidad)
        async def block_heavy(route):
            r = route.request
            if r.resource_type in ("image", "font", "media"):
                return await route.abort()
            await route.continue_()

        await context.route("**/*", block_heavy)

        page = await context.new_page()

        # Entrar a página inicial
        await safe_goto(page, START, wait_css="body", timeout=60000)
        await try_close_cookie_banner(page)
        await page.wait_for_selector("script", state="attached", timeout=60000)

        # Paginación total
        last_page = await get_last_page(page)
        if MAX_LIST_PAGES is not None:
            last_page = min(last_page, MAX_LIST_PAGES)

        print(f"📄 Páginas a recorrer hoy: {last_page}")

        total_new = 0
        corte_total = False

        for pno in range(1, last_page + 1):
            list_url = build_list_url(pno)
            print(f"\n📃 LIST [{pno}/{last_page}] {list_url}")

            await safe_goto(page, list_url, wait_css="body", timeout=60000)
            await try_close_cookie_banner(page)
            await page.wait_for_selector("script", state="attached", timeout=60000)

            ad_ids = await extract_ad_ids_from_list_page(page)
            print(f"   🆔 IDs detectados: {len(ad_ids)}")

            seguidos_vistos = 0

            for ad_id in ad_ids:
                ad_id = str(ad_id)

                if ad_id in seen:
                    seguidos_vistos += 1
                    if seguidos_vistos >= MAX_SEGUIDOS_YA_VISTOS:
                        print(f"🛑 Corte temprano: {seguidos_vistos} avisos seguidos ya vistos (llegamos a lo antiguo).")
                        corte_total = True
                        break
                    continue

                seguidos_vistos = 0

                detail_url = await find_detail_url_for_ad(page, ad_id)
                if not detail_url:
                    print(f"   ⚠️ No encontré URL para ad_id={ad_id} en el listado (skip).")
                    continue

                try:
                    ad = await scrape_detail(page, detail_url, ad_id)
                except PWTimeoutError:
                    print(f"   ❌ Timeout en detalle: {detail_url} (skip)")
                    # Volver al listado y seguir
                    await safe_goto(page, list_url, wait_css="body", timeout=60000)
                    await asyncio.sleep(0.2)
                    continue

                # Guardado local (respaldo + persistencia)
                append_jsonl(out_jsonl, ad)
                append_csv(out_csv, ad)
                seen.add(ad_id)
                total_new += 1

                print(
                    f"   ✅ NUEVO {total_new} | ad_id={ad_id} "
                    f"titulo={ad.titulo!r} publicado={ad.fecha_publicado_texto!r} ({ad.fecha_publicado_iso}) "
                    f"marca={ad.marca!r} modelo={ad.modelo!r} transmision={ad.transmision!r} "
                    f"precio={ad.precio} año={ad.anio} km={ad.kilometros} combustible={ad.combustible!r}"
                )

                await asyncio.sleep(SLEEP_DETAIL)

                # Volver al listado para seguir iterando (1 sola pestaña)
                await safe_goto(page, list_url, wait_css="body", timeout=60000)
                await asyncio.sleep(0.1)

                if MAX_ADS_TOTAL is not None and total_new >= MAX_ADS_TOTAL:
                    print("\n🛑 Corte por MAX_ADS_TOTAL")
                    corte_total = True
                    break

            if corte_total:
                break

            await asyncio.sleep(SLEEP_LIST)

        await context.close()
        await browser.close()

    print(f"\n✅ Terminado. Avisos nuevos guardados hoy: {total_new}")
    print(f"📁 JSONL: {out_jsonl}")
    print(f"📁 CSV : {out_csv}")


if __name__ == "__main__":
    asyncio.run(main())
