# -*- coding: utf-8 -*-
import os
import re
import json
import time
import hashlib
import sys
import traceback
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright
from utils import saveCar

# ===================== utilidades =====================
PRECIO_RE = re.compile(r"\$[\d\.\s]+")

def precio_a_int(txt: str):
    if not txt:
        return None
    m = PRECIO_RE.search(txt.replace("\xa0", " ").replace("\u202f", " "))
    if not m:
        return None
    num = re.sub(r"[^\d]", "", m.group(0))
    return int(num) if num else None

def ensure_outdir():
    os.makedirs("salida_modelos", exist_ok=True)
    os.makedirs("salida_modelos/debug", exist_ok=True)

def scroll_carga(page, barridos=8, pausa=0.25):
    for _ in range(barridos):
        page.mouse.wheel(0, 1600)
        time.sleep(pausa)
    page.mouse.wheel(0, -5000)
    time.sleep(0.1)

def close_cookies_if_any(page):
    for sel in [
        "button:has-text('Aceptar')", "button:has-text('ACEPTAR')",
        "button:has-text('Acepto')", "button:has-text('OK')",
        "[aria-label='close']", "[aria-label='Close']",
        ".cookies button", ".cookie button"
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def safe_text(locator, timeout=1200, default=""):
    try:
        if locator.count():
            return (locator.first.text_content(timeout=timeout) or "").strip()
    except Exception:
        pass
    return default

def safe_attr(locator, name, timeout=800, default=""):
    try:
        if locator.count():
            v = locator.first.get_attribute(name, timeout=timeout)
            return v or default
    except Exception:
        pass
    return default

def slugify(text: str) -> str:
    return hashlib.md5((text or "").encode("utf-8")).hexdigest()[:8]

# ===================== headers normalizados =====================
HEAD_NORMALIZADAS = {
    "modelo": "version", "versión": "version", "version": "version",
    "precio lista sugerido": "precio_lista_int",
    "precio lista sugerido*": "precio_lista_int",
    "bono directo": "bono_int",
    "precio con bono directo": "precio_con_bono_directo_int",
    "bono financiamiento auto credit": "bono_fin_auto_credit_int",
    "precio con financiamiento auto credit": "precio_con_fin_auto_credit_int",
    "smart credit": "smart_credit_int",
    "precio final con smart credit": "precio_credito_inteligente_int",
    "precio": "precio_lista_int",
    "precio lista": "precio_lista_int",
    "lista": "precio_lista_int",
    "desde": "precio_lista_int",
    "precio desde": "precio_lista_int",
    "bono": "bono_int",
}

def normaliza_header(h: str) -> str:
    key = (h or "").strip().lower()
    key = key.replace("*", "")
    key = re.sub(r"\s+", " ", key)
    return HEAD_NORMALIZADAS.get(key, key)

# ===================== extracción listado =====================
def extract_model_link(card):
    href = safe_attr(card.locator(".auto a[href]"), "href")
    if href:
        return href

    href = safe_attr(card.locator(".botones a.vermas[href]"), "href")
    if href:
        return href

    try:
        href = card.evaluate("""
        (el) => {
            const as = el.querySelectorAll('a[href]');
            for (const a of as) {
                const h = a.getAttribute('href') || '';
                if (h.includes('/modelo/')) return h;
            }
            return '';
        }
        """)
        if href:
            return href
    except Exception:
        pass
    return ""

def extraer_modelos_zentrum(page):
    page.wait_for_load_state("domcontentloaded", timeout=45000)
    close_cookies_if_any(page)
    scroll_carga(page, barridos=10)

    cards = page.locator(".listado-modelos .card-model")
    n = cards.count()
    modelos = []

    for i in range(n):
        card = cards.nth(i)
        url_modelo = extract_model_link(card)
        if not url_modelo:
            print(f"  [WARN] card {i}: sin href, se omite")
            continue

        nombre = safe_text(card.locator(".middle .content h4")) or safe_text(card.locator("h4"))

        precio_card_int = None
        try:
            ps = card.locator(".middle .content p")
            for j in range(ps.count()):
                txt = (ps.nth(j).text_content() or "").strip()
                if "$" in txt:
                    precio_card_int = precio_a_int(txt)
                    break
        except Exception:
            pass

        marca = "Volkswagen"
        cotiza_href = safe_attr(card.locator(".botones a.cotiza[href]"), "href")
        if cotiza_href:
            try:
                qs = parse_qs(urlparse(cotiza_href).query)
                marca = (qs.get("marca", ["Volkswagen"])[0] or "Volkswagen").title()
            except Exception:
                pass

        modelos.append({
            "marca": marca,
            "modelo": nombre,
            "url_modelo": url_modelo,
            "precio_desde_card_int": precio_card_int
        })

    return modelos

# ===================== extracción versiones =====================
def extraer_versiones_en_modelo(page, info_modelo):
    try:
        tit = page.locator(".titular-sect h4:has-text('Precios y bonos')")
        if tit.count():
            tit.first.scroll_into_view_if_needed()
            time.sleep(0.2)
    except Exception:
        pass

    scroll_carga(page, barridos=5)
    versiones = []
    tablas = page.locator("table")

    for t_i in range(tablas.count()):
        t = tablas.nth(t_i)
        head = t.locator("thead tr")
        if head.count() == 0:
            head = t.locator("tr").first
        ths = head.locator("th, td")
        headers = [(ths.nth(j).text_content() or "").strip() for j in range(ths.count())]
        headers_norm = [normaliza_header(h) for h in headers]
        if not headers_norm or "version" not in set(headers_norm):
            continue

        idx = {}
        for i, h in enumerate(headers_norm):
            idx.setdefault(h, []).append(i)

        body_trs = t.locator("tbody tr") if t.locator("tbody tr").count() else t.locator("tr").locator("xpath=./following-sibling::tr")
        for r_i in range(body_trs.count()):
            tr = body_trs.nth(r_i)
            tds = tr.locator("th, td")
            row = [(tds.nth(k).text_content() or "").strip() for k in range(tds.count())]
            if not any(row):
                continue

            def val(key):
                for pos in idx.get(key, []):
                    if pos < len(row):
                        return (row[pos] or "").strip()
                return ""

            version_txt = val("version")
            if not version_txt:
                continue

            reg = {
                "marca": info_modelo["marca"],
                "modelo": info_modelo["modelo"],
                "version": version_txt,
                "precio_card_int": info_modelo.get("precio_desde_card_int"),
                "bono_int": precio_a_int(val("bono_int")) or precio_a_int(val("bono directo")),
                "precio_credito_inteligente_int": precio_a_int(val("precio_credito_inteligente_int")) or precio_a_int(val("precio final con smart credit")),
                "precio_credito_convencional_int": precio_a_int(val("precio_con_fin_auto_credit_int")) or precio_a_int(val("precio con financiamiento auto credit")),
                "precio_todo_medio_pago_int": precio_a_int(val("precio_con_bono_directo_int")) or precio_a_int(val("precio con bono directo")),
                "precio_lista_int": precio_a_int(val("precio_lista_int")) or precio_a_int(val("precio lista sugerido")),
                "cc": None,
                "combustible": "",
                "transmision": "",
                "potencia_hp": None,
                "url_modelo": info_modelo["url_modelo"],
                "url_version": f'{info_modelo["url_modelo"]}#{slugify(version_txt)}'
            }
            versiones.append(reg)

    return versiones

def titulo_modelo(page):
    t = safe_text(page.locator("h1"))
    if t:
        return t
    try:
        return (page.title() or "").strip()
    except Exception:
        return ""

# ===================== orquestación por marca =====================
def scrape_zentrum_json_plano(url_listado, out_file="zentrum_planito.json", headless=False):
    ensure_outdir()

    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    resultados = []
    browser = None
    ctx = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, args=["--window-size=1366,900"])
            ctx = browser.new_context(
                locale="es-CL",
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
            )
            page = ctx.new_page()

            page.goto(url_listado, wait_until="domcontentloaded", timeout=45000)
            close_cookies_if_any(page)
            scroll_carga(page, barridos=10)

            modelos = extraer_modelos_zentrum(page)
            stats["models_found"] = len(modelos)
            print(f"[INFO] {len(modelos)} modelos recogidos")

            for m in modelos:
                print(f"→ {m['modelo'] or '(sin título)'}")
                try:
                    try:
                        page.goto(m["url_modelo"], wait_until="domcontentloaded", timeout=45000)
                    except Exception:
                        page.goto(m["url_modelo"], wait_until="commit", timeout=45000)

                    close_cookies_if_any(page)
                    scroll_carga(page, barridos=6)

                    if not m["modelo"]:
                        m["modelo"] = titulo_modelo(page)

                    versiones = extraer_versiones_en_modelo(page, m)
                    if not versiones:
                        scroll_carga(page, barridos=8)
                        versiones = extraer_versiones_en_modelo(page, m)

                    if not versiones:
                        stats["model_errors"] += 1
                        s = slugify(m["url_modelo"])
                        try:
                            page.screenshot(path=f"salida_modelos/debug/{s}.png", full_page=True)
                            with open(f"salida_modelos/debug/{s}.html", "w", encoding="utf-8") as f:
                                f.write(page.content())
                            print(f"  [WARN] sin versiones detectadas -> {m['url_modelo']}")
                        except Exception:
                            pass
                        continue

                    resultados.extend(versiones)
                    stats["models_processed"] += 1
                    stats["versions_found"] += len(versiones)

                except Exception as e:
                    stats["model_errors"] += 1
                    print(f"  [ERR] {m['url_modelo']}: {e}")
                    traceback.print_exc()
                    continue

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        summary = {
            "status": "error",
            "source": url_listado,
            **stats
        }
        print(json.dumps(summary, ensure_ascii=False))
        return None, [], stats, summary

    finally:
        if ctx:
            try:
                ctx.close()
            except Exception:
                pass
        if browser:
            try:
                browser.close()
            except Exception:
                pass

    out_path = os.path.join("salida_modelos", out_file)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    for r in resultados:
        try:
            tiposprecio = ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista']
            precio = [
                r.get('precio_credito_inteligente_int'),
                r.get('precio_credito_convencional_int'),
                r.get('precio_todo_medio_pago_int'),
                r.get('precio_lista_int')
            ]

            datos = {
                'modelo': r.get('modelo'),
                'marca': r.get('marca'),
                'modelDetail': r.get('version'),
                'tiposprecio': tiposprecio,
                'precio': precio
            }

            print("-" * 100)
            print(datos)
            saveCar(r['marca'], datos, 'www.zentrum.cl')
            stats["saved_ok"] += 1

        except Exception as e:
            stats["save_errors"] += 1
            print(f"[ERROR] saveCar falló para fila {r}: {e}")
            traceback.print_exc()

    summary = {
        "status": "success",
        "source": url_listado,
        **stats
    }

    if stats["models_found"] == 0:
        summary["status"] = "error"
    elif stats["models_processed"] == 0:
        summary["status"] = "error"
    elif stats["versions_found"] == 0:
        summary["status"] = "error"
    elif stats["saved_ok"] == 0:
        summary["status"] = "error"
    elif stats["models_found"] > 0:
        error_ratio = stats["model_errors"] / stats["models_found"]
        summary["error_ratio"] = round(error_ratio, 4)
        if error_ratio >= 0.5:
            summary["status"] = "error"

    print(f"[OK] {len(resultados)} versiones guardadas en {out_path}")
    return out_path, resultados, stats, summary

# ===================== CLI / ORQUESTADOR =====================
def main():
    headless = os.getenv("HEADLESS", "false").lower() == "true"

    urls = [
        "https://zentrum.cl/modelos/volkswagen/",
        "https://zentrum.cl/modelos/audi/",
        "https://zentrum.cl/modelos/skoda/",
        "https://zentrum.cl/modelos/seat/",
        "https://zentrum.cl/modelos/cupra/"
    ]

    global_stats = {
        "brands_total": len(urls),
        "brands_processed": 0,
        "brand_errors": 0,
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    summaries = []
    had_error = False

    for u in urls:
        marca_slug = urlparse(u).path.strip("/").split("/")[-1] or "zentrum"
        out_file = f"zentrum_{marca_slug}_planito.json"

        print(f"\n=== PROCESANDO {u} ===")
        out_path, resultados, stats, summary = scrape_zentrum_json_plano(
            u,
            out_file=out_file,
            headless=headless
        )

        summaries.append(summary)

        global_stats["models_found"] += stats["models_found"]
        global_stats["models_processed"] += stats["models_processed"]
        global_stats["model_errors"] += stats["model_errors"]
        global_stats["versions_found"] += stats["versions_found"]
        global_stats["saved_ok"] += stats["saved_ok"]
        global_stats["save_errors"] += stats["save_errors"]

        if summary["status"] == "success":
            global_stats["brands_processed"] += 1
        else:
            global_stats["brand_errors"] += 1
            had_error = True

    final_summary = {
        "status": "error" if had_error else "success",
        "source": "www.zentrum.cl",
        **global_stats,
        "brand_summaries": summaries
    }

    if global_stats["brands_processed"] == 0:
        final_summary["status"] = "error"
        print(json.dumps(final_summary, ensure_ascii=False))
        sys.exit(1)

    if global_stats["models_found"] == 0:
        final_summary["status"] = "error"
        print(json.dumps(final_summary, ensure_ascii=False))
        sys.exit(1)

    if global_stats["models_processed"] == 0:
        final_summary["status"] = "error"
        print(json.dumps(final_summary, ensure_ascii=False))
        sys.exit(1)

    if global_stats["versions_found"] == 0:
        final_summary["status"] = "error"
        print(json.dumps(final_summary, ensure_ascii=False))
        sys.exit(1)

    if global_stats["saved_ok"] == 0:
        final_summary["status"] = "error"
        print(json.dumps(final_summary, ensure_ascii=False))
        sys.exit(1)

    if global_stats["brands_total"] > 0:
        brand_error_ratio = global_stats["brand_errors"] / global_stats["brands_total"]
        final_summary["brand_error_ratio"] = round(brand_error_ratio, 4)

    print("RUN_OK")
    print(json.dumps(final_summary, ensure_ascii=False))
    sys.exit(0)

if __name__ == "__main__":
    main()