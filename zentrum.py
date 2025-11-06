# -*- coding: utf-8 -*-
import os, re, json, time, hashlib
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright
from utils import saveCar
# ===================== utilidades =====================
PRECIO_RE = re.compile(r"\$[\d\.\s]+")
def precio_a_int(txt: str):
    if not txt: return None
    m = PRECIO_RE.search(txt.replace("\xa0"," ").replace("\u202f"," "))
    if not m: return None
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
        "button:has-text('Aceptar')","button:has-text('ACEPTAR')",
        "button:has-text('Acepto')","button:has-text('OK')",
        "[aria-label='close']","[aria-label='Close']",
        ".cookies button",".cookie button"
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
    # fallbacks genéricos
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
    # 1) .auto a[href]
    href = safe_attr(card.locator(".auto a[href]"), "href")
    if href: return href
    # 2) botón "Ver +"
    href = safe_attr(card.locator(".botones a.vermas[href]"), "href")
    if href: return href
    # 3) cualquier anchor con /modelo/
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
        if href: return href
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
        # precio "Desde"
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

        # marca (desde URL de cotiza si existe)
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
            if not any(row): continue

            def val(key):
                for pos in idx.get(key, []):
                    if pos < len(row): return (row[pos] or "").strip()
                return ""

            version_txt = val("version")
            if not version_txt: continue

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
    if t: return t
    try:
        return (page.title() or "").strip()
    except Exception:
        return ""

# ===================== orquestación =====================
def scrape_zentrum_json_plano(url_listado, out_file="zentrum_volkswagen_planito.json", headless=False):
    ensure_outdir()
    resultados = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--window-size=1366,900"])
        ctx = browser.new_context(
    locale="es-CL",
    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)
        page = ctx.new_page()

        # 1) Ir al listado y **COLECTAR TODOS LOS MODELOS PRIMERO**
        page.goto(url_listado, wait_until="domcontentloaded", timeout=45000)
        close_cookies_if_any(page)
        scroll_carga(page, barridos=10)
        modelos = extraer_modelos_zentrum(page)
        print(f"[INFO] {len(modelos)} modelos recogidos")
        # IMPORTANTE: a partir de aquí NO usamos más los locators de las cards

        # 2) Recorrer la lista y entrar a cada modelo
        for m in modelos:
            print(f"→ {m['modelo'] or '(sin título)'}")
            try:
                page.goto(m["url_modelo"], wait_until="domcontentloaded", timeout=45000)
            except Exception:
                try:
                    page.goto(m["url_modelo"], wait_until="commit", timeout=45000)
                except Exception:
                    print(f"  [ERR] no se pudo abrir {m['url_modelo']}")
                    continue

            close_cookies_if_any(page)
            scroll_carga(page, barridos=6)

            # fallback de nombre si venía vacío en el listado
            if not m["modelo"]:
                m["modelo"] = titulo_modelo(page)

            versiones = extraer_versiones_en_modelo(page, m)
            if not versiones:
                scroll_carga(page, barridos=8)
                versiones = extraer_versiones_en_modelo(page, m)

            if not versiones:
                s = slugify(m["url_modelo"])
                try:
                    page.screenshot(path=f"salida_modelos/debug/{s}.png", full_page=True)
                    with open(f"salida_modelos/debug/{s}.html","w",encoding="utf-8") as f:
                        f.write(page.content())
                    print(f"  [WARN] sin versiones detectadas -> {m['url_modelo']}")
                except Exception:
                    pass

            resultados.extend(versiones)

        ctx.close()
        browser.close()

    out_path = os.path.join("salida_modelos", out_file)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
        
    for r in resultados:
        tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
        precio = [r['precio_credito_inteligente_int'],r['precio_credito_convencional_int'],r['precio_todo_medio_pago_int'],r['precio_lista_int']]

        print("-"*100)
        datos = {
            'modelo': r['modelo'],
            'marca': r['marca'],
            'modelDetail': r['version'],
            'tiposprecio':tiposprecio,
            'precio':precio
        }
        print(datos)
        saveCar(r['marca'],datos,'www.zentrum.cl')
    print(f"[OK] {len(resultados)} versiones guardadas en {out_path}")
    return out_path

# ===================== CLI =====================
if __name__ == "__main__":
    urls = ["https://zentrum.cl/modelos/volkswagen/","https://zentrum.cl/modelos/audi/","https://zentrum.cl/modelos/skoda/","https://zentrum.cl/modelos/seat/","https://zentrum.cl/modelos/cupra/"]
    for u in urls:
        
        URL_LISTADO = u
        scrape_zentrum_json_plano(URL_LISTADO, out_file="zentrum_volkswagen_planito.json", headless=False)