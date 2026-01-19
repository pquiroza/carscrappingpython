import asyncio
import json
import re
from urllib.parse import urljoin, urlparse
from utils import saveCar
from utils import to_title_custom
from playwright.async_api import async_playwright


BASE_URL = "https://www.kia.cl"


def precio_a_int(texto: str | None) -> int | None:
    if not texto:
        return None
    digits = re.sub(r"[^\d]", "", texto)
    return int(digits) if digits else None


def slug_from_model_url(detalle_url: str) -> str | None:
    """
    Ej: https://www.kia.cl/modelos/automoviles/kia-soluto.html -> soluto
    """
    path = urlparse(detalle_url).path.lower()
    m = re.search(r"kia-([a-z0-9\-]+)\.html$", path)
    return m.group(1) if m else None


async def asegurar_menu_modelos_visible(page):
    try:
        await page.wait_for_selector(".cmp-kia-header__menu-modelos__content", timeout=4000)
        return
    except Exception:
        pass

    # hover best-effort
    for sel in ["li.modelos >> text=Modelos", "li.modelos", "text=Modelos"]:
        try:
            await page.hover(sel, timeout=2000)
            break
        except Exception:
            pass

    await page.wait_for_selector(".cmp-kia-header__menu-modelos__content", timeout=15000)


async def extraer_modelos_desde_menu(page) -> list[dict]:
    await asegurar_menu_modelos_visible(page)

    modelos = await page.evaluate(
        """
        () => {
          const root = document.querySelector('.cmp-kia-header__menu-modelos__content');
          if (!root) return [];

          const items = root.querySelectorAll('.cmp-kia-header__menu-modelos_vehiculo');
          const clean = (s) => (s || '').replace(/\\s+/g, ' ').trim();

          return Array.from(items).map(el => {
            const name = clean(el.querySelector('.cmp-kia-header__menu-modelos__info_name')?.innerText);

            const detalleA = el.querySelector('a[href*="/modelos/"]');
            const detalleHref = detalleA?.getAttribute('href') || null;

            const precioDesde = clean(
              el.querySelector('.cmp-kia-header__menu-modelos__info_desc1 .cmp-kia-header__menu-modelos__info_valor')?.innerText
            );
            const impuestoVerde = clean(
              el.querySelector('.cmp-kia-header__menu-modelos__info_desc2 .cmp-kia-header__menu-modelos__info_valor')?.innerText
            );
            const precioTotal = clean(
              el.querySelector('.cmp-kia-header__menu-modelos__info_desc3 .cmp-kia-header__menu-modelos__info_valor')?.innerText
            );

            const cotizarA = el.querySelector('a[href*="cotiza-tu-kia"]');
            const cotizarHref = cotizarA?.getAttribute('href') || null;

            return {
              model: name || null,
              detalle_path: detalleHref,
              precio_desde_texto: precioDesde || null,
              impuesto_verde_texto: impuestoVerde || null,
              precio_total_texto: precioTotal || null,
              cotizar_path: cotizarHref
            };
          }).filter(x => x.model && x.detalle_path);
        }
        """
    )

    out = []
    for m in modelos:
        detalle_url = urljoin(BASE_URL, m["detalle_path"])
        cotizar_url = urljoin(BASE_URL, m["cotizar_path"]) if m.get("cotizar_path") else None
        out.append(
            {
                "brand": "KIA",
                "model": m["model"],
                "detalle_url": detalle_url,
                "precio_desde_texto": m.get("precio_desde_texto"),
                "precio_desde": precio_a_int(m.get("precio_desde_texto")),
                "impuesto_verde_texto": m.get("impuesto_verde_texto"),
                "impuesto_verde": precio_a_int(m.get("impuesto_verde_texto")),
                "precio_total_texto": m.get("precio_total_texto"),
                "precio_total": precio_a_int(m.get("precio_total_texto")),
                "cotizar_url": cotizar_url,
                "modelo_filtro": f"KIA {m['model']}".upper(),
            }
        )

    # dedup por URL
    dedup = {x["detalle_url"]: x for x in out}
    return list(dedup.values())


async def extraer_versiones_de_modelo(page, detalle_url: str, model_name: str) -> list[dict]:
    """
    1) Preferimos el dataset VersionPrecioPreciosModelo (trae precioLista/bonos/etc.)
    2) Fallback: leer cards .kia-cmp__version-precio__content (si el dataset no existe)
    """
    await page.goto(detalle_url, wait_until="domcontentloaded", timeout=60000)

    # esperar que el bloque de versiones exista (si no existe, igual intentamos script)
    try:
        await page.wait_for_selector(".kia-cmp__version-precio", timeout=15000)
    except Exception:
        pass

    slug = slug_from_model_url(detalle_url)

    data = await page.evaluate(
        """
        () => {
          const clean = (s) => (s || '').replace(/\\s+/g, ' ').trim();

          // --- 1) Intento: variable VersionPrecioPreciosModelo en <script> ---
          const scripts = Array.from(document.querySelectorAll('script'))
            .map(s => s.textContent || '')
            .filter(t => t.includes('VersionPrecioPreciosModelo'));

          let dataset = null;
          for (const t of scripts) {
            // captura el string asignado: VersionPrecioPreciosModelo = '....';
            const m = t.match(/VersionPrecioPreciosModelo\\s*=\\s*'([\\s\\S]*?)';/);
            if (!m) continue;

            try {
              // el contenido viene con escapes tipo \\x22
              const decoded = m[1].replace(/\\\\x([0-9A-Fa-f]{2})/g, (_,hh) =>
                String.fromCharCode(parseInt(hh,16))
              );
              dataset = JSON.parse(decoded);
              break;
            } catch (e) {
              // sigue probando otros scripts
            }
          }

          // --- 2) Fallback: cards visibles ---
          const cards = Array.from(document.querySelectorAll('.kia-cmp__version-precio__content.cargarContenido')).map(card => {
            const sapCode = card.getAttribute('id') || null;
            const versionTitle = clean(card.querySelector('.kia-cmp__version-precio__content__title-tab')?.innerText);
            const modelVisible = clean(card.querySelector('.kia-cmp__version-precio__content__version-name')?.innerText);

            const priceBlock = card.querySelector('.kia-cmp__version-precio__content__price');
            const priceText = clean(priceBlock ? priceBlock.innerText : '');

            // el botón "Cotizar" tiene id=slug del modelo (ej: soluto)
            const cotizarId = card.querySelector('.kia-cmp__version-precio__content__button')?.getAttribute('id') || null;

            return {
              sapCode,
              versionTitle: versionTitle || null,
              modelVisible: modelVisible || null,
              priceText: priceText || null,
              cotizarId
            };
          });

          return { dataset, cards };
        }
        """
    )

    versiones = []

    # A) Si hay dataset: filtrar solo las filas del modelo actual y mapear
    if data.get("dataset"):
        for row in data["dataset"]:
            # row.modelo a veces viene "Nuevo Sportage" / "Niro Hibrido" etc.
            # Lo usamos si matchea (case-insensitive) con el model_name, si no,
            # igual lo aceptamos si el modelo está en la URL (slug) y row.version parece de este modelo.
            row_model = (row.get("modelo") or "").strip()
            ok_model = row_model.lower() == (model_name or "").strip().lower()

            # Si no calza exacto, igual lo dejamos pasar (porque algunas páginas agrupan nombres)
            # pero como estamos dentro de la página del modelo, normalmente el dataset viene filtrado
            # o al menos las primeras entradas son del modelo.
            if not ok_model and row_model and model_name:
                # heurística suave: contiene palabras principales
                a = set(row_model.lower().split())
                b = set(model_name.lower().split())
                ok_model = len(a & b) >= 1

            if not ok_model and row_model:
                # si no matchea, lo saltamos
                continue

            sap = row.get("SAPCode")
            version = row.get("version")

            precio_lista_texto = row.get("precioLista")
            bono_directo_texto = row.get("bonoDirecto")
            bono_forum_texto = row.get("bonoForum")

            # En el dataset, este suele ser el “precio final con bonos”:
            precio_desde_texto = row.get("precioBonoDirectoBonoForum") or row.get("precioConBonoDirecto")
            precio_desde = row.get("orderPrice")  # ya viene numérico
            if precio_desde is None:
                precio_desde = precio_a_int(precio_desde_texto)

            # impuesto verde con descuento (si aplica)
            iv_texto = row.get("greenTaxDiscount") or row.get("greenTax")

            versiones.append(
                {
                    "brand": "KIA",
                    "model": model_name,
                    "model_url": detalle_url,
                    "sap_code": str(sap) if sap is not None else None,
                    "version": (version or "").strip() or None,

                    "precio_desde_texto": precio_desde_texto,
                    "precio_desde": precio_desde,

                    "precio_lista_texto": precio_lista_texto,
                    "precio_lista": precio_a_int(precio_lista_texto),

                    "bono_directo_texto": bono_directo_texto,
                    "bono_directo": precio_a_int(bono_directo_texto),

                    "bono_financiamiento_texto": bono_forum_texto,
                    "bono_financiamiento": precio_a_int(bono_forum_texto),

                    "impuesto_verde_texto": iv_texto,
                    "impuesto_verde": precio_a_int(iv_texto),

                    # cotizar: la mayoría usa el slug del modelo (no por versión)
                    "cotizar_url": urljoin(BASE_URL, f"/quiero-un-kia/cotiza-tu-kia.html?modelo={slug}") if slug else None,

                    "modelo_filtro": f"KIA {model_name}".upper(),
                }
            )

    # B) Fallback DOM cards: si no salió nada del dataset
    if not versiones:
        for c in data.get("cards", []):
            version_title = c.get("versionTitle")
            price_text = c.get("priceText") or ""
            cotizar_id = c.get("cotizarId") or slug

            # parse muy básico desde texto visible (sirve como fallback)
            # busca "A partir de" y captura el $...
            m_desde = re.search(r"A\\s*partir\\s*de\\s*\\$\\s*([\\d\\.]+)", price_text, re.IGNORECASE)
            precio_desde_texto = f"$ {m_desde.group(1)}" if m_desde else None

            m_total = re.search(r"Precio\\s*Total\\s*:\\s*\\$\\s*([\\d\\.]+)", price_text, re.IGNORECASE)
            precio_total_texto = f"$ {m_total.group(1)}" if m_total else None

            m_iv = re.search(r"I\\.V\\.\\s*\\$\\s*([\\d\\.]+)", price_text, re.IGNORECASE)
            iv_texto = f"I.V. $ {m_iv.group(1)}" if m_iv else None

            versiones.append(
                {
                    "brand": "KIA",
                    "model": model_name,
                    "model_url": detalle_url,
                    "sap_code": c.get("sapCode"),
                    "version": version_title,

                    "precio_desde_texto": precio_desde_texto,
                    "precio_desde": precio_a_int(precio_desde_texto),

                    "precio_total_texto": precio_total_texto,
                    "precio_total": precio_a_int(precio_total_texto),

                    "impuesto_verde_texto": iv_texto,
                    "impuesto_verde": precio_a_int(iv_texto),

                    "cotizar_url": urljoin(BASE_URL, f"/quiero-un-kia/cotiza-tu-kia.html?modelo={cotizar_id}") if cotizar_id else None,
                    "modelo_filtro": f"KIA {model_name}".upper(),
                }
            )

    # dedup por (sap_code o version)
    seen = set()
    uniq = []
    for v in versiones:
        key = (v.get("sap_code") or "").strip() or (v.get("version") or "").strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(v)

    return uniq


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(locale="es-CL")
        page = await context.new_page()

        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

        # Cookies best-effort
        for sel in [
            "button:has-text('Aceptar')",
            "button:has-text('Acepto')",
            "button:has-text('Aceptar todo')",
            "button:has-text('Aceptar todas')",
        ]:
            try:
                if await page.locator(sel).first.is_visible(timeout=1500):
                    await page.locator(sel).first.click()
                    break
            except Exception:
                pass

        modelos = await extraer_modelos_desde_menu(page)
        print(f"Modelos encontrados: {len(modelos)}")

        all_versions = []
        errors = []

        for i, m in enumerate(modelos, 1):
            detalle_url = m["detalle_url"]
            model_name = m["model"]
            print(f"\n[{i}/{len(modelos)}] Entrando a: {model_name} -> {detalle_url}")

            try:
                versiones = await extraer_versiones_de_modelo(page, detalle_url, model_name)
                print(f"   versiones: {len(versiones)}")
                all_versions.extend(versiones)
            except Exception as e:
                errors.append({"model": model_name, "url": detalle_url, "error": str(e)})
                print(f"   ❌ error: {e}")

        payload = {
            "brand": "KIA",
            "start_url": BASE_URL,
            "total_models": len(modelos),
            "total_versions": len(all_versions),
            "items": all_versions,
            "errors": errors,
        }

        with open("kia_versiones.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


        for r in payload["items"]:
            tiposprecio = ['Crédito inteligente','Crédito convencional','Todo medio de pago','Precio de lista']
            precio = [r['precio_desde'],r['precio_desde'],r['precio_lista'],r['precio_lista']]
            datos = {
                'modelo': r['model'],
                        'marca': 'KIA',
                        'modelDetail': r['version'],    
                        'tiposprecio': tiposprecio,
                        'precio': precio
            }
            print(datos)
            saveCar('Kia',datos,"www.kia.cl")
        print("\n✅ Guardado: kia_versiones.json")
        print(f"Total versiones: {len(all_versions)} | Errores: {len(errors)}")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
