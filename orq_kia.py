import asyncio
import json
import re
import os
import sys
import traceback
from urllib.parse import urljoin, urlparse

from utils import saveCar
from utils import to_title_custom
from playwright.async_api import async_playwright


BASE_URL = "https://www.kia.cl"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"


def precio_a_int(texto: str | None) -> int | None:
    if not texto:
        return None
    digits = re.sub(r"[^\d]", "", texto)
    return int(digits) if digits else None


def slug_from_model_url(detalle_url: str) -> str | None:
    path = urlparse(detalle_url).path.lower()
    m = re.search(r"kia-([a-z0-9\-]+)\.html$", path)
    return m.group(1) if m else None


def safe_subtract(a, b):
    if a in (None, 0):
        return None if a is None else a
    return a - (b or 0)


async def safe_text(locator, timeout=2000):
    try:
        if await locator.count() == 0:
            return ""
        return re.sub(r"\s+", " ", (await locator.first.inner_text(timeout=timeout)) or "").strip()
    except Exception:
        return ""


async def asegurar_menu_modelos_visible(page):
    try:
        await page.wait_for_selector(".cmp-kia-header__menu-modelos__content", timeout=4000)
        return
    except Exception:
        pass

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

    dedup = {x["detalle_url"]: x for x in out}
    return list(dedup.values())


async def extraer_versiones_de_modelo(page, detalle_url: str, model_name: str) -> list[dict]:
    await page.goto(detalle_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    try:
        for _ in range(3):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(300)
    except Exception:
        pass

    try:
        await page.wait_for_selector(".kia-cmp__version-precio, .card-version, .kia-cmp__version-precio__content", timeout=15000)
    except Exception:
        pass

    slug = slug_from_model_url(detalle_url)

    data = await page.evaluate(
        """
        () => {
          const clean = (s) => (s || '').replace(/\\s+/g, ' ').trim();

          const scripts = Array.from(document.querySelectorAll('script'))
            .map(s => s.textContent || '')
            .filter(t => t.includes('VersionPrecioPreciosModelo'));

          let dataset = null;
          for (const t of scripts) {
            const m = t.match(/VersionPrecioPreciosModelo\\s*=\\s*'([\\s\\S]*?)';/);
            if (!m) continue;

            try {
              const decoded = m[1].replace(/\\\\x([0-9A-Fa-f]{2})/g, (_,hh) =>
                String.fromCharCode(parseInt(hh,16))
              );
              dataset = JSON.parse(decoded);
              break;
            } catch (e) {}
          }

          const cards = Array.from(document.querySelectorAll('.kia-cmp__version-precio__content.cargarContenido')).map(card => {
            const sapCode = card.getAttribute('id') || null;
            const versionTitle = clean(card.querySelector('.kia-cmp__version-precio__content__title-tab')?.innerText);
            const modelVisible = clean(card.querySelector('.kia-cmp__version-precio__content__version-name')?.innerText);
            const priceBlock = card.querySelector('.kia-cmp__version-precio__content__price');
            const priceText = clean(priceBlock ? priceBlock.innerText : '');
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

    if data.get("dataset"):
        for row in data["dataset"]:
            try:
                row_model = (row.get("modelo") or "").strip()
                ok_model = row_model.lower() == (model_name or "").strip().lower()

                if not ok_model and row_model and model_name:
                    a = set(row_model.lower().split())
                    b = set(model_name.lower().split())
                    ok_model = len(a & b) >= 1

                if not ok_model and row_model:
                    continue

                sap = row.get("SAPCode")
                version = row.get("version")

                precio_lista_texto = row.get("precioLista")
                bono_directo_texto = row.get("bonoDirecto")
                bono_forum_texto = row.get("bonoForum")

                precio_desde_texto = row.get("precioBonoDirectoBonoForum") or row.get("precioConBonoDirecto")
                precio_desde = row.get("orderPrice")
                if precio_desde is None:
                    precio_desde = precio_a_int(precio_desde_texto)

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
                        "cotizar_url": urljoin(BASE_URL, f"/quiero-un-kia/cotiza-tu-kia.html?modelo={slug}") if slug else None,
                        "modelo_filtro": f"KIA {model_name}".upper(),
                    }
                )
            except Exception as e:
                versiones.append({
                    "_error": str(e),
                    "brand": "KIA",
                    "model": model_name,
                    "model_url": detalle_url,
                    "modelo_filtro": f"KIA {model_name}".upper(),
                })

    if not versiones:
        for c in data.get("cards", []):
            try:
                version_title = c.get("versionTitle")
                price_text = c.get("priceText") or ""
                cotizar_id = c.get("cotizarId") or slug

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
            except Exception as e:
                versiones.append({
                    "_error": str(e),
                    "brand": "KIA",
                    "model": model_name,
                    "model_url": detalle_url,
                    "modelo_filtro": f"KIA {model_name}".upper(),
                })

    seen = set()
    uniq = []
    for v in versiones:
        if v.get("_error"):
            uniq.append(v)
            continue

        key = (v.get("sap_code") or "").strip() or (v.get("version") or "").strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(v)

    return uniq


async def scrape():
    stats = {
        "models_found": 0,
        "models_processed": 0,
        "model_errors": 0,
        "versions_found": 0,
        "version_errors": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(locale="es-CL")
        page = await context.new_page()

        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

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
        stats["models_found"] = len(modelos)
        print(f"Modelos encontrados: {len(modelos)}")

        all_versions = []
        errors = []

        for i, m in enumerate(modelos, 1):
            detalle_url = m["detalle_url"]
            model_name = m["model"]
            print(f"\n[{i}/{len(modelos)}] Entrando a: {model_name} -> {detalle_url}")

            try:
                versiones = await extraer_versiones_de_modelo(page, detalle_url, model_name)
                stats["models_processed"] += 1
                stats["versions_found"] += len([v for v in versiones if not v.get("_error")])
                stats["version_errors"] += len([v for v in versiones if v.get("_error")])
                print(f"   versiones: {len(versiones)}")
                all_versions.extend(versiones)
            except Exception as e:
                stats["model_errors"] += 1
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

        await context.close()
        await browser.close()

    return payload, stats


def main():
    try:
        payload, stats = asyncio.run(scrape())

        for r in payload["items"]:
            try:
                if r.get("_error"):
                    stats["save_errors"] += 1
                    continue

                precio_desde = r.get("precio_desde")
                precio_lista = r.get("precio_lista")

                # Mantengo tu lógica base: CI y Convencional usan precio_desde
                # si no hay precio_lista, caen a precio_desde para no romper
                precio_final_lista = precio_lista if precio_lista is not None else precio_desde

                precio = [
                    precio_desde,
                    precio_desde,
                    precio_final_lista,
                    precio_final_lista
                ]

                datos = {
                    'modelo': r.get('model'),
                    'marca': 'KIA',
                    'modelDetail': r.get('version'),
                    'tiposprecio': ['Crédito inteligente', 'Crédito convencional', 'Todo medio de pago', 'Precio de lista'],
                    'precio': precio
                }

                if datos["marca"] and datos["modelo"] and datos["modelDetail"]:
                    print(datos)
                    saveCar('Kia', datos, "www.kia.cl")
                    stats["saved_ok"] += 1
                else:
                    stats["save_errors"] += 1

            except Exception as e:
                stats["save_errors"] += 1
                print(f"[ERROR] saveCar falló para fila {r}: {e}")
                traceback.print_exc()

        summary = {
            "status": "success",
            "source": "www.kia.cl",
            **stats
        }

        if stats["models_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se encontraron modelos")
            sys.exit(1)

        if stats["models_processed"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se pudo procesar ningún modelo")
            sys.exit(1)

        if stats["versions_found"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se encontraron versiones")
            sys.exit(1)

        if stats["saved_ok"] == 0:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print("[ERROR] No se guardó ningún registro en Firebase")
            sys.exit(1)

        if stats["models_found"] > 0:
            error_ratio = stats["model_errors"] / stats["models_found"]
            summary["error_ratio"] = round(error_ratio, 4)

            if error_ratio >= 0.5:
                summary["status"] = "error"
                print(json.dumps(summary, ensure_ascii=False))
                print(f"[ERROR] Demasiados errores de modelo: {stats['model_errors']} de {stats['models_found']}")
                sys.exit(1)

        print("\n✅ Guardado: kia_versiones.json")
        print(f"Total versiones: {payload['total_versions']} | Errores: {len(payload['errors'])}")
        print("RUN_OK")
        print(json.dumps(summary, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()