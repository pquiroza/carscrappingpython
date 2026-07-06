import os
import re
import sys
import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from utils import quitar_palabra
from utils import to_title_custom
from utils import saveCar
from utils import saveCarDate
from bs4 import BeautifulSoup
import unicodedata

import firebase_admin
from firebase_admin import credentials, firestore

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug_bruno"
DEBUG_DIR.mkdir(exist_ok=True)

SERVICE_ACCOUNT_PATH = BASE_DIR / "carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json"
CHROMEDRIVER_PATH = "/opt/homebrew/bin/chromedriver"

SOURCE_NAME = "Bruno Fritsch"

BRANDS = [
    "toyota",
    "nissan",
    "peugeot",
    "citroen",
    "ram",
    "chery",
    "mg",
    'hyundai',
    "lexus",
    "opel",
    "jeep",
    "fiat",
    "exeed",
    "omoda-jaecoo",
]


#BRANDS = ['mg',"omoda-jaecoo"]

PAGE_LOAD_TIMEOUT = 45
ELEMENT_TIMEOUT = 20


# =========================================================
# FIREBASE
# =========================================================

def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(SERVICE_ACCOUNT_PATH))
        firebase_admin.initialize_app(cred)
    return firestore.client()


# =========================================================
# HELPERS
# =========================================================

def now_ts() -> int:
    return int(time.time())


def save_debug_html(driver, filename: str):
    try:
        path = DEBUG_DIR / filename
        with open(path, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass


def clean_text(value):
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_get_first(lst, default=None):
    return lst[0] if lst and len(lst) > 0 else default


def init_run_date(firestore_db):
    fecha_hoy = datetime.today().strftime("%Y%m%d")
    doc_ref = firestore_db.collection("fechas").document()
    doc_ref.set({
        "fecha": fecha_hoy,
        "timestamp": now_ts(),
    })


def normalizar_texto(texto):
    if not texto:
        return texto
    
    # Normaliza caracteres (ej: Ë → E + ¨)
    texto = unicodedata.normalize('NFKD', texto)
    
    # Elimina acentos/diéresis
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    
    # Opcional: todo en mayúsculas para consistencia
    texto = texto.upper().strip()
    
    return texto

# =========================================================
# SELENIUM
# =========================================================

def setup_driver():
    headless = os.getenv("HEADLESS", "true").lower() == "true"

    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=es-CL")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def safe_get(driver, url: str, wait_seconds: float = 3.0):
    driver.get(url)
    time.sleep(wait_seconds)


# =========================================================
# BRUNO FRITSCH
# =========================================================

def get_brand_name(slug: str) -> str:
    #if slug == "mg":
    #    return "MG"
    #if slug == "omoda-jaecoo"    return "Omoda Jaecoo"
    return slug[0].upper() + slug[1:]


def ensure_brand_doc(firestore_db, brand_id: int, brand_name: str, website: str):
    firestore_db.collection("marcas").document().set({
        "brandID": brand_id,
        "name": brand_name,
        "website": website,
    })


def extract_model_links_from_brand_page(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    links = soup.find_all(id="collection-card")

    modelos = []
    for l in links:
        linksmodelos = re.findall(r'href="(.*?)"', str(l))
        href = safe_get_first(linksmodelos)
        if href:
            modelos.append(href)

    return modelos


def extract_versions_from_model_page(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.find_all(id="new-car-version-card")

    items = []
    for l in cards:
        nmodelo = re.findall(r'css-1ub7r5r">(.*?)</span>', str(l))
        ndetallemodelo = re.findall(r'css-wp624j">(.*?)</span>', str(l))
        ntiposprecio = re.findall(r'css-17fd5p">(.*?)</p>', str(l))
        nprecios = re.findall(r'css-uztjiy">(.*?)</p>', str(l))

        model_name = clean_text(safe_get_first(nmodelo))
        model_detail = clean_text(safe_get_first(ndetallemodelo))

        if not model_name or not model_detail:
            continue

        items.append({
            "model": model_name,
            "modelDetail": model_detail,
            "tiposprecio": [clean_text(x) for x in ntiposprecio],
            "precio": [clean_text(x) for x in nprecios],
        })

    return items


def save_model_doc(firestore_db, brand_id: int, brand_name: str, item: dict):
    #doc_ref = firestore_db.collection("modelos").document()
    #doc_id = doc_ref.id
    modelo = normalizar_texto(item["model"])
    modelo = quitar_palabra(item["model"],brand_name)
    modelo = to_title_custom(modelo)



    #doc_ref.set({
     #   "carID": doc_id,
    #    "model": model,
    #    "modelDetail": item["modelDetail"],
    #    "brandID": brand_id,
    #    "marca": brand_name,
    #    "tiposprecio": item["tiposprecio"],
    #    "precio": item["precio"],
    #    "date_add": now_ts(),
    #    "fuente": SOURCE_NAME,
    #})
    datos = {
                    'modelo': modelo,
                    'marca':brand_name[0].upper() + brand_name[1:],
                    'modelDetail': item["modelDetail"],
                    'tiposprecio': item["tiposprecio"],
                    'precio': item["precio"]
                }
    print(datos)
    saveCar(brand_name[0].upper() + brand_name[1:],datos,'https://www.brunofritsch.cl/')


def bruno(driver, firestore_db):
    stats = {
        "brands_total": len(BRANDS),
        "brands_processed": 0,
        "brand_errors": 0,
        "model_pages_found": 0,
        "model_pages_processed": 0,
        "model_page_errors": 0,
        "versions_found": 0,
        "saved_ok": 0,
        "save_errors": 0,
    }

    for idx, brand_slug in enumerate(BRANDS, start=1):
        brand_name = get_brand_name(brand_slug)
        brand_url = f"https://www.brunofritsch.cl/{brand_slug}"

        print(f"\n=== {brand_name} ===")
        print(f"[INFO] URL marca: {brand_url}")

        try:
            safe_get(driver, brand_url, wait_seconds=4)

            # Espera blanda para que monte el contenido principal
            WebDriverWait(driver, ELEMENT_TIMEOUT).until(
                lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
            )

            model_links = extract_model_links_from_brand_page(driver)
            stats["brands_processed"] += 1
            stats["model_pages_found"] += len(model_links)

            ensure_brand_doc(firestore_db, idx, brand_name, brand_url)

            print(f"[INFO] Modelos encontrados en {brand_name}: {len(model_links)}")

        except Exception as e:
            stats["brand_errors"] += 1
            save_debug_html(driver, f"brand_error_{brand_slug}.html")
            print(f"[WARN] Error cargando marca {brand_name}: {e}")
            traceback.print_exc()
            continue

        for rel_link in model_links:
            model_url = rel_link if rel_link.startswith("http") else f"https://www.brunofritsch.cl/{rel_link}"

            try:
                safe_get(driver, model_url, wait_seconds=4)

                WebDriverWait(driver, ELEMENT_TIMEOUT).until(
                    lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
                )

                version_items = extract_versions_from_model_page(driver)
                stats["model_pages_processed"] += 1
                stats["versions_found"] += len(version_items)

                print(f"[INFO] {model_url} -> versiones encontradas: {len(version_items)}")

                for item in version_items:
                    try:
                        save_model_doc(firestore_db, idx, brand_name, item)
                        stats["saved_ok"] += 1
                        print(item["model"], item["modelDetail"], idx)
                        print("-" * 100)
                    except Exception as e:
                        stats["save_errors"] += 1
                        print(f"[ERROR] save_model_doc falló para {brand_name} / {item.get('model')} / {item.get('modelDetail')}: {e}")
                        traceback.print_exc()

            except Exception as e:
                stats["model_page_errors"] += 1
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", model_url[-80:])
                save_debug_html(driver, f"model_error_{safe_name}.html")
                print(f"[WARN] Error procesando página de modelo {model_url}: {e}")
                traceback.print_exc()

    return stats


# =========================================================
# MAIN
# =========================================================

def main():
    firestore_db = init_firebase()
    init_run_date(firestore_db)

    print("Comenzando Proceso")
    driver = None

    try:
        driver = setup_driver()
        stats = bruno(driver, firestore_db)

    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    summary = {
        "status": "success",
        "source": SOURCE_NAME,
        **stats,
    }

    # Reglas para el orquestador
    if stats["brands_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar ninguna marca")
        sys.exit(1)

    if stats["model_pages_found"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se encontraron páginas de modelo")
        sys.exit(1)

    if stats["model_pages_processed"] == 0:
        summary["status"] = "error"
        print(json.dumps(summary, ensure_ascii=False))
        print("[ERROR] No se pudo procesar ninguna página de modelo")
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

    # Si falla demasiada proporción de páginas de modelo, marcar error
    if stats["model_pages_found"] > 0:
        error_ratio = stats["model_page_errors"] / stats["model_pages_found"]
        summary["error_ratio"] = round(error_ratio, 4)

        if error_ratio >= 0.5:
            summary["status"] = "error"
            print(json.dumps(summary, ensure_ascii=False))
            print(f"[ERROR] Demasiados errores de páginas de modelo: {stats['model_page_errors']} de {stats['model_pages_found']}")
            sys.exit(1)

    print(json.dumps(summary, ensure_ascii=False))
    print("RUN_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()