# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import time

URL = "https://www.mazda.cl/busqueda"  # <-- cámbiala

def start_driver(headless=False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,1000")
    driver = webdriver.Chrome(options=opts)
    return driver

def wait_css(driver, css, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css))
    )

def get_model_names(driver):
    """
    Devuelve una lista con los nombres exactos (texto del <label>) de cada modelo.
    """
    # Esperar contenedor visible
    wait_css(driver, "div.plp_filter__show")
    # El contenedor con scroll
    scroll = wait_css(driver, ".plp_scroll__container")
    ul = wait_css(driver, "ul#plp_list__Modelo")

    # Hacer un pequeño 'scroll sweep' por si la lista es más larga y necesita renderizar elementos diferidos
    driver.execute_script("arguments[0].scrollTop = 0;", scroll)
    time.sleep(0.2)
    last_height = -1
    # Scroll hasta que no cambie más la altura (simple approach para listas largas)
    for _ in range(20):
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scroll)
        time.sleep(0.2)
        new_height = driver.execute_script("return arguments[0].scrollTop;", scroll)
        if new_height == last_height:
            break
        last_height = new_height

    # Tomar todos los <li> y leer el texto del label
    items = ul.find_elements(By.CSS_SELECTOR, "li.plp_checkbox__items.plp_items__Modelo")
    modelos = []
    for li in items:
        try:
            label = li.find_element(By.CSS_SELECTOR, "label.plp_label__checkbox")
            texto = label.text.strip()
            if texto:
                modelos.append(texto)
        except Exception:
            pass
    # Quitar duplicados preservando orden
    seen = set()
    modelos = [m for m in modelos if not (m in seen or seen.add(m))]
    return modelos

def click_model_by_text(driver, modelo_texto):
    """
    Hace click en el checkbox/label del modelo, realizando scroll dentro del contenedor si hace falta.
    """
    scroll = wait_css(driver, ".plp_scroll__container")
    ul = wait_css(driver, "ul#plp_list__Modelo")

    # Buscar el label por su texto (case-sensitive tal como aparece)
    # Estrategia: intentar directamente; si no está visible, scrollear y reintentar
    def _find_label():
        labels = ul.find_elements(By.CSS_SELECTOR, "label.plp_label__checkbox")
        for lab in labels:
            if lab.text.strip() == modelo_texto:
                return lab
        return None

    label = _find_label()
    # si no se encontró, recorrer con scroll (por si la lista es virtualizada/larga)
    if label is None:
        # Hacer scroll incremental
        for _ in range(30):
            driver.execute_script("arguments[0].scrollTop += 180;", scroll)
            time.sleep(0.08)
            label = _find_label()
            if label is not None:
                break

    if label is None:
        raise RuntimeError(f"No pude encontrar el modelo '{modelo_texto}' en la lista.")

    # Intentar clickear el label (más robusto que el input)
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)
        time.sleep(0.05)
        label.click()
    except ElementClickInterceptedException:
        # Forzar click vía JavaScript si algún overlay molesta
        driver.execute_script("arguments[0].click();", label)

    # (Opcional) Verificar que el checkbox quedó marcado
    li = label.find_element(By.XPATH, "./ancestor::li[1]")
    checkbox = li.find_element(By.CSS_SELECTOR, "input.plp_input__checkbox")
    checked = checkbox.is_selected()
    return checked

def main():
    driver = start_driver(headless=False)
    try:
        driver.get(URL)

        # Asegurar que la lista esté cargada
        wait_css(driver, "ul#plp_list__Modelo")

        # 1) Extraer modelos
        modelos = get_model_names(driver)
        print("Modelos encontrados:", modelos)

        # 2) Seleccionarlos uno a uno
        for nombre in modelos:
            ok = click_model_by_text(driver, nombre)
            print(f"Seleccionado '{nombre}': {ok}")
            # Si la UI aplica filtros al instante y oculta la lista, quizá debas reabrir el filtro o desmarcar.
            # Aquí pausamos levemente para que el sitio re-renderice.
            time.sleep(0.3)

        # --- Ejemplos extra útiles ---

        # Si quieres seleccionar solo algunos específicos:
        # objetivos = ["MAZDA CX-90", "MAZDA 3"]
        # for n in objetivos:
        #     ok = click_model_by_text(driver, n)
        #     print(f"Seleccionado '{n}': {ok}")
        #     time.sleep(0.3)

        # Si quieres usar el buscador ("Buscar Modelo") antes de clickear:
        # finder = wait_css(driver, "input#plp_finder__Modelo")
        # finder.clear()
        # finder.send_keys("CX-")
        # time.sleep(0.4)
        # ok = click_model_by_text(driver, "MAZDA CX-5")

    except TimeoutException:
        print("❌ Timeout esperando elementos del filtro de modelos.")
    finally:
        # driver.quit()
        pass  # deja abierto el navegador para inspección mientras pruebas

if __name__ == "__main__":
    main()
