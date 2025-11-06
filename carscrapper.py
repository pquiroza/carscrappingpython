import os
import shutil
import glob
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import Select
from datetime import datetime
import requests



import logging
from bs4 import BeautifulSoup
from lxml import html
from lxml import etree
import re


import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore


cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()
class Auto:
    
    def __init__(self,id,marca,modelo,categoria,precio,bonos,caracteristicas):
        self.id = id
        self.marca = marca
        self.modelo = modelo
        self.categoria = categoria
        self.precio = precio
        self.bonos = bonos
        self.caracteristicas = caracteristicas
        

def carga_basefob():
    doc_ref = db.collection("modelos")
    docs = doc_ref.stream()
    
    for doc in docs:
        print(doc.get('marca'))
        print(f'{doc.id} => {doc.to_dict()}')
        fob_ref = db.collection("datos_fob")
        query = (
            fob_ref.where("marca","==",doc.get('marca'))
            .where('model','==',doc.get('model'))
            .where('modelDetail','==',doc.get('modelDetail'))
        )
        resultados = query.stream()
        lista = list(resultados)
        if not lista:
            print("no eta")
            doc_ref = db.collection("datos_fob").document()
            doc_id = doc_ref.id
            doc_ref.set({
            'fobId': doc_id,
            'model': doc.get('model'),
            'modelDetail':doc.get('modelDetail'),
            'brandID': doc.get('brandID'),
            'marca':doc.get('marca'),
            'origen':'',
            'seguro':0,
            'flete':0,
            'iva':0,
            'cif':0,
            'fob':0,
            'preciofob':0,
            'date_add':int(time.time()),
            
                })
            
        


def setup_driver():
    options = Options()
    #options.add_argument("--no-sandbox")
    #options.add_argument("--disable-dev-shm-usage")
    #options.add_argument("--headless")  # Uncomment if running headless is required
    #options.add_argument("--disable-gpu")
    #options.add_argument("--disable-extensions")
    #options.add_argument('--window-size=1600,900')

    #options.add_argument("--disable-search-engine-choice-screen")
    
    prefs = {
        "download.default_directory": '',
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    #options.add_experimental_option("prefs", prefs)
    options = Options()
    options.add_argument("—headless")
    print(options)
    service = Service('/opt/homebrew/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def chevi(driver):
    
    
    url = "https://www.chevrolet.cl"
    driver.get(url)
    button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="generic_template_page-e533887071"]/gb-global-nav//nav/button[1]')))
    
    
    
    

def chevrolet(driver):
    
    
    modelos_urls = [["https://secure-developments.com/commonwealth/chile/gm_specs/sail",True],["https://secure-developments.com/commonwealth/chile/gm_specs/SAIL-copy-1714102720",False],["https://secure-developments.com/commonwealth/chile/gm_specs/onix-sedan"
,False],["https://secure-developments.com/commonwealth/chile/gm_specs/onix_premier",False],["https://secure-developments.com/commonwealth/chile/gm_specs/groove",False],["https://secure-developments.com/commonwealth/chile/gm_specs/all_new_tracker",True],["https://secure-developments.com/commonwealth/chile/gm_specs/captiva",False],["https://secure-developments.com/commonwealth/chile/gm_specs/bolt",False],["https://secure-developments.com/commonwealth/chile/gm_specs/traverse",False],["https://secure-developments.com/commonwealth/chile/gm_specs/tahoe",False],["https://secure-developments.com/commonwealth/chile/gm_specs/suburban",False]]
    modelos = ['https://www.chevrolet.cl/autos/sail-sedan','https://www.chevrolet.cl/autos/sail-hatchback']
    #url = "https://secure-developments.com/commonwealth/chile/gm_specs/sail"
    #url = "https://secure-developments.com/commonwealth/chile/gm_specs/SAIL-copy-1714102720"
    #url ="https://secure-developments.com/commonwealth/chile/gm_specs/onix-sedan"
    #url = "https://secure-developments.com/commonwealth/chile/gm_specs/onix_premier"
    #url = "https://secure-developments.com/commonwealth/chile/gm_specs/groove"
    url = "https://secure-developments.com/commonwealth/chile/gm_specs/all_new_tracker"
    
    for m in modelos_urls:
        #print(m[0],m[1])
        driver.get(m[0])
        time.sleep(5)
        if (m[1]):
            boton_cierra = driver.find_element(By.XPATH,'//*[@id="remove-car"]')
            driver.execute_script('arguments[0].click()',boton_cierra) 
        
        time.sleep(5)
        
        select_element = driver.find_element(By.XPATH,'//*[@id="cars-header-images"]/div[2]/div[2]')
        driver.execute_script('arguments[0].click()', select_element)

        
        select_element2 = driver.find_element(By.XPATH,' //*[@id="cars-header-images"]/div[2]/div[2]/select')
        
        
        driver.execute_script('arguments[0].style.display = "block";', select_element2)
        select = Select(select_element2)
        i = 0
        etiquetas = []
            
        for s in select.options:
            etiquetas.append(s.text)
                
    
        i=0
        for s in etiquetas:
            select_element2 = driver.find_element(By.XPATH,' //*[@id="cars-header-images"]/div[2]/div[2]/select')
            driver.execute_script('arguments[0].style.display = "block";', select_element2)
            select = Select(select_element2)
            
            select.select_by_index(i)
            
            time.sleep(10)
            soup = BeautifulSoup(driver.page_source, 'lxml')
        
            precio = re.findall(r'<div class="price">(.*?)</div>',str(soup))
            print(s,precio)
            ts = int(time.time())
                
            doc_ref = db.collection("autos").document(str(ts))
            doc_ref.set({"marca":"Chevrolet","modelo":s,"precio":precio[0],"extras":""})    
                
            i = i+1
                    
            
    
    #code = driver.execute_script("return document.body.innerHTML")
    
 
    
    
    
    #button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="select2-model-dropdown-add-container"]')))
    #driver.execute_script('arguments[0].click()', button)
    #button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="select2-model-dropdown-add-result-xyof-1.5L_LTZ_MT_S"]')))
    #driver.execute_script('arguments[0].click()', button)
    
    
    
   
    
    
    

 

 




    
 
   
    


def toyota(driver):
    url = "https://toyota.cl/modelos/destacados/"
    driver.get(url)
    listamodelos = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links = soup.select(".sibling-nav-container")
    modelos = re.findall(r'href="(.*?)"',str(links))
    for m in modelos: #recorrer los links de los distintas clases de vehiculos
        
        driver.get(m)
        
        
        
        soup2 = BeautifulSoup(driver.page_source, 'html.parser')
        autos = soup2.select(".items-cars-container")
        for a in autos:
            
            linksmodelos = re.findall(r'href="(.*?)"',str(a))
            for l in linksmodelos:
                listamodelos.append(l)
                
    
    
    
    
    for l in listamodelos:
        
        timeout = 10  # seconds
        
        driver.get(l)
        time.sleep(10)

        soup = BeautifulSoup(driver.page_source,'html.parser')
        tags = soup.select(".card-body")
        for t in tags:
            
            modelo = re.findall(r'<div><h3>(.*?)</h3>',str(t))
            precio = re.findall(r'<p><small>(.*?)<sup>',str(t))
            bonos = re.findall(r'incluye:</p>(.*?)</ul>',str(t))
            if(len(bonos)>0):
                b = bonos[0].replace('<ul>',"")
                b = b.replace('<strong>',"")
                b = b.replace('</strong>',"")
                b = b.replace('</li>',"")
                b = b.replace('<li>',"")
            if (len(modelo)>0):
                ts = int(time.time())
                
                doc_ref = db.collection("autos").document(str(ts))
                doc_ref.set({"marca":"Toyota","modelo":modelo[0],"precio":precio[0],"extras":b})

                print(modelo,precio,b)
                
                
        

    
    
    
    
    
    
    
    
  
        
    

    



    


def bruno(driver):
    brunos = ["toyota","nissan","peugeot","citroen","ram","chery","mg","lexus","hyundai","opel","jeep","fiat","exeed","omoda-jaecoo"]
    id=0
    for b in brunos: 
        id+=1
        url = "https://www.brunofritsch.cl/"+b    
        driver.get(url)
        #print(url)
        time.sleep(5)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        links = soup.find_all(id="collection-card")
        #linksmodelos = re.findall(r'href="(.*?)"',str(soup))
        modelos = []
        #print(soup.prettify()[:1000])
        for l in links:
            linksmodelos = re.findall(r'href="(.*?)"',str(l))
            modelos.append(linksmodelos[0])
        
        doc_ref = db.collection("marcas").document()
        doc_id = doc_ref.id
        doc_ref.set({
            'brandID': id,
            'name': b[0].upper() + b[1:],
            'website': url
        })
        print(id,b,url)
        
        for m in modelos:
            url = "https://www.brunofritsch.cl/"+m
            driver.get(url)
            #print(url)
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            links = soup.find_all(id="new-car-version-card")
            modelosautos = []
            for l in links:
         
                nmodelo = re.findall(r'css-1ub7r5r">(.*?)</span>',str(l))
                
                ndetallemodelo = re.findall(r'css-wp624j">(.*?)</span>',str(l))
                ndetalleprecio = re.findall(r'css-uztjiy">(.*?)</p>',str(l))
                ntiposprecio = re.findall(r'css-17fd5p">(.*?)</p>',str(l))
                nprecios = re.findall(r'css-uztjiy">(.*?)</p>',str(l))
                
                #print(nmodelo,ndetallemodelo,ndetalleprecio,ntiposprecio,nprecios)
                print(nmodelo[0],ndetallemodelo[0],id)
       
                precios = []
                for i in range(len(ntiposprecio)):
                    p = [ntiposprecio[i],nprecios[i]]
                    precios.append(p)
                
                
                
                
                doc_ref = db.collection("modelos").document()
                doc_id = doc_ref.id
                doc_ref.set({
                'carID': doc_id,
                'model': nmodelo[0],
                'modelDetail':ndetallemodelo[0],
                'brandID': id,
                'marca':b[0].upper() + b[1:],
                'tiposprecio':ntiposprecio,
                'precio':nprecios,
                'date_add':int(time.time()),
                'fuente': 'Bruno Fritsch'
                })
                    
                print("-"*100)



def mazda(driver):
    URL = "https://www.mazda.cl/formulario/cotizacion/?utm_source=google&utm_medium=cpa&utm_campaign=cl_ao_mazda_conversion_marca_abr2025_mantenciones_google_search_&utm_term=%7Bmazda%7D&utm_content=branding_search_ene2025_brand_abierto&https%3A%2F%2Fwww.mazda.cl=&gad_source=1&gad_campaignid=19319337507&gbraid=0AAAAADqpjI_vrK9MbuLOndw7oQRHUr9yc&gclid=CjwKCAiAwqHIBhAEEiwAx9cTeZPCj0b8O0Z87IzeUPpDOflG_fGzrWqylVUNrGvTEyQskMKVc-_bcxoC0XQQAvD_BwE"  # cámbiala si la estructura está en otra URL

    # Descargar HTML
    print(f"Descargando contenido de {URL} ...")
    resp = requests.get(URL)
    resp.raise_for_status()

    # Analizar con BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")

    # Base URL para concatenar rutas relativas
    base_url = "https://www.mazda.cl"

    modelos = []
    for card in soup.select(".card-wrapper-quotation"):
        onclick = card.get("onclick", "")
        code = onclick.split("`")[1] if "`" in onclick else None

        name_tag = card.select_one(".name")
        name = name_tag.get_text(strip=True) if name_tag else None

        img_tag = card.select_one("img")
        img_src = img_tag["src"] if img_tag and img_tag.get("src") else None
        if img_src and img_src.startswith("/"):
            img_src = base_url + img_src

        if code and name and img_src:
            modelos.append({
                "codigo": code,
                "nombre": name,
                "imagen": img_src
            })

    # Mostrar resultados
    print("\n=== MODELOS DETECTADOS ===")
    for m in modelos:
        print(f"{m['codigo']:8} | {m['nombre']:20} | {m['imagen']}")

    print(f"\nTotal modelos encontrados: {len(modelos)}")
    


def main():
    
    
    #carga_basefob()
    #exit(0)
    
    fecha_hoy = datetime.today().strftime('%Y%m%d')
    

    doc_ref = db.collection("fechas").document()
    doc_id = doc_ref.id
    doc_ref.set({
            'fecha': fecha_hoy,
            'timestamp': int(time.time()),
          
        })
    
    print("Comenzando Proceso")
    driver = setup_driver()

    bruno(driver)
if __name__ == "__main__":
    main()




