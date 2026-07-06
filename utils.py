import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
import time
from marcas import marcas
import re
from datetime import datetime
import unicodedata
from zoneinfo import ZoneInfo
from google.cloud.firestore_v1.base_query import FieldFilter


cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()


def quitar_palabra(texto, palabra):
    if not texto or not palabra:
        return texto
    
    # regex para eliminar la palabra completa (case insensitive)
    patron = r'\b' + re.escape(palabra) + r'\b'
    
    resultado = re.sub(patron, '', texto, flags=re.IGNORECASE)
    
    # limpiar espacios extra
    resultado = re.sub(r'\s+', ' ', resultado).strip()
    
    return resultado


def normalize_text(text: str) -> str:
    if not text:
        return ""

    # lowercase
    text = text.lower()

    # quitar acentos
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    # reemplazar separadores comunes por espacio
    text = re.sub(r"[\/\-\_]", " ", text)

    # quitar todo lo que no sea alfanumérico o espacio
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # colapsar espacios
    text = re.sub(r"\s+", " ", text).strip()

    return text

def tokenize(text: str, min_len: int = 2) -> list[str]:
    norm = normalize_text(text)
    tokens = norm.split(" ")

    # filtrar tokens cortos / basura
    tokens = [
        t for t in tokens
        if len(t) >= min_len and not t.isdigit()
    ]

    return sorted(set(tokens))


def build_model_search_fields(model: str, model_detail: str | None = None) -> dict:
    base = model or ""
    detail = model_detail or ""

    combined = f"{base} {detail}".strip()

    norm = normalize_text(combined)
    tokens = tokenize(combined)

    return {
        "model_norm": norm,
        "model_tokens": tokens
    }


    
def guarda_yapo(record):
     datos_b = build_model_search_fields(record['modelo'], record['titulo'])
     try:
          id_marca = marcas[record['marca']]
     except:
          id_marca = 999
     datac = {
          'carID': record['ad_id'],
          'model': record['modelo'],
          'modelDetail':record['titulo'],
          'brandID': id_marca,
          'marca':record['marca'],
          'precio': record['precio'],
          'date_add':record['fecha_publicado_iso'],
          'fuente': record['url'],
          'kilometraje': record['kilometros'],
          'combustible': record['combustible'],
          'anio': record['anio'],
          'transmision': record['transmision'],
          'model_norm': datos_b['model_norm'],
          'model_tokens': datos_b['model_tokens'],
          'origen': 'yapo.cl'
     }
     doc_ref = db.collection("usados").document()
     print(datac)
     doc_id = doc_ref.id
     doc_ref.set(datac)

def guarda_usado(record):
     datos_b = build_model_search_fields(record['model_list'], record['model_detail'])
     try:
          id_marca = marcas[record['make_list']]
     except:
          id_marca = 999 
     datac = {
          'carID': record['listing_id'],
          'model': record['model_list'],
          'modelDetail':record['model_detail'],
          'brandID': id_marca,
          'marca':record['make_list'],
          'precio': record['price_list'],
          'date_add':record['run_date'],
          'fuente': record['detail_url'],
          'kilometraje': record['km_detail'],
          'combustible': record['fuel_detail'],
          'anio': record['year_detail'],
          'transmision': record['transmission_detail'],
          'color': record['color_detail'],
          'model_norm': datos_b['model_norm'],
          'model_tokens': datos_b['model_tokens'],
          'origen': 'chileautos.cl'


     }
     doc_ref = db.collection("usados").document()
     print(datac)
     doc_id = doc_ref.id
     doc_ref.set(datac)
     
def guarda_autocl(record):
    datos_b = build_model_search_fields(record['modelo'], record['version'])
    try:
        id_marca = marcas[record['marca']]
    except:
        id_marca = 999
    datac = {
        'carID': record['id'],
        'model': record['modelo'],
        'modelDetail': record['version'],
        'brandID': id_marca,
        'marca': record['marca'],
        'precio': record['precio'],
        'date_add': record['scraped_at'],
        'fuente': record['url'],
        'kilometraje': record['kilometraje'],
        'combustible': record['combustible'],
        'anio': record['año'],
        'transmision': record['transmision'],
        'model_norm': datos_b['model_norm'],
<<<<<<< HEAD
        'model_tokens': datos_b['model_tokens']
=======
        'model_tokens': datos_b['model_tokens'],
        'origen': 'auto.cl'
>>>>>>> 4bbf2dc (nuevos procesos)
    }
    doc_ref = db.collection("usados").document()
    print(datac)
    doc_id = doc_ref.id
    doc_ref.set(datac)
     
def borrar_error():
     col_ref = db.collection("modelos")
     query = col_ref.where("marca", "==", "Chevrolet").stream()
     count = 0
     for doc in query:
          print(f"Borrando {doc.id}")
          doc.reference.delete()
          count += 1

     print(f"✅ Se eliminaron {count} documentos de Suzuki.")
def to_title_custom(texto: str) -> str:
    excepciones = {"de", "del", "la", "el", "y", "en", "por", "a"}
    palabras = texto.lower().split()
    return " ".join(
        [p.capitalize() if p not in excepciones else p for p in palabras]
    )
def saveCar2(marca,datos,fuente):
    
    id_marca = marcas[marca]
    doc_ref = db.collection("modelos").document()
    print(doc_ref)
    doc_id = doc_ref.id
    print(doc_id)
    print(datos)
    
    arreglo = {
         'carID': doc_id,
    'model': datos['modelo'],
    'modelDetail':datos['modelDetail'],
    'brandID': id_marca,
    'marca':marca,
    'tiposprecio':datos['tiposprecio'],
    'precio':datos['precio'],
    'date_add':int(time.time()),
    'fuente': fuente 
    }
    print(arreglo)
    doc_ref.set(arreglo)
    print(f"Guardando {marca} {datos}")




def guardaSpecs(marca,modelo,version,datos):
     id_marca = marcas[marca]
     doc_ref = db.collection("especificaciones").document()
     arreglo = {
        'brandID': id_marca,
        'marca': marca,
        'modelo': modelo,
        'version': version,
        'specs': datos,
        'date_add':int(time.time())
     }
     doc_ref.set(arreglo)


def saveCar(marca, datos, fuente):
    
    id_marca = marcas[marca]

    # Buscar si ya existe un modelo previo con misma marca + modelo + modelDetail
<<<<<<< HEAD
    existing_docs = db.collection("modelos") \
        .where("marca", "==", marca) \
        .where("model", "==", datos['modelo']) \
        .where("modelDetail", "==", datos['modelDetail']) \
        .limit(1) \
        .stream()
=======
    existing_docs = (
    db.collection("modelos")
    .where(filter=FieldFilter("marca", "==", marca))
    .where(filter=FieldFilter("model", "==", datos['modelo']))
    .limit(1)
    .stream()
    )
>>>>>>> 4bbf2dc (nuevos procesos)

    # Extraer categoria y origen si existen
    categoria = None
    origen = None
    for doc in existing_docs:
        existing_data = doc.to_dict()
        categoria = existing_data.get("categoria")
        origen = existing_data.get("origen")
        print(f"Datos previos encontrados → categoria: {categoria}, origen: {origen}")
        break

    doc_ref = db.collection("modelos").document()
    doc_id = doc_ref.id

    arreglo = {
        'carID': doc_id,
        'model': datos['modelo'],
        'modelDetail': datos['modelDetail'],
        'brandID': id_marca,
        'marca': marca,
        'tiposprecio': datos['tiposprecio'],
        'precio': datos['precio'],
        'date_add': int(time.time()),
        'fuente': fuente,
        'categoria': categoria,
        'origen': origen,
    }

    print(arreglo)
    doc_ref.set(arreglo)
<<<<<<< HEAD
=======
    print(f"Guardando {marca} {datos}")



def convertir_date_add_a_timestamp(date_add: str) -> int:
    
    dt = datetime.strptime(date_add, "%Y-%m-%d")
    dt = dt.replace(tzinfo=ZoneInfo("America/Santiago"))
    return int(dt.timestamp())

def saveCarDate(marca, datos, fuente,date_add):
    
    id_marca = marcas[marca]

        # Buscar si ya existe un modelo previo con misma marca + modelo + modelDetail
    existing_docs = db.collection("modelos") \
        .where("marca", "==", marca) \
        .where("model", "==", datos['modelo']) \
        .limit(1) \
        .stream()

        # Extraer categoria y origen si existen
    categoria = None
    origen = None
    for doc in existing_docs:
        existing_data = doc.to_dict()
        categoria = existing_data.get("categoria")
        origen = existing_data.get("origen")
        print(f"Datos previos encontrados → categoria: {categoria}, origen: {origen}")
        break

    doc_ref = db.collection("modelos").document()
    doc_id = doc_ref.id
    timestamp_date_add = convertir_date_add_a_timestamp(date_add)

    arreglo = {
            'carID': doc_id,
            'model': datos['modelo'],
            'modelDetail': datos['modelDetail'],
            'brandID': id_marca,
            'marca': marca,
            'tiposprecio': datos['tiposprecio'],
            'precio': datos['precio'],
            'date_add': timestamp_date_add,
            'fuente': fuente,
            'categoria': categoria,
            'origen': origen,
        }

    print(arreglo)
    doc_ref.set(arreglo)
>>>>>>> 4bbf2dc (nuevos procesos)
    print(f"Guardando {marca} {datos}")