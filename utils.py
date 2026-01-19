import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
import time
from marcas import marcas
import re
import unicodedata
cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()


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
          'model_tokens': datos_b['model_tokens']
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
          'model_tokens': datos_b['model_tokens']


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
def saveCar(marca,datos,fuente):
    
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

