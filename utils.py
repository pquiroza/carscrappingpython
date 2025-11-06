import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
import time
from marcas import marcas
cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()



def borrar_error():
     col_ref = db.collection("modelos")
     query = col_ref.where("marca", "==", "GWM").stream()
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

