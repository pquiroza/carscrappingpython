import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
import time
from marcas import marcas
cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.cloud.firestore_v1.base_query import FieldFilter



def borrar_error():
     col_ref = db.collection("modelos")
     tz_cl = ZoneInfo("America/Santiago")
     ahora_cl = datetime.now(tz_cl)
     inicio_hoy = datetime(ahora_cl.year, ahora_cl.month, ahora_cl.day, tzinfo=tz_cl)
     fin_hoy = inicio_hoy + timedelta(days=1)

     # Convertir a epoch (segundos)
     inicio_epoch = int(inicio_hoy.timestamp())
     fin_epoch = int(fin_hoy.timestamp())
     print(inicio_epoch,fin_epoch)
     query = (
    col_ref
    .where(filter=FieldFilter("fuente", "==", "Bruno Fritsch"))
    .where(filter=FieldFilter("date_add", ">=", inicio_epoch))
    .where(filter=FieldFilter("date_add", "<", fin_epoch))
)


     try:
          for doc in query.stream():
               print(doc.id, doc.to_dict())
     except Exception as e:
     # Si falta índice compuesto, Firestore típicamente lanza FailedPrecondition con un link para crearlo
          print("Error al ejecutar la consulta:", e)
          print("Suele requerir un índice compuesto: (marca ==) + (fechaRegistro en rango).")


borrar_error()