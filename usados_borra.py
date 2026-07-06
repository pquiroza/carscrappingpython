import firebase_admin
from firebase_admin import credentials, firestore
from collections import defaultdict

cred = credentials.Certificate("carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

DELETE_MODE = True
COLLECTION_NAME = "usados"

grupos = defaultdict(list)

docs = db.collection(COLLECTION_NAME).stream()

for doc in docs:
    data = doc.to_dict() or {}
    fuente = str(data.get("fuente") or "").strip().lower()

    if not fuente:
        continue

    if "auto.cl" not in fuente:
        continue

    if fuente.endswith("/"):
        fuente = fuente[:-1]

    grupos[fuente].append(doc.id)

for fuente, doc_ids in grupos.items():
    if len(doc_ids) > 1:
        print("=" * 100)
        print(f"FUENTE DUPLICADA: {fuente}")
        print(f"mantener: {doc_ids[0]}")
        print(f"borrar:   {doc_ids[1:]}")
        print()

        if DELETE_MODE:
            batch = db.batch()
            for doc_id in doc_ids[1:]:
                ref = db.collection(COLLECTION_NAME).document(doc_id)
                batch.delete(ref)
            batch.commit()