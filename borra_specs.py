import firebase_admin
from firebase_admin import credentials, firestore

# 🔐 Ruta a tu service account
cred = credentials.Certificate('carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json')

firebase_admin.initialize_app(cred)

db = firestore.client()

def delete_mazda():
    collection_ref = db.collection("especificaciones")

    query = collection_ref.where("marca", "==", "Subaru")

    docs = query.stream()

    deleted = 0
    batch = db.batch()
    batch_size = 0

    for doc in docs:
        batch.delete(doc.reference)
        batch_size += 1
        deleted += 1

        # 🔥 Firestore permite máximo 500 por batch
        if batch_size == 500:
            batch.commit()
            print(f"🧹 Eliminados {deleted} documentos...")
            batch = db.batch()
            batch_size = 0

    # Últimos pendientes
    if batch_size > 0:
        batch.commit()

    print(f"✅ Eliminación completada. Total eliminados: {deleted}")


if __name__ == "__main__":
    delete_mazda()