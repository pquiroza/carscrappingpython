import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime


# =========================
# CONFIG FIREBASE
# =========================
cred = credentials.Certificate("carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json")

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

db = firestore.client()


# =========================
# HELPERS
# =========================
def format_ts(value):
    """
    Intenta mostrar bonito date_add si viene en epoch.
    """
    if isinstance(value, int):
        try:
            return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return value
    return value


# =========================
# BUSCAR DOCUMENTOS
# =========================
def buscar_modelos(
    marca: str | None = None,
    modelo: str | None = None,
    version: str | None = None,
    collection_name: str = "modelos",
):
    """
    Busca documentos por marca, modelo y versión.
    Ajusta los nombres de campos si en tu colección difieren.
    """

    query = db.collection(collection_name)

    if marca:
        query = query.where(filter=FieldFilter("marca", "==", marca))

    if modelo:
        query = query.where(filter=FieldFilter("model", "==", modelo))

    if version:
        query = query.where(filter=FieldFilter("modelDetail", "==", version))

    encontrados = []

    try:
        for doc in query.stream():
            data = doc.to_dict()

            item = {
                "id": doc.id,
                "marca": data.get("marca"),
                "model": data.get("model"),
                "modelDetail": data.get("modelDetail"),
                "precio": data.get("precio"),
                "tiposprecio": data.get("tiposprecio"),
                "date_add": data.get("date_add"),
                "date_add_fmt": format_ts(data.get("date_add")),
            }
            encontrados.append(item)

        print("\n================ RESULTADOS ================\n")

        if not encontrados:
            print("No se encontraron documentos.")
            return []

        for i, item in enumerate(encontrados, start=1):
            print(f"[{i}] ID: {item['id']}")
            print(f"    marca       : {item['marca']}")
            print(f"    model       : {item['model']}")
            print(f"    modelDetail : {item['modelDetail']}")
            print(f"    date_add    : {item['date_add']} ({item['date_add_fmt']})")
            print(f"    precio      : {item['precio']}")
            print(f"    tiposprecio : {item['tiposprecio']}")
            print("-" * 60)

        print(f"Total encontrados: {len(encontrados)}")
        return encontrados

    except Exception as e:
        print("Error al buscar documentos:", e)
        print("Puede requerir índice compuesto.")
        raise


# =========================
# BORRAR POR ID
# =========================
def borrar_por_id(
    doc_id: str,
    collection_name: str = "modelos",
    dry_run: bool = True,
):
    """
    Borra un documento específico por su document ID.
    """

    doc_ref = db.collection(collection_name).document(doc_id)
    snap = doc_ref.get()

    if not snap.exists:
        print(f"No existe documento con id: {doc_id}")
        return

    data = snap.to_dict()

    print("\n=========== DOCUMENTO SELECCIONADO ===========")
    print(f"ID          : {snap.id}")
    print(f"marca       : {data.get('marca')}")
    print(f"model       : {data.get('model')}")
    print(f"modelDetail : {data.get('modelDetail')}")
    print(f"date_add    : {data.get('date_add')} ({format_ts(data.get('date_add'))})")
    print(f"precio      : {data.get('precio')}")
    print("=============================================\n")

    if dry_run:
        print("Modo DRY RUN: no se borró nada.")
        return

    doc_ref.delete()
    print(f"✅ Documento borrado: {doc_id}")


# =========================
# FLUJO INTERACTIVO
# =========================
def revisar_y_borrar():
    print("=== Buscar duplicados Hyundai ===")
    marca = input("Marca: ").strip() or None
    modelo = input("Modelo: ").strip() or None
    version = input("Versión (modelDetail): ").strip() or None

    resultados = buscar_modelos(
        marca=marca,
        modelo=modelo,
        version=version,
    )

    if not resultados:
        return

    doc_id = input("\nPega el ID exacto del documento que quieres borrar: ").strip()
    if not doc_id:
        print("No ingresaste ID.")
        return

    confirmar = input("Escribe DELETE para borrar de verdad, o Enter para simular: ").strip()

    borrar_por_id(
        doc_id=doc_id,
        dry_run=(confirmar != "DELETE")
    )


# =========================
# EJEMPLOS DIRECTOS
# =========================

# 1) Buscar Hyundai Tucson versión específica
# buscar_modelos(
#     marca="Hyundai",
#     modelo="Tucson",
#     version="1.6T AT GLS"
# )

# 2) Simular borrado por ID
# borrar_por_id(
#     doc_id="TU_DOC_ID_AQUI",
#     dry_run=True
# )

# 3) Borrar de verdad por ID
# borrar_por_id(
#     doc_id="TU_DOC_ID_AQUI",
#     dry_run=False
# )

# 4) Flujo interactivo completo
revisar_y_borrar()