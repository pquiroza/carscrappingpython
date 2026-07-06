import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime

CREDENTIALS_FILE = "carscrapping-2225c-firebase-adminsdk-fbsvc-6abe929cb8.json"
COLLECTION_NAME = "modelos"
DRY_RUN = False

CAMPOS_NORMALIZAR = ["categoria", "origen", "tipomotor"]

if not firebase_admin._apps:
    cred = credentials.Certificate(CREDENTIALS_FILE)
    firebase_admin.initialize_app(cred)

db = firestore.client()


def esta_vacio(valor):
    if valor is None:
        return True
    if isinstance(valor, str) and valor.strip() == "":
        return True
    return False


def normalizar(valor):
    if valor is None:
        return ""
    return " ".join(str(valor).strip().lower().split())


def date_add_num(data):
    v = data.get("date_add")
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return v
    try:
        return int(v)
    except Exception:
        return 0


def limpiar_data_para_mostrar(data):
    campos = [
        "marca",
        "model",
        "modelDetail",
        "version",
        "categoria",
        "origen",
        "tipomotor",
        "source",
        "date_add"
    ]
    return {k: data.get(k) for k in campos if k in data}


def buscar_docs_incompletos():
    incompletos = []
    total = 0

    for doc in db.collection(COLLECTION_NAME).stream():
        total += 1
        data = doc.to_dict() or {}

        if any(esta_vacio(data.get(campo)) for campo in CAMPOS_NORMALIZAR):
            incompletos.append({
                "id": doc.id,
                "data": data
            })

    print(f"Total docs revisados: {total}")
    return incompletos


def construir_indice():
    indice = {}
    total = 0

    for doc in db.collection(COLLECTION_NAME).stream():
        data = doc.to_dict() or {}

        marca_norm = normalizar(data.get("marca"))
        model_norm = normalizar(data.get("model"))

        if not marca_norm or not model_norm:
            continue

        key = f"{marca_norm}|||{model_norm}"

        indice.setdefault(key, []).append({
            "id": doc.id,
            "data": data
        })

        total += 1

    for key in indice:
        indice[key].sort(
            key=lambda x: date_add_num(x["data"]),
            reverse=True
        )

    print(f"Índice construido. Docs indexados: {total}. Claves: {len(indice)}")
    return indice


def buscar_candidato_para_campo(indice, marca, model, doc_id_actual, campo):
    key = f"{normalizar(marca)}|||{normalizar(model)}"

    candidatos = indice.get(key, [])

    for item in candidatos:
        if item["id"] == doc_id_actual:
            continue

        data = item["data"]

        if not esta_vacio(data.get(campo)):
            return {
                "id": item["id"],
                "data": data,
                "date_add": date_add_num(data)
            }

    return None


def exportar_sin_candidato(registros):
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"sin_candidato_normalizacion_{fecha}.json"

    with open(nombre_archivo, "w", encoding="utf-8") as f:
        json.dump(registros, f, indent=2, ensure_ascii=False)

    print(f"\nJSON exportado: {nombre_archivo}")


def procesar_normalizacion(dry_run=True, limitar=None):
    incompletos = buscar_docs_incompletos()
    indice = construir_indice()

    items = incompletos[:limitar] if limitar else incompletos

    resumen_campos = {
        campo: {
            "vacios_detectados": 0,
            "actualizados": 0,
            "sin_candidato": 0
        }
        for campo in CAMPOS_NORMALIZAR
    }

    sin_candidato_export = []

    omitidos = 0
    docs_actualizables = 0

    batch = db.batch()
    batch_count = 0
    batch_limit = 400

    print(f"\nDocs incompletos: {len(items)}\n")

    for i, item in enumerate(items, start=1):
        doc_id = item["id"]
        data = item["data"]

        marca = data.get("marca")
        model = data.get("model")

        print("=" * 100)
        print(f"[{i}/{len(items)}] DOC INCOMPLETO")
        print(f"ID: {doc_id}")
        print(json.dumps(limpiar_data_para_mostrar(data), indent=2, ensure_ascii=False))

        if esta_vacio(marca) or esta_vacio(model):
            print(">> OMITIDO: sin marca/model")
            omitidos += 1

            sin_candidato_export.append({
                "id": doc_id,
                "motivo": "sin marca o model",
                "campos_sin_candidato": [
                    campo for campo in CAMPOS_NORMALIZAR
                    if esta_vacio(data.get(campo))
                ],
                "documento": limpiar_data_para_mostrar(data)
            })

            continue

        updates = {}
        campos_sin_candidato_doc = []

        for campo in CAMPOS_NORMALIZAR:
            if not esta_vacio(data.get(campo)):
                continue

            resumen_campos[campo]["vacios_detectados"] += 1

            candidato = buscar_candidato_para_campo(
                indice=indice,
                marca=marca,
                model=model,
                doc_id_actual=doc_id,
                campo=campo
            )

            if candidato:
                updates[campo] = candidato["data"].get(campo)

                print(f"\nCANDIDATO PARA {campo}:")
                print(json.dumps({
                    "campo": campo,
                    "valor": candidato["data"].get(campo),
                    "id": candidato["id"],
                    **limpiar_data_para_mostrar(candidato["data"])
                }, indent=2, ensure_ascii=False))
            else:
                resumen_campos[campo]["sin_candidato"] += 1
                campos_sin_candidato_doc.append(campo)

        if campos_sin_candidato_doc:
            sin_candidato_export.append({
                "id": doc_id,
                "marca": marca,
                "model": model,
                "modelDetail": data.get("modelDetail"),
                "version": data.get("version"),
                "date_add": data.get("date_add"),
                "campos_sin_candidato": campos_sin_candidato_doc,
                "documento": limpiar_data_para_mostrar(data)
            })

        if not updates:
            print(">> SIN UPDATES")
            continue

        print(f"\nUPDATES PROPUESTOS: {updates}")

        for campo in updates:
            resumen_campos[campo]["actualizados"] += 1

        if not dry_run:
            ref = db.collection(COLLECTION_NAME).document(doc_id)
            batch.update(ref, updates)
            batch_count += 1

            if batch_count >= batch_limit:
                batch.commit()
                batch = db.batch()
                batch_count = 0

        docs_actualizables += 1

    if not dry_run and batch_count > 0:
        batch.commit()

    exportar_sin_candidato(sin_candidato_export)

    print("\n" + "=" * 100)
    print("RESUMEN FINAL")
    print("=" * 100)
    print(f"Docs revisados: {len(items)}")
    print(f"Docs con updates: {docs_actualizables}")
    print(f"Omitidos: {omitidos}")
    print(f"DRY_RUN: {dry_run}")

    print("\nRESUMEN POR CAMPO")
    print("-" * 100)

    for campo, resumen in resumen_campos.items():
        print(f"\nCampo: {campo}")
        print(f"Vacíos detectados: {resumen['vacios_detectados']}")
        print(f"Actualizados: {resumen['actualizados']}")
        print(f"Sin candidato: {resumen['sin_candidato']}")


if __name__ == "__main__":
    procesar_normalizacion(dry_run=DRY_RUN, limitar=None)