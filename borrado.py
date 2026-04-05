import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud.firestore_v1.base_query import FieldFilter


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
def to_epoch_range(
    fecha_desde: str,
    fecha_hasta: str,
    tz_name: str = "America/Santiago"
) -> tuple[int, int]:
    """
    Convierte fechas YYYY-MM-DD a rango epoch [desde, hasta_exclusivo).
    """
    tz = ZoneInfo(tz_name)
    dt_desde = datetime.strptime(fecha_desde, "%Y-%m-%d").replace(tzinfo=tz)
    dt_hasta = datetime.strptime(fecha_hasta, "%Y-%m-%d").replace(tzinfo=tz) + timedelta(days=1)
    return int(dt_desde.timestamp()), int(dt_hasta.timestamp())


def precio_vacio(precio) -> bool:
    """
    Define cuándo el campo precio se considera vacío o inválido.
    Casos considerados:
      - None
      - ""
      - 0
      - []
      - [0], [None], [""]
    """
    if precio is None:
        return True

    if precio == "":
        return True

    if precio == 0:
        return True

    if isinstance(precio, list):
        if len(precio) == 0:
            return True

        if all(x is None or x == 0 or x == "" for x in precio):
            return True

    return False


# =========================
# FUNCIÓN PRINCIPAL
# =========================
def borrar_modelos(
    marca: str | None = None,
    modelo: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    solo_precio_vacio: bool = False,
    collection_name: str = "modelos",
    dry_run: bool = True,
    batch_size: int = 200,
):
    """
    Borra documentos de Firestore filtrando por:
      - marca (opcional)
      - modelo (opcional)
      - rango de fechas (opcional)
      - solo_precio_vacio=True para borrar solo docs con precio vacío

    Reglas:
      - Debes indicar al menos marca o modelo, salvo que solo_precio_vacio=True
      - dry_run=True: solo muestra qué borraría
      - dry_run=False: borra de verdad
    """

    if not marca and not modelo and not solo_precio_vacio:
        raise ValueError(
            "Debes indicar al menos marca o modelo, "
            "o usar solo_precio_vacio=True."
        )

    if (fecha_desde and not fecha_hasta) or (fecha_hasta and not fecha_desde):
        raise ValueError("Debes indicar ambas fechas: fecha_desde y fecha_hasta.")

    col_ref = db.collection(collection_name)
    query = col_ref

    if marca:
        query = query.where(filter=FieldFilter("marca", "==", marca))

    # Ajusta este campo si en tu colección se llama distinto:
    # "model", "modelo" o "modelDetail"
    if modelo:
        query = query.where(filter=FieldFilter("model", "==", modelo))

    if fecha_desde and fecha_hasta:
        desde_epoch, hasta_epoch = to_epoch_range(fecha_desde, fecha_hasta)
        print(f"Rango epoch: desde={desde_epoch} hasta_exclusivo={hasta_epoch}")
        query = query.where(filter=FieldFilter("date_add", ">=", desde_epoch))
        query = query.where(filter=FieldFilter("date_add", "<", hasta_epoch))

    total_revisados = 0
    total_borrables = 0
    batch = db.batch()

    try:
        for doc in query.stream():
            total_revisados += 1
            data = doc.to_dict()

            # Si solo_precio_vacio=True, filtra en Python
            if solo_precio_vacio and not precio_vacio(data.get("precio")):
                continue

            total_borrables += 1

            motivo = "precio vacío" if solo_precio_vacio else "filtro aplicado"
            print(
                f"[{'DRY' if dry_run else 'DEL'}] {doc.id} | "
                f"marca={data.get('marca')} | "
                f"model={data.get('model')} | "
                f"date_add={data.get('date_add')} | "
                f"precio={data.get('precio')} | "
                f"motivo={motivo}"
            )

            if not dry_run:
                batch.delete(doc.reference)

                if total_borrables % batch_size == 0:
                    batch.commit()
                    print(f"✅ Batch borrado: {total_borrables}")
                    batch = db.batch()

        if not dry_run and total_borrables % batch_size != 0:
            batch.commit()

        print("\n====================")
        print(f"Total revisados : {total_revisados}")
        print(f"Total borrables : {total_borrables}")

        if dry_run:
            print("Modo DRY RUN: no se borró nada.")
        else:
            print("Borrado completado.")

    except Exception as e:
        print("Error al ejecutar la consulta o borrado:", e)
        print("Puede requerir índice compuesto si combinas igualdad + rango.")
        raise


# =========================
# EJEMPLOS DE USO
# =========================

# 1) Ver qué borraría por marca
# borrar_modelos(
#     marca="Kia",
#     dry_run=True,
# )

# 2) Ver qué borraría por marca + rango de fechas
# borrar_modelos(
#     marca="Kia",
#     fecha_desde="2026-03-25",
#     fecha_hasta="2026-03-30",
#     dry_run=True,
# )

# 3) Ver qué borraría por marca + modelo + fechas
# borrar_modelos(
#     marca="Kia",
#     modelo="Sportage",
#     fecha_desde="2026-03-25",
#     fecha_hasta="2026-03-30",
#     dry_run=True,
# )

# 4) Ver solo docs con precio vacío de una marca
# borrar_modelos(
#     marca="Kia",
#     solo_precio_vacio=True,
#     dry_run=True,
# )

# 5) Ver solo docs con precio vacío de una marca/modelo en rango
# borrar_modelos(
#     marca="Kia",
#     modelo="Sportage",
#     fecha_desde="2026-03-25",
#     fecha_hasta="2026-03-30",
#     solo_precio_vacio=True,
#     dry_run=True,
# )

# 6) Borrar de verdad solo docs con precio vacío de una marca
# borrar_modelos(
#     marca="Kia",
#     solo_precio_vacio=True,
#     dry_run=False,
# )

# 7) Borrar de verdad todo lo de una marca en un rango
borrar_modelos(
    marca="Hyundai",
    fecha_desde="2026-03-29",
    fecha_hasta="2026-04-01",
    dry_run=False,
)