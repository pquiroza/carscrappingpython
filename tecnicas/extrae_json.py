import json
import argparse
import sys
import os
from glob import glob
from typing import Any, Dict, List
from utils import guardaSpecs
from utils import to_title_custom
import unicodedata
try:
    import pandas as pd
except ImportError:
    pd = None

# Barra de progreso (opcional)
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None



def remove_accents(text: str) -> str:
    if not isinstance(text, str):
        return text
    return ''.join(
        c for c in unicodedata.normalize('NFKD', text)
        if not unicodedata.combining(c)
    )


def normalize_unicode(obj):
    if isinstance(obj, dict):
        return {remove_accents(k): normalize_unicode(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_unicode(v) for v in obj]
    if isinstance(obj, str):
        return remove_accents(obj)
    return obj

# -----------------------------
# Helpers
# -----------------------------
def norm_version_key(s: str) -> str:
    return " ".join((s or "").strip().split())


def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def load_json_from_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ JSON inválido en {path}: {e}")
        sys.exit(1)


def list_json_files(directory: str, recursive: bool = False) -> List[str]:
    if not os.path.isdir(directory):
        print(f"❌ Directorio no existe: {directory}")
        sys.exit(1)

    pattern = os.path.join(directory, "**", "*.json") if recursive else os.path.join(directory, "*.json")
    files = glob(pattern, recursive=recursive)

    # Orden estable
    files = sorted(files)
    return files


# -----------------------------
# Core parsing
# -----------------------------
def extract_versions(data: Dict[str, Any]) -> List[str]:
    veh = data.get("vehiculo", {})
    raw = ensure_list(veh.get("versiones"))
    versions = [norm_version_key(v) for v in raw if str(v).strip()]
    seen = set()
    out = []
    for v in versions:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def flatten_core_specs(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "marca": data.get("vehiculo", {}).get("marca"),
        "modelo": data.get("vehiculo", {}).get("modelo"),
        "motor": data.get("motor", {}),
        "dimensiones": data.get("dimensiones", {}),
        "suspension_frenos": data.get("suspension_frenos", {}),
    }


def comparative_tables_to_profiles(data: Dict[str, Any], versions: List[str]):
    tables = {
        "ruedas": data.get("tabla_comparativa_ruedas", []),
        "peso": data.get("tabla_comparativa_peso", []),
        "capacidades": data.get("tabla_comparativa_capacidades", []),
        "rendimiento": data.get("tabla_comparativa_rendimiento", []),
    }

    result = {t: {v: {} for v in versions} for t in tables}

    for tname, rows in tables.items():
        for row in rows:
            car = row.get("caracteristica")
            if not car:
                continue
            for v in versions:
                if v in row:
                    result[tname][v][car] = row.get(v)

    return result


def differential_equipment(data: Dict[str, Any], versions: List[str]):
    eq = data.get("equipamiento_diferencial_detallado", {})
    result = {v: {} for v in versions}

    for categoria, items in eq.items():
        for it in items:
            car = it.get("caracteristica")
            if not car:
                continue
            for v in versions:
                # Ojo: si el valor es False, no lo agregamos. Si es True o texto, sí.
                if v in it and it.get(v) not in (None, False, "", 0):
                    result.setdefault(v, {}).setdefault(categoria, []).append(car)

    return result


def build_profiles(data: Dict[str, Any]):
    versions = extract_versions(data)
    core = flatten_core_specs(data)
    tables = comparative_tables_to_profiles(data, versions)
    diff = differential_equipment(data, versions)
    std = data.get("equipamiento_estandar_todas_versiones", {})

    profiles = {}

    for v in versions:
        profiles[v] = {
            "version": v,
            "marca": core["marca"],
            "modelo": core["modelo"],
            "motor": core["motor"],
            "dimensiones": core["dimensiones"],
            "suspension_frenos": core["suspension_frenos"],
            "comparativas": {t: tables[t][v] for t in tables},
            "equipamiento_estandar": std,
            "equipamiento_diferencial_true": diff.get(v, {}),
        }

    return profiles


# -----------------------------
# DataFrames opcionales
# -----------------------------
def to_dataframes(data: Dict[str, Any]):
    if pd is None:
        return None

    versions = extract_versions(data)

    rows = []
    tables = {
        "ruedas": data.get("tabla_comparativa_ruedas", []),
        "peso": data.get("tabla_comparativa_peso", []),
        "capacidades": data.get("tabla_comparativa_capacidades", []),
        "rendimiento": data.get("tabla_comparativa_rendimiento", []),
    }

    for tname, items in tables.items():
        for r in items:
            car = r.get("caracteristica")
            for v in versions:
                if v in r:
                    rows.append(
                        {
                            "tabla": tname,
                            "caracteristica": car,
                            "version": v,
                            "valor": r[v],
                        }
                    )

    return pd.DataFrame(rows)


def print_profiles(profiles: Dict[str, Dict[str, Any]]):
    doc = {}
    for v, info in profiles.items():
        marca = info.get("marca", "")
        modelo = info.get("modelo", "")

        print("\n" + "=" * 60)
        print(f"🚗 {marca} {modelo} — VERSION: {v}")
        print("=" * 60)

        print("\n🔧 MOTOR")
        for k, val in info["motor"].items():
            print(f"  {k}: {val}")

        print("\n📏 DIMENSIONES")
        for k, val in info["dimensiones"].items():
            print(f"  {k}: {val}")

        print("\n🛞 COMPARATIVAS")
        for tabla, items in info["comparativas"].items():
            print(f"\n  [{tabla.upper()}]")
            for car, val in items.items():
                print(f"    - {car}: {val}")

        print("\n✅ EQUIPAMIENTO ESTÁNDAR")
        for cat, items in info["equipamiento_estandar"].items():
            print(f"\n  {cat.upper()}")
            for it in items:
                print(f"    • {it}")

        print("\n⭐ EQUIPAMIENTO DIFERENCIAL")
        for cat, items in info["equipamiento_diferencial_true"].items():
            print(f"\n  {cat.upper()}")
            for it in items:
                print(f"    ✔ {it}")

        datos = {
            "marca": marca,
            "modelo": modelo,
            "version": v,
            "motor": info["motor"],
            "dimensiones": info["dimensiones"],
            "comparativas": info["comparativas"],
            "equipamiento_estandar": info["equipamiento_estandar"],
            "equipamiento_diferencial": info["equipamiento_diferencial_true"],
        }

        # Guardado por versión (tu comportamiento actual)
        #guardaSpecs(to_title_custom(marca), to_title_custom(modelo), to_title_custom(v), datos)

    return doc


# -----------------------------
# CLI
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Analizador de fichas técnicas de vehículos")
    parser.add_argument("--json", help="Ruta del archivo JSON (modo único)")
    parser.add_argument("--dir", help="Directorio con múltiples JSON (modo batch)")
    parser.add_argument("--recursive", action="store_true", help="Buscar JSON en subdirectorios (solo con --dir)")
    parser.add_argument("--export", help="Exportar resultados a un JSON (global)")
    parser.add_argument("--csv", help="Exportar comparativas a CSV (global, requiere pandas)")
    args = parser.parse_args()

    if not args.json and not args.dir:
        print("❌ Debes usar --json archivo.json o --dir carpeta/")
        sys.exit(1)

    # Export global:
    # - Para evitar colisiones de claves (versiones repetidas entre archivos),
    #   guardamos por archivo: { "archivo.json": {<version>: <profile>...}, ... }
    all_results: Dict[str, Any] = {}
    csv_frames = []

    # -----------------------
    # MODO ARCHIVO ÚNICO
    # -----------------------
    if args.json:
        data = load_json_from_file(args.json)
        data = normalize_unicode(data)
        profiles = build_profiles(data)

        print("✅ Versiones detectadas:")
        for v in profiles:
            print("  -", v)

        # Mostrar resumen
        for v, info in profiles.items():
            diff = info["equipamiento_diferencial_true"]
            print(f"\n📌 {v}")
            for cat, items in diff.items():
                print(f"  {cat}: {len(items)} items")

        print_profiles(profiles)

        all_results[os.path.basename(args.json)] = profiles

        if args.csv:
            df = to_dataframes(data)
            if df is None:
                print("⚠ pandas no instalado. pip install pandas")
            else:
                df.insert(0, "source", os.path.basename(args.json))
                csv_frames.append(df)

    # -----------------------
    # MODO DIRECTORIO
    # -----------------------
    if args.dir:
        files = list_json_files(args.dir, recursive=args.recursive)
        if not files:
            print(f"⚠ No se encontraron .json en: {args.dir} (recursive={args.recursive})")
            sys.exit(0)

        iterator = files
        if tqdm is not None:
            iterator = tqdm(files, desc="Procesando JSON", unit="archivo")

        for path in iterator:
            try:
                data = load_json_from_file(path)
                data = normalize_unicode(data)
                profiles = build_profiles(data)

                # Print corto por archivo (sin spamear demasiado)
                base = os.path.basename(path)
                versiones = list(profiles.keys())
                print(f"\n📄 {base} — versiones: {len(versiones)}")

                all_results[base] = profiles

                # Si quieres el detalle completo por archivo, deja esto activo:
                print_profiles(profiles)

                if args.csv:
                    df = to_dataframes(data)
                    if df is None:
                        print("⚠ pandas no instalado. pip install pandas")
                    else:
                        df.insert(0, "source", base)
                        csv_frames.append(df)

            except Exception as e:
                # No detenemos todo el batch si un archivo falla
                print(f"\n❌ Error procesando {path}: {e}", file=sys.stderr)

    # -----------------------
    # EXPORT GLOBAL
    # -----------------------
    if args.export:
        with open(args.export, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Exportado (global) a {args.export}")

    if args.csv:
        if pd is None:
            print("⚠ pandas no instalado. pip install pandas")
        else:
            if csv_frames:
                out_df = pd.concat(csv_frames, ignore_index=True)
                out_df.to_csv(args.csv, index=False)
                print(f"\n📊 CSV exportado (global) a {args.csv}")
            else:
                print("⚠ No hay datos de comparativas para exportar a CSV.")

    # Aviso si no tienes tqdm
    if args.dir and tqdm is None:
        print("\nℹ Para barra de progreso instala tqdm: pip install tqdm")


if __name__ == "__main__":
    main()
