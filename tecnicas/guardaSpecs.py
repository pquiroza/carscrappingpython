import json
import argparse
import sys
import os
import re
from glob import glob
from typing import Any, Dict, List
from utils import guardaSpecs
from utils import to_title_custom
import unicodedata

try:
    import pandas as pd
except ImportError:
    pd = None

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
    return sorted(files)


def normalize_search_text(text: str) -> str:
    if not text:
        return ""
    text = remove_accents(text).lower().strip()
    text = re.sub(r"[^a-z0-9\.\+\-\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_search_tokens(marca: str, modelo: str, version: str) -> List[str]:
    marca_n = normalize_search_text(marca)
    modelo_n = normalize_search_text(modelo)
    version_n = normalize_search_text(version)

    modelo_parts = [p for p in modelo_n.split() if p]
    version_parts = [p for p in version_n.split() if p]

    tokens = set()

    # básicos
    if marca_n:
        tokens.add(marca_n)
    if modelo_n:
        tokens.add(modelo_n)
    if version_n:
        tokens.add(version_n)

    # palabras individuales
    for p in modelo_parts:
        tokens.add(p)
    for p in version_parts:
        tokens.add(p)

    # combinaciones del modelo
    for i in range(len(modelo_parts)):
        for j in range(i + 1, len(modelo_parts) + 1):
            tokens.add(" ".join(modelo_parts[i:j]))

    # combinaciones de la versión
    for i in range(len(version_parts)):
        for j in range(i + 1, len(version_parts) + 1):
            tokens.add(" ".join(version_parts[i:j]))

    # combinaciones modelo + versión
    if modelo_parts and version_parts:
        full_parts = modelo_parts + version_parts
        for i in range(len(full_parts)):
            for j in range(i + 1, len(full_parts) + 1):
                tokens.add(" ".join(full_parts[i:j]))

    # combinaciones marca + modelo, marca + modelo + version
    if marca_n and modelo_n:
        tokens.add(f"{marca_n} {modelo_n}")
    if marca_n and modelo_n and version_n:
        tokens.add(f"{marca_n} {modelo_n} {version_n}")

    return sorted(t for t in tokens if t)


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


def is_version_mapping(d: Dict[str, Any], versions: List[str]) -> bool:
    if not isinstance(d, dict) or not d:
        return False

    keys = set(norm_version_key(k) for k in d.keys())
    version_set = set(versions)

    return keys.issubset(version_set) and len(keys) > 0


def pick_value_for_version(value: Any, version: str, versions: List[str]) -> Any:
    if isinstance(value, dict):
        if is_version_mapping(value, versions):
            return value.get(version)
        return {k: pick_value_for_version(v, version, versions) for k, v in value.items()}

    if isinstance(value, list):
        return [pick_value_for_version(v, version, versions) for v in value]

    return value


def comparative_tables_to_profile(data: Dict[str, Any], version: str):
    tables = {
        "ruedas": data.get("tabla_comparativa_ruedas", []),
        "peso": data.get("tabla_comparativa_peso", []),
        "capacidades": data.get("tabla_comparativa_capacidades", []),
        "rendimiento": data.get("tabla_comparativa_rendimiento", []),
    }

    result = {}

    for tname, rows in tables.items():
        result[tname] = {}
        for row in rows:
            car = row.get("caracteristica")
            if not car:
                continue
            if version in row:
                result[tname][car] = row.get(version)

    return result


def differential_equipment_for_version(data: Dict[str, Any], version: str):
    eq = data.get("equipamiento_diferencial_detallado", {})
    result = {}

    for categoria, items in eq.items():
        selected = []
        for it in items:
            car = it.get("caracteristica")
            if not car:
                continue

            val = it.get(version)
            if val not in (None, False, "", 0):
                selected.append({
                    "caracteristica": car,
                    "valor": val
                })

        if selected:
            result[categoria] = selected

    return result


def build_profiles(data: Dict[str, Any]):
    versions = extract_versions(data)
    vehiculo = data.get("vehiculo", {})
    marca = vehiculo.get("marca")
    modelo = vehiculo.get("modelo")
    anio = vehiculo.get("año")

    profiles = {}

    for version in versions:
        profile = {
            "marca": marca,
            "modelo": modelo,
            "version": version,
            "tokens_busqueda": build_search_tokens(marca, modelo, version),
            "vehiculo": {
                "marca": marca,
                "modelo": modelo,
                "año": anio,
                "version": version
            },
            "motor": pick_value_for_version(data.get("motor", {}), version, versions),
            "direccion": pick_value_for_version(data.get("direccion", {}), version, versions),
            "suspension_frenos": pick_value_for_version(data.get("suspension_frenos", {}), version, versions),
            "dimensiones": pick_value_for_version(data.get("dimensiones", {}), version, versions),
            "comparativas": comparative_tables_to_profile(data, version),
            "equipamiento_estandar": data.get("equipamiento_estandar_todas_versiones", {}),
            "equipamiento_diferencial": differential_equipment_for_version(data, version),
            "colores": data.get("colores", []),
            "metadata": data.get("metadata", {}),
        }

        profiles[version] = profile

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

    for version, info in profiles.items():
        veh = info.get("vehiculo", {})
        marca = veh.get("marca", "")
        modelo = veh.get("modelo", "")

        print("\n" + "=" * 60)
        print(f"🚗 {marca} {modelo} — VERSION: {version}")
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
        for cat, items in info["equipamiento_diferencial"].items():
            print(f"\n  {cat.upper()}")
            for it in items:
                print(f"    ✔ {it['caracteristica']}: {it['valor']}")

        datos = {
            "marca": info["marca"],
            "modelo": info["modelo"],
            "version": info["version"],
            "tokens_busqueda": info["tokens_busqueda"],
            "vehiculo": info["vehiculo"],
            "motor": info["motor"],
            "direccion": info["direccion"],
            "suspension_frenos": info["suspension_frenos"],
            "dimensiones": info["dimensiones"],
            "comparativas": info["comparativas"],
            "equipamiento_estandar": info["equipamiento_estandar"],
            "equipamiento_diferencial": info["equipamiento_diferencial"],
            "colores": info["colores"],
            "metadata": info["metadata"],
        }

        guardaSpecs(
            to_title_custom(marca),
            to_title_custom(modelo),
            to_title_custom(version),
            datos
        )

        doc[version] = datos

    return doc


# -----------------------------
# CLI
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Analizador de fichas técnicas de vehículos")
    parser.add_argument("--json", help="Ruta del archivo JSON (modo único)")
    parser.add_argument("--dir", help="Directorio con múltiples JSON (modo batch)")
    parser.add_argument("--recursive", action="store_true", help="Buscar JSON en subdirectorios (solo con --dir)")
    parser.add_argument("--export", help="Exportar resultados a un JSON global")
    parser.add_argument("--csv", help="Exportar comparativas a CSV (global, requiere pandas)")
    args = parser.parse_args()

    if not args.json and not args.dir:
        print("❌ Debes usar --json archivo.json o --dir carpeta/")
        sys.exit(1)

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

        for v, info in profiles.items():
            diff = info["equipamiento_diferencial"]
            print(f"\n📌 {v}")
            for cat, items in diff.items():
                print(f"  {cat}: {len(items)} items")

        exported = print_profiles(profiles)
        all_results[os.path.basename(args.json)] = exported

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

                base = os.path.basename(path)
                versiones = list(profiles.keys())
                print(f"\n📄 {base} — versiones: {len(versiones)}")

                exported = print_profiles(profiles)
                all_results[base] = exported

                if args.csv:
                    df = to_dataframes(data)
                    if df is None:
                        print("⚠ pandas no instalado. pip install pandas")
                    else:
                        df.insert(0, "source", base)
                        csv_frames.append(df)

            except Exception as e:
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

    if args.dir and tqdm is None:
        print("\nℹ Para barra de progreso instala tqdm: pip install tqdm")


if __name__ == "__main__":
    main()