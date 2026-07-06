import subprocess
import json
import time
from datetime import datetime

# ===================== CONFIG =====================

SCRIPTS = [
    "orq_bmw.py",
    "orq_carscrapper.py",
    "orq_chevrolet.py",
    "orq_derco.py",
    "orq_dfsk.py",
    "orq_difor.py",
    "orq_geely.py",
    "orq_jac.py",
    "orq_kia.py",
    "orq_lynkco.py",
    "orq_mahindra.py",
    "orq_mercedes.py",
    "orq_mazda.py",
    "orq_subaru.py",
    "orq_valenzuela.py",
    "orq_volvo.py",
    "orq_zentrum.py",
]

PYTHON_CMD = "python3"

# ===================== EJECUTOR =====================

def run_script(script_name):
    print(f"\n🚀 Ejecutando: {script_name}")
    start = time.time()

    process = subprocess.Popen(
        [PYTHON_CMD, script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    output_lines = []
    run_ok = False
    summary_json = None

    for line in process.stdout:
        print(line.strip())
        output_lines.append(line)

        if "RUN_OK" in line:
            run_ok = True

        # intentar parsear JSON summary
        try:
            parsed = json.loads(line.strip())
            if isinstance(parsed, dict) and "status" in parsed:
                summary_json = parsed
        except:
            pass

    process.wait()
    end = time.time()

    success = run_ok and process.returncode == 0

    result = {
        "script": script_name,
        "success": success,
        "duration_sec": round(end - start, 2),
        "return_code": process.returncode,
        "summary": summary_json
    }

    return result


# ===================== MAIN =====================

def main():
    print("\n==============================")
    print("🔥 ORQUESTADOR SCRAPERS AUTOS")
    print("==============================\n")

    global_start = time.time()

    results = []

    for script in SCRIPTS:
        res = run_script(script)
        results.append(res)

    global_end = time.time()

    # ===================== RESUMEN =====================

    total = len(results)
    success = len([r for r in results if r["success"]])
    failed = total - success

    final_summary = {
        "timestamp": datetime.now().isoformat(),
        "total_scripts": total,
        "success": success,
        "failed": failed,
        "duration_total_sec": round(global_end - global_start, 2),
        "details": results
    }

    print("\n==============================")
    print("📊 RESUMEN FINAL")
    print("==============================")

    print(json.dumps(final_summary, indent=2, ensure_ascii=False))

    # guardar log
    with open("orquestador_resultado.json", "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    # exit code global
    if failed > 0:
        print("\n❌ Algunos procesos fallaron")
        exit(1)
    else:
        print("\n✅ Todos los procesos OK")
        exit(0)


if __name__ == "__main__":
    main()