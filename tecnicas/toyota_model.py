#!/usr/bin/env python3
"""
PDF Extractor using Claude Vision - NO HALLUCINATION MODE
Extracts ONLY what is visible in the image, nothing from training data.
"""

import sys
import json
import os
import base64
from datetime import date
from io import BytesIO
from pdf2image import convert_from_path
import pytesseract
import anthropic
from PIL import Image, ImageEnhance

Image.MAX_IMAGE_PIXELS = None


def resize_for_tesseract(image):
    max_dim = 10000
    if image.width > max_dim or image.height > max_dim:
        scale = min(max_dim / image.width, max_dim / image.height)
        image = image.resize(
            (int(image.width * scale), int(image.height * scale)),
            Image.Resampling.LANCZOS
        )
    return image


def image_to_base64_jpeg(image, max_pixels=2048*2048, quality=92):
    if image.width * image.height > max_pixels:
        scale = (max_pixels / (image.width * image.height)) ** 0.5
        image = image.resize(
            (int(image.width * scale), int(image.height * scale)),
            Image.Resampling.LANCZOS
        )
    image = image.convert('RGB')
    image = ImageEnhance.Contrast(image).enhance(1.3)
    image = ImageEnhance.Sharpness(image).enhance(1.5)

    buffered = BytesIO()
    image.save(buffered, format="JPEG", quality=quality)
    data = base64.b64encode(buffered.getvalue()).decode()

    size_mb = len(data) / 1024 / 1024
    print(f"  Image: {image.width}x{image.height}px, {size_mb:.1f}MB", file=sys.stderr)
    return data


def find_specs_pages(images):
    specs_pages = []
    for i, image in enumerate(images):
        img = image.convert('L')
        img = resize_for_tesseract(img)
        text = pytesseract.image_to_string(img, lang='eng')
        if any(kw in text.upper() for kw in ['ESPECIFICACIONES', 'MOTOR', 'CILINDRADA', 'EQUIPAMIENTO']):
            specs_pages.append(i)
            print(f"  Page {i+1}: specs detected", file=sys.stderr)
        else:
            print(f"  Page {i+1}: cover page (skipped)", file=sys.stderr)
    return specs_pages


def extract_from_image(images, specs_page_indices, client, pdf_path):
    today = date.today().isoformat()
    filename = os.path.basename(pdf_path)

    content = []
    for idx in specs_page_indices:
        image_data = image_to_base64_jpeg(images[idx])
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": image_data},
        })

    content.append({
        "type": "text",
        "text": """⚠️ CRITICAL ANTI-HALLUCINATION INSTRUCTIONS ⚠️

You are looking at a Chilean automotive specification document image.

YOUR ONLY JOB: Extract the EXACT text and numbers you can SEE in this specific image.

DO NOT:
❌ Use your training data about this vehicle model
❌ Fill in "typical" values for this brand/model
❌ Guess missing values
❌ Assume standard equipment
❌ Use your knowledge of what "usually" comes with this trim level

ONLY use values that are CLEARLY VISIBLE in the image text.
If a table cell is empty or you cannot read it clearly → use null

Example:
- You see "4460" next to "Longitud total (mm)" → use "4460"
- You see an empty cell next to "Distancia entre ejes" → use null
- You see "No / Si" in a feature row → first version false, second version true
- You cannot see a value clearly → use null

Read this vehicle specification document and extract ALL information into this JSON:

{
  "vehiculo": {
    "marca": "",
    "modelo": "",
    "año": null,
    "versiones": []
  },
  "motor": {
    "tipo": "",
    "cilindrada": "",
    "potencia_hp": "",
    "potencia_rpm": "",
    "torque_nm": "",
    "torque_rpm": "",
    "combustible": "",
    "transmision": "",
    "traccion": ""
  },
  "direccion": {"tipo": ""},
  "suspension_frenos": {
    "suspension_delantera": "",
    "suspension_trasera": "",
    "frenos_delanteros": "",
    "frenos_traseros": "",
    "neumaticos": "",
    "rueda_repuesto": ""
  },
  "dimensiones": {
    "largo_mm": "",
    "ancho_mm": "",
    "alto_mm": "",
    "distancia_ejes_mm": "",
    "despeje_suelo_mm": "",
    "peso_bruto_kg": "",
    "capacidad_maletero_lts": "",
    "capacidad_estanque_lts": ""
  },
  "tabla_comparativa_ruedas": [
    {"caracteristica": "Llantas", "VERSION_1": "", "VERSION_2": ""}
  ],
  "tabla_comparativa_peso": [
    {"caracteristica": "Peso neto (kg)", "VERSION_1": "", "VERSION_2": ""}
  ],
  "tabla_comparativa_capacidades": [
    {"caracteristica": "Carga util (kg)", "VERSION_1": "", "VERSION_2": ""},
    {"caracteristica": "Numero de pasajeros", "VERSION_1": "", "VERSION_2": ""}
  ],
  "tabla_comparativa_rendimiento": [
    {"caracteristica": "Ciudad (km/lts)", "VERSION_1": "", "VERSION_2": ""},
    {"caracteristica": "Carretera (km/lts)", "VERSION_1": "", "VERSION_2": ""},
    {"caracteristica": "Mixto (km/lts)", "VERSION_1": "", "VERSION_2": ""}
  ],
  "equipamiento_diferencial_detallado": {
    "interior": [
      {"caracteristica": "exact text from image", "VERSION_1": true, "VERSION_2": false}
    ],
    "exterior": [],
    "seguridad": [],
    "tecnologia": []
  },
  "equipamiento_estandar_todas_versiones": {
    "interior": [],
    "exterior": [],
    "seguridad": []
  },
  "metadata": {
    "fecha_extraccion": \"""" + today + """\",
    "metodo_extraccion": "Claude Vision",
    "fuente": \"""" + filename + """\",
    "versiones_en_documento": 0,
    "notas": ""
  }
}

RULES:
1. VERSION NAMES: Read exact column headers (usually colored row at top)
   Replace spaces with _ for JSON keys. Example: "XEI HV" → key "XEI_HV"

2. equipamiento_diferencial_detallado: ONLY features where columns show DIFFERENT values
   - "No / Si" patterns = first false, second true
   - Different text ("Tela" vs "Cuero") = use the actual strings
   - Same value in all columns = goes to equipamiento_estandar instead

3. equipamiento_estandar_todas_versiones: ONLY features that are IDENTICAL in ALL columns
   (or marked as "Si" for all versions)

4. Split ratios:
   - "122/5200" → potencia_hp="122", potencia_rpm="5200"
   - "142/3600" → torque_nm="142", torque_rpm="3600"

5. If cell is empty or you can't read it → use null (do NOT guess)

6. versiones_en_documento = total number of version columns you found

7. Extract EVERY row from the equipment table

Return ONLY valid JSON, no other text."""
    })

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        temperature=0,
        messages=[{"role": "user", "content": content}],
    )

    return message.content[0].text


def extract_pdf_to_json(pdf_path, api_key):
    print("Step 1: Converting PDF...", file=sys.stderr)
    images = convert_from_path(pdf_path, dpi=250)
    print(f"Found {len(images)} pages", file=sys.stderr)

    client = anthropic.Anthropic(api_key=api_key)

    print("\nStep 2: Finding specs pages...", file=sys.stderr)
    specs_pages = find_specs_pages(images)

    if not specs_pages:
        print("WARNING: No specs pages found, trying all...", file=sys.stderr)
        specs_pages = list(range(len(images)))

    print(f"\nStep 3: Extracting from pages {[p+1 for p in specs_pages]}...", file=sys.stderr)
    response_text = extract_from_image(images, specs_pages, client, pdf_path)

    import re
    json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        json_str = json_match.group(0) if json_match else response_text

    result = json.loads(json_str)

    v = result.get('vehiculo', {})
    eq = result.get('equipamiento_diferencial_detallado', {})

    print("\nDone!", file=sys.stderr)
    print(f"  {v.get('marca')} {v.get('modelo')}", file=sys.stderr)
    print(f"  Versiones: {v.get('versiones')}", file=sys.stderr)
    print(f"  Features: Int={len(eq.get('interior',[]))} Ext={len(eq.get('exterior',[]))} Seg={len(eq.get('seguridad',[]))} Tech={len(eq.get('tecnologia',[]))}", file=sys.stderr)

    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_hybrid.py <pdf_file>", file=sys.stderr)
        sys.exit(1)

    pdf_path = sys.argv[1]
    api_key = os.getenv('ANTHROPIC_API_KEY')

    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(pdf_path):
        print(f"ERROR: File not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    try:
        result = extract_pdf_to_json(pdf_path, api_key)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()