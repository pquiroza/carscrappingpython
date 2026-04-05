#!/usr/bin/env python3
"""
Hybrid PDF Extractor: OCR + LLM (optimized for complete equipment extraction)
"""

import sys
import json
import os
from pdf2image import convert_from_path
import pytesseract
import anthropic
from PIL import Image

Image.MAX_IMAGE_PIXELS = None


def extract_pdf_to_json(pdf_path: str, api_key: str) -> dict:
    """
    Hybrid extraction: OCR for text, LLM for structure
    """
    print("Step 1: Converting PDF to image...", file=sys.stderr)
    images = convert_from_path(pdf_path, dpi=200)
    image = images[0]
    
    print(f"Original image size: {image.width}x{image.height}", file=sys.stderr)
    
    # Tesseract limits - resize if too large
    max_dimension = 10000
    
    if image.width > max_dimension or image.height > max_dimension:
        print("Image too large for OCR, resizing...", file=sys.stderr)
        scale = min(max_dimension / image.width, max_dimension / image.height)
        new_width = int(image.width * scale)
        new_height = int(image.height * scale)
        image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
        print(f"Resized to: {new_width}x{new_height}", file=sys.stderr)
    
    # Enhance image for better OCR
    from PIL import ImageEnhance
    image = image.convert('L')
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    
    image.save('debug_ocr_input.png')
    print(f"✓ Saved OCR input: debug_ocr_input.png", file=sys.stderr)
    
    print("\nStep 2: Running OCR...", file=sys.stderr)
    
    try:
        text = pytesseract.image_to_string(image, lang='eng')
    except Exception as e:
        print(f"OCR failed, trying smaller: {e}", file=sys.stderr)
        new_width = int(image.width * 0.7)
        new_height = int(image.height * 0.7)
        image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
        print(f"Resized to: {new_width}x{new_height}", file=sys.stderr)
        text = pytesseract.image_to_string(image, lang='eng')
    
    with open('debug_ocr_text.txt', 'w', encoding='utf-8') as f:
        f.write(text)
    
    print(f"✓ OCR complete. Extracted {len(text)} characters", file=sys.stderr)
    print(f"✓ Saved to debug_ocr_text.txt", file=sys.stderr)
    
    # Count how many equipment lines we have
    equipment_lines = [line for line in text.split('\n') if any(keyword in line.upper() for keyword in ['AIRE', 'ASIENTO', 'VOLANTE', 'SISTEMA', 'PANTALLA', 'CALEFACCION', 'TAPIZADO', 'MEMORIA', 'BOSE', 'CUERO', 'PIEL', 'ALUMINIO', 'RIELES', 'SUNROOF', 'TECHO'])]
    print(f"✓ Found ~{len(equipment_lines)} equipment-related lines in OCR", file=sys.stderr)
    
    print("\nStep 3: Sending to Claude for structuring...", file=sys.stderr)
    
    # IMPROVED PROMPT - more explicit about extracting ALL rows
    prompt = f"""Analiza este texto extraído por OCR de una ficha técnica chilena de automóvil.

TEXTO COMPLETO:
{text}

Tu tarea: Extraer TODA la información en JSON estructurado.

**CRÍTICO - EQUIPAMIENTO DIFERENCIAL:**
Busca la tabla "EQUIPAMIENTO DIFERENCIAL POR VERSIÓN" o similar.
Esta tabla tiene características listadas a la izquierda y versiones como columnas.
DEBES extraer TODAS las filas de características, incluyendo pero no limitado a:

INTERIOR:
- Aire acondicionado / climatizador (cualquier mención)
- Asientos (calefacción, ventilación, ajuste, memoria, cuero, tela, piel)
- Volante (calefacción, ajuste, eléctrico, manual)
- Pantallas (8", 9", táctil)
- Sistema de audio / Bose / parlantes / bocinas
- Cargador inalámbrico
- Luces de ambientación
- Cualquier otro equipamiento interior

EXTERIOR:
- Rieles, barras
- Llantas, neumáticos
- Sunroof, techo panorámico
- Espejos
- Cualquier otro equipamiento exterior

TECNOLOGÍA:
- Nissan Intelligent Mobility
- Nissan Connect
- Cámaras, sensores
- Cualquier tecnología

Para cada característica encontrada, determina si cada versión la tiene:
- Si hay un círculo/punto (●, •, o) → true
- Si hay guión/vacío (-, ×, espacio) → false

JSON estructura:

{{
  "vehiculo": {{
    "marca": "",
    "modelo": "",
    "versiones": []
  }},
  "motor": {{
    "tipo": "",
    "cilindrada": "",
    "potencia_hp": "",
    "torque_nm": "",
    "combustible": "",
    "transmision": "",
    "traccion": ""
  }},
  "dimensiones": {{
    "ancho_mm": "",
    "alto_mm": "",
    "distancia_ejes_mm": "",
    "peso_bruto_kg": ""
  }},
  "suspension_frenos": {{
    "suspension_delantera": "",
    "suspension_trasera": "",
    "frenos_delanteros": "",
    "frenos_traseros": ""
  }},
  "tabla_comparativa_ruedas": [
    {{"caracteristica": "Llantas", "VERSION_1": "", "VERSION_2": ""}}
  ],
  "tabla_comparativa_peso": [
    {{"caracteristica": "Peso neto (kg)", "VERSION_1": "", "VERSION_2": ""}}
  ],
  "tabla_comparativa_capacidades": [
    {{"caracteristica": "Carga útil", "VERSION_1": "", "VERSION_2": ""}},
    {{"caracteristica": "Pasajeros", "VERSION_1": "", "VERSION_2": ""}}
  ],
  "tabla_comparativa_rendimiento": [
    {{"caracteristica": "Ciudad", "VERSION_1": "", "VERSION_2": ""}},
    {{"caracteristica": "Carretera", "VERSION_1": "", "VERSION_2": ""}},
    {{"caracteristica": "Mixto", "VERSION_1": "", "VERSION_2": ""}}
  ],
  "equipamiento_diferencial_detallado": {{
    "interior": [
      {{"caracteristica": "nombre completo del feature", "VERSION_1": true, "VERSION_2": false}}
    ],
    "exterior": [
      {{"caracteristica": "nombre completo del feature", "VERSION_1": true, "VERSION_2": false}}
    ],
    "tecnologia": [
      {{"caracteristica": "nombre completo del feature", "VERSION_1": true, "VERSION_2": false}}
    ]
  }},
  "equipamiento_estandar_todas_versiones": {{
    "interior": [],
    "exterior": [],
    "seguridad": []
  }}
}}

REGLAS CRÍTICAS:
1. Extrae TODAS las filas de equipamiento, no solo algunas
2. Lee el texto línea por línea para no perder información
3. Versiones: usa nombres exactos (SENSE AT 3R, ADVANCE AT 3R, etc.)
4. Si ves "●" o similar = true, si ves "-" o vacío = false
5. Mantén los nombres de características completos y exactos
6. NO omitas características por falta de espacio - extrae TODO

Retorna SOLO JSON."""
    
    client = anthropic.Anthropic(api_key=api_key)
    
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,  # INCREASED from 4000 to 8000 for more complete output
        temperature=0,
        messages=[{
            "role": "user",
            "content": prompt
        }],
    )
    
    response_text = message.content[0].text
    
    # Extract JSON
    import re
    json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        json_str = json_match.group(0) if json_match else response_text
    
    result = json.loads(json_str)
    
    # Show summary
    interior_count = len(result.get('equipamiento_diferencial_detallado', {}).get('interior', []))
    exterior_count = len(result.get('equipamiento_diferencial_detallado', {}).get('exterior', []))
    tech_count = len(result.get('equipamiento_diferencial_detallado', {}).get('tecnologia', []))
    
    print("\n✓ Extraction complete!", file=sys.stderr)
    print(f"\nExtracted equipment features:", file=sys.stderr)
    print(f"  - Interior: {interior_count} features", file=sys.stderr)
    print(f"  - Exterior: {exterior_count} features", file=sys.stderr)
    print(f"  - Tecnología: {tech_count} features", file=sys.stderr)
    print(f"  - Total: {interior_count + exterior_count + tech_count} features", file=sys.stderr)
    
    if interior_count < 5:
        print(f"\n⚠️  WARNING: Only {interior_count} interior features found.", file=sys.stderr)
        print(f"   Check debug_ocr_text.txt to verify OCR quality.", file=sys.stderr)
    
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