#!/usr/bin/env python3
"""
Debug script to verify Claude is reading the actual PDF image
This will show you EXACTLY what Claude sees and extracts
"""

import sys
import json
import base64
import os
from pdf2image import convert_from_path
import anthropic
from PIL import Image

Image.MAX_IMAGE_PIXELS = None


def debug_extraction(pdf_path: str, api_key: str):
    """
    Debug version that shows what Claude is seeing
    """
    print("=" * 80, file=sys.stderr)
    print("DEBUG MODE - Verifying Claude's Input", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    
    # Convert PDF to image
    print("\n1. Converting PDF to image...", file=sys.stderr)
    images = convert_from_path(pdf_path, dpi=200)
    image = images[0]
    
    print(f"   Image size: {image.width}x{image.height} pixels", file=sys.stderr)
    
    # Save debug image
    debug_image_path = "debug_image.png"
    image.save(debug_image_path)
    print(f"   ✓ Saved image to: {debug_image_path}", file=sys.stderr)
    print(f"   → Open this file to see what Claude sees!", file=sys.stderr)
    
    # Resize if needed
    max_dimension = 4096
    if image.width > max_dimension or image.height > max_dimension:
        ratio = min(max_dimension / image.width, max_dimension / image.height)
        new_size = (int(image.width * ratio), int(image.height * ratio))
        image = image.resize(new_size, Image.Resampling.LANCZOS)
        print(f"   ✓ Resized to: {image.width}x{image.height} pixels", file=sys.stderr)
    
    # Convert to base64
    print("\n2. Converting to base64...", file=sys.stderr)
    from io import BytesIO
    buffered = BytesIO()
    image.save(buffered, format="PNG", optimize=True)
    image_data = base64.b64encode(buffered.getvalue()).decode()
    print(f"   ✓ Base64 size: {len(image_data)} characters", file=sys.stderr)
    
    # First, ask Claude to just describe what it sees
    print("\n3. Asking Claude to describe what it sees...", file=sys.stderr)
    
    client = anthropic.Anthropic(api_key=api_key)
    
    describe_prompt = """Por favor, describe EXACTAMENTE lo que ves en esta imagen.

Lista:
1. ¿Qué marca y modelo de vehículo es?
2. ¿Cuántas versiones/trim levels puedes ver? Lista sus nombres EXACTOS
3. ¿Qué especificaciones del motor ves? (potencia, torque, etc.)
4. ¿Hay una tabla de comparación? ¿Qué columnas tiene?
5. ¿Ves una tabla de equipamiento con círculos (●) y guiones (-)?

Sé MUY específico y cita el texto exacto que ves."""
    
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        temperature=0,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": image_data,
                    },
                },
                {"type": "text", "text": describe_prompt}
            ],
        }],
    )
    
    description = message.content[0].text
    print("\n" + "=" * 80, file=sys.stderr)
    print("CLAUDE'S DESCRIPTION OF WHAT IT SEES:", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(description, file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    
    # Now do the actual extraction
    print("\n4. Now extracting structured data...", file=sys.stderr)
    
    extract_prompt = """EXTRAE SOLAMENTE LO QUE VES EN ESTA IMAGEN. NO USES CONOCIMIENTO EXTERNO.

Lee el documento palabra por palabra y extrae:

1. VERSIONES: Copia los nombres EXACTOS de las versiones/trim levels
2. MOTOR: Potencia en HP (exactamente como está escrito)
3. MOTOR: Torque en Nm (exactamente como está escrito)
4. TABLA DE EQUIPAMIENTO: Para cada característica, marca true si hay círculo (●), false si hay guión (-)

Retorna JSON con esta estructura:

{
  "vehiculo": {
    "marca": "",
    "modelo": "",
    "versiones": []
  },
  "motor": {
    "potencia_hp": "",
    "torque_nm": ""
  },
  "equipamiento_diferencial_detallado": {
    "features": [
      {"caracteristica": "", "VERSION_1": true, "VERSION_2": false}
    ]
  }
}

CRÍTICO: Copia el texto EXACTO. Si ves "270 HP @ 6400 RPM" escribe eso exactamente, NO inventes "284 HP"."""
    
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        temperature=0,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": image_data,
                    },
                },
                {"type": "text", "text": extract_prompt}
            ],
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
    
    print("\n" + "=" * 80, file=sys.stderr)
    print("EXTRACTED DATA:", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    
    # Print to stdout for normal usage
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    print("\n" + "=" * 80, file=sys.stderr)
    print("VERIFICATION CHECKLIST:", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"✓ Check debug_image.png - does it show your PDF correctly?", file=sys.stderr)
    print(f"✓ Compare Claude's description above with your actual PDF", file=sys.stderr)
    print(f"✓ Check if extracted specs match what's in the image", file=sys.stderr)
    print("=" * 80, file=sys.stderr)


def main():
    if len(sys.argv) < 2:
        print("Usage: python debug_extract.py <pdf_file>", file=sys.stderr)
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
        debug_extraction(pdf_path, api_key)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()