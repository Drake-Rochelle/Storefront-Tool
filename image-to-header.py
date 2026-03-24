#!/usr/bin/env python3
import sys
from pathlib import Path
from PIL import Image

def png_to_header(image_path: str):
    # Load and convert to RGBA8
    img = Image.open(image_path).convert("RGBA")
    width, height = img.size
    raw = img.tobytes()

    # Build header filename
    out_path = Path(image_path).with_suffix(".h")
    var_name = Path(image_path).stem.replace("-", "_").replace(".", "_")

    with open(out_path, "w") as f:
        f.write(f"#pragma once\n")
        f.write(f"static const int {var_name}_width = {width};\n")
        f.write(f"static const int {var_name}_height = {height};\n")
        f.write(f"static const unsigned char {var_name}_data[] = {{\n")

        # Write bytes as hex
        for i, b in enumerate(raw):
            f.write(f"0x{b:02X},")
            if (i + 1) % 16 == 0:
                f.write("\n")
        f.write("};\n")

    print(f"Header written to {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python png_to_header.py <image.png>")
        sys.exit(1)
    png_to_header(sys.argv[1])