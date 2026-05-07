#!/usr/bin/env python3
"""Strip empty externalDocs blocks from swag-generated OpenAPI specs."""
import json
import sys
from pathlib import Path

import yaml


def strip(doc: dict) -> dict:
    ed = doc.get("externalDocs")
    if isinstance(ed, dict) and not ed.get("url") and not ed.get("description"):
        doc.pop("externalDocs", None)
    return doc


def process(path: Path) -> None:
    text = path.read_text()
    if path.suffix == ".yaml":
        doc = yaml.safe_load(text)
        path.write_text(yaml.safe_dump(strip(doc), sort_keys=False))
    elif path.suffix == ".json":
        doc = json.loads(text)
        path.write_text(json.dumps(strip(doc), indent=4) + "\n")
    else:
        sys.exit(f"unsupported file: {path}")


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        process(Path(arg))
