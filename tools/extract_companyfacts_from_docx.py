from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import Path


def extract_docx_json(docx_path: Path) -> dict:
    with zipfile.ZipFile(docx_path, "r") as zf:
        xml = zf.read("word/document.xml").decode("utf-8", "ignore")
    text = re.sub(r"<[^>]+>", "", xml)
    # Find a JSON object that starts with {"cik":...}
    m = re.search(r'(\{\s*"cik"\s*:\s*\d+.*\})\s*$', text, re.S)
    if not m:
        # Fallback: first brace to last brace
        first = text.find("{")
        last = text.rfind("}")
        if first == -1 or last == -1 or last <= first:
            raise ValueError("Could not find JSON payload in .docx")
        payload = text[first : last + 1]
    else:
        payload = m.group(1)
    # Word may encode quotes
    payload = payload.replace("&quot;", '"')
    return json.loads(payload)


def main() -> int:
    try:
        import sys

        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--docx", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    docx_path = Path(args.docx).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = extract_docx_json(docx_path)
    out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    try:
        print(str(out_path))
    except Exception:
        print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
