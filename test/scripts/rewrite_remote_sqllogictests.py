#!/usr/bin/env python3
import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rewrite h5db SQLLogicTests to use remote file URLs")
    p.add_argument("--input-root", default="test/sql", help="Input SQL test root")
    p.add_argument("--output-root", required=True, help="Output root for rewritten tests")
    p.add_argument("--base-url", required=True, help="Remote base URL that replaces test/data")
    p.add_argument(
        "--prepend-file",
        default="",
        help="Optional SQL file whose contents are prepended to each test file",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    input_root = Path(args.input_root).resolve()
    output_root = Path(args.output_root).resolve()
    base = args.base_url.rstrip("/")

    prepend = ""
    if args.prepend_file:
        prepend = Path(args.prepend_file).read_text(encoding="utf-8").rstrip() + "\n\n"

    for src in sorted(input_root.rglob("*.test")):
        rel = src.relative_to(input_root)
        dst = output_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)

        text = src.read_text(encoding="utf-8")
        # Rewrite all file paths rooted at test/data to remote URLs.
        text = text.replace("test/data/", f"{base}/")

        if prepend:
            text = prepend + text

        dst.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
