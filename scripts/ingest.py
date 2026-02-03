import argparse
import asyncio

from disinfo_lab.storage import ensure_storage
from disinfo_lab.pipeline import ingest_latest_wp


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ingest",
        description="Pobiera najnowsze artykuły z WP REST API i zapisuje do DB (z mirrorem CSV).",
    )
    p.add_argument(
        "--category",
        type=int,
        default=None,
        help="ID kategorii WordPress (np. 9). Jeśli pominięte: pobiera bez filtra kategorii.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Ile artykułów pobrać (1..100). Domyślnie 50.",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    limit = max(1, min(args.limit, 100))

    # zapewnij storage: sqlite jeśli jest, albo odtwórz z CSV, albo stwórz sqlite
    ensure_storage()

    added, skipped, failed = asyncio.run(
        ingest_latest_wp(category_id=args.category, limit=limit)
    )
    print(f"INGEST: category_id={args.category} limit={limit} added={added} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    main()
