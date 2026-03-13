import argparse
import asyncio
import sys

from disinfo_lab.config import cfg
from disinfo_lab.llm_label import ollama_is_available
from disinfo_lab.storage import ensure_storage
from disinfo_lab.pipeline import label_latest


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="label",
        description="Etykietuje artykuły z DB przez Ollama (z mirrorem CSV).",
    )
    p.add_argument(
        "--task",
        type=str,
        default="stance_influence_v1",
        help="Nazwa taska w llm_labels.",
    )
    p.add_argument(
        "--batch",
        type=int,
        default=200,
        help="Maksymalna liczba artykułów do rozważenia (najnowsze).",
    )
    p.add_argument(
        "--category-filter",
        type=str,
        default=None,
        help="Opcjonalnie: etykietuj tylko Article.category == (np. '9' lub 'opinia').",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    batch = max(1, args.batch)

    # zapewnij storage: sqlite jeśli jest, albo odtwórz z CSV, albo stwórz sqlite
    ensure_storage()

    ok, error = asyncio.run(ollama_is_available())
    if not ok:
        print(
            "Ollama is not reachable.\n"
            f"Configured base URL: {cfg.ollama_base_url}\n"
            f"Configured model: {cfg.ollama_model}\n"
            f"Connection error: {error}",
            file=sys.stderr,
        )
        raise SystemExit(2)

    added, skipped, failed = asyncio.run(
        label_latest(task=args.task, batch_limit=batch, category_filter=args.category_filter)
    )
    print(
        f"LABEL: task={args.task} batch={batch} category_filter={args.category_filter} "
        f"added={added} skipped={skipped} failed={failed}"
    )


if __name__ == "__main__":
    main()
