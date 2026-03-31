from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.veille.pipeline import run_pipeline
from src.veille.utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Veille quotidienne assurance/finance")
    parser.add_argument("run", nargs="?", default="run", help="Commande: run")
    parser.add_argument("--config", type=Path, default=Path("config/sources.yml"), help="Chemin du YAML sources")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING...")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    configure_logging(args.log_level)
    result = run_pipeline(args.config)
    print(json.dumps(result["state"], ensure_ascii=False, indent=2))
