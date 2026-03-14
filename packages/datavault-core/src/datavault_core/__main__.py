from __future__ import annotations

import argparse

import uvicorn

from .app import create_app
from .settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="datavault-core")
    parser.add_argument("command", choices=["serve"], nargs="?", default="serve")
    args = parser.parse_args()
    if args.command == "serve":
        settings = Settings.from_env()
        uvicorn.run(
            create_app(settings),
            host=settings.bind_host,
            port=settings.bind_port,
        )


if __name__ == "__main__":
    main()
