import argparse
import asyncio
import sys

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local analytics API")
    parser.add_argument("--port", type=int, default=58000)
    args = parser.parse_args()
    server = uvicorn.Server(
        uvicorn.Config(
            "ipaklink_analytics.api.main:app",
            host="127.0.0.1",
            port=args.port,
            access_log=False,
        )
    )
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    with asyncio.Runner(loop_factory=loop_factory) as runner:
        runner.run(server.serve())


if __name__ == "__main__":
    main()
