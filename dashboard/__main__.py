"""python -m dashboard [--api URL] [--flow]

Starts On Store, the terminal dashboard. It watches the controller at --api
(default http://127.0.0.1:5000, the port docker compose publishes) and needs
nothing else; kill / start / add worker / corrupt also need the docker command.
"""
import argparse
import os

from .app import OnStore
from .docker_ctl import Docker
from .model import HEARTBEAT_TIMEOUT


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="python -m dashboard", description="On Store: terminal dashboard for the object store")
    p.add_argument("--api", default=os.environ.get("ONSTORE_API", "http://127.0.0.1:5000"),
                   help="the controller (default: %(default)s)")
    p.add_argument("--flow", action="store_true", help="open on the flow screen (heartbeats, requests, repairs)")
    p.add_argument("--project", default=os.environ.get("COMPOSE_PROJECT", "object-store"),
                   help="docker compose project, for kill / start (default: %(default)s)")
    p.add_argument("--compose-file", default=None, help="docker-compose.yml, for adding workers (default: the project's)")
    p.add_argument("--heartbeat-timeout", type=float, default=HEARTBEAT_TIMEOUT,
                   help="the controller's HEARTBEAT_TIMEOUT in seconds (default: %(default)s)")
    args = p.parse_args(argv)
    OnStore(api_url=args.api, docker=Docker(args.project, args.compose_file),
            heartbeat_timeout=args.heartbeat_timeout, start_on_flow=args.flow).run()


if __name__ == "__main__":
    main()
