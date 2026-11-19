"""python -m dashboard [--api URL] [--key KEY] [--sign-in] [--flow]

Signs in, then opens the dashboard the key is for:

    an admin's key   On Store, the admin dashboard: every worker, every chunk
                     and its copies, heartbeats, failures, repair
    a client's key   the client dashboard: only that client's files, the files
                     shared with it, and its own history

The key comes from --key, else API_KEY, else the key saved for this controller
(by registering here or with `python client.py register`). With none that
works, the sign-in portal opens: type a key, or register as a new client and
get one. A controller without authentication opens the admin dashboard.
"""
import argparse
import os
import sys

from . import credentials
from .api import Api, ApiError
from .model import HEARTBEAT_TIMEOUT


def find_key(args, api: Api):
    """(key, where it came from) or (None, None)."""
    if args.key:
        return args.key, "--key"
    if os.environ.get("API_KEY", "").strip():
        return os.environ["API_KEY"].strip(), "API_KEY"
    if not args.sign_in:
        _, saved = credentials.load(api.api_url)
        if saved:
            return saved, credentials.path()
    return None, None


def sign_in(args):
    """The Session to open a dashboard with, or None (the user quit)."""
    from .portal import OLD_CONTROLLER, Portal, Session, older_controller
    api = Api(args.api)
    key, source = find_key(args, api)
    try:
        me = Api(args.api, key).me()
        return Session(api_key=key, me=me)            # a key that works, or no authentication at all
    except ApiError as e:
        if e.status == 401 and not key:               # authentication on, and no key yet
            return Portal(args.api).run()
        if older_controller(e):                       # built before GET /me existed
            return Portal(args.api, key=key, notice=OLD_CONTROLLER).run()
        if e.status == 401:                           # a wrong or revoked key: start afresh
            return Portal(args.api, notice=f"The key from {source} is not accepted here. Sign in or register.").run()
        # the controller is not reachable: keep the key, so Enter tries it again
        return Portal(args.api, key=key, notice=f"{e}. Is the system running (docker compose up -d)? "
                                                  "Enter tries again.").run()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m dashboard", description="On Store: terminal dashboards for the object store")
    p.add_argument("--api", default=os.environ.get("ONSTORE_API", "http://127.0.0.1:5000"),
                   help="the controller (default: %(default)s)")
    p.add_argument("--key", default=None, help="your API key (default: API_KEY, else the one saved for this controller)")
    p.add_argument("--sign-in", action="store_true", help="open the sign-in portal even if a key is saved "
                                                          "(to register, or to switch to another client)")
    p.add_argument("--sign-out", action="store_true", help="forget the key saved for this controller, then quit")
    p.add_argument("--flow", action="store_true", help="admin: open on the flow screen (heartbeats, requests, repairs)")
    p.add_argument("--project", default=os.environ.get("COMPOSE_PROJECT", "object-store"),
                   help="admin: docker compose project, for kill / start (default: %(default)s)")
    p.add_argument("--compose-file", default=None, help="admin: docker-compose.yml, for adding workers "
                                                        "(default: the project's)")
    p.add_argument("--heartbeat-timeout", type=float, default=HEARTBEAT_TIMEOUT,
                   help="admin: the controller's HEARTBEAT_TIMEOUT in seconds (default: %(default)s)")
    args = p.parse_args(argv)

    if args.sign_out:
        gone = credentials.forget(Api(args.api).api_url)
        print("Forgot the saved key." if gone else "No key was saved for this controller.")
        return 0

    session = sign_in(args)
    if session is None:
        return 0
    if session.admin:
        from .app import OnStore
        from .docker_ctl import Docker
        OnStore(api_url=args.api, docker=Docker(args.project, args.compose_file), api_key=session.api_key,
                me=session.me, heartbeat_timeout=args.heartbeat_timeout, start_on_flow=args.flow).run()
        return 0
    from .client_app import ClientDashboard
    if ClientDashboard(api_url=args.api, api_key=session.api_key, me=session.me).run() == "logout":
        print("Signed out: the saved key is forgotten on this computer.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
