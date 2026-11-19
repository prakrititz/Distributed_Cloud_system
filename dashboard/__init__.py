"""On Store: terminal dashboards for the object store (admin and client).

It is a separate program: it talks to the controller only through the REST API
(and, for kill / start / scale / corrupt, through the docker command), and
imports nothing from the rest of the project. Start it with

    python -m dashboard
"""
