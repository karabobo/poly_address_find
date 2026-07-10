#!/usr/bin/env python3
"""TCP bridge from the Docker host bridge address to a host loopback port."""

from __future__ import annotations

import os
import select
import socket
import threading


LISTEN_HOST = os.environ.get("PMROBOT_BRIDGE_LISTEN_HOST", "172.18.0.1")
LISTEN_PORT = int(os.environ.get("PMROBOT_BRIDGE_LISTEN_PORT", "18787"))
TARGET_HOST = os.environ.get("PMROBOT_BRIDGE_TARGET_HOST", "127.0.0.1")
TARGET_PORT = int(os.environ.get("PMROBOT_BRIDGE_TARGET_PORT", "18787"))
BUFFER_SIZE = 64 * 1024


def pipe(client: socket.socket) -> None:
    upstream = socket.create_connection((TARGET_HOST, TARGET_PORT), timeout=10)
    sockets = [client, upstream]
    try:
        for sock in sockets:
            sock.setblocking(False)
        while sockets:
            readable, _, errored = select.select(sockets, [], sockets, 60)
            if errored:
                return
            if not readable:
                return
            for sock in readable:
                try:
                    data = sock.recv(BUFFER_SIZE)
                except OSError:
                    return
                if not data:
                    return
                other = upstream if sock is client else client
                other.sendall(data)
    finally:
        for sock in (client, upstream):
            try:
                sock.close()
            except OSError:
                pass


def main() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(128)
    print(f"bridging {LISTEN_HOST}:{LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)
    while True:
        client, _ = server.accept()
        threading.Thread(target=pipe, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
