#!/usr/bin/env python3
"""Minimal localhost-only HTTP CONNECT proxy for NAS outbound tunnels."""

from __future__ import annotations

import argparse
import selectors
import socket
import socketserver
from dataclasses import dataclass


MAX_HEADER_BYTES = 65536


@dataclass(frozen=True)
class ProxyConfig:
    timeout_seconds: float


class ConnectProxyHandler(socketserver.BaseRequestHandler):
    config: ProxyConfig

    def handle(self) -> None:
        self.request.settimeout(self.config.timeout_seconds)
        try:
            request = self._read_headers()
            if not request:
                return
            first_line = request.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
            method, target, _version = self._parse_request_line(first_line)
            if method.upper() != "CONNECT":
                self._send_response(405, "Method Not Allowed")
                return
            host, port = self._parse_target(target)
            with socket.create_connection((host, port), timeout=self.config.timeout_seconds) as upstream:
                upstream.settimeout(self.config.timeout_seconds)
                self.request.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
                self._relay(self.request, upstream)
        except Exception:
            try:
                self._send_response(502, "Bad Gateway")
            except Exception:
                pass

    def _read_headers(self) -> bytes:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(data) > MAX_HEADER_BYTES:
                raise ValueError("request headers too large")
        return data

    @staticmethod
    def _parse_request_line(line: str) -> tuple[str, str, str]:
        parts = line.split()
        if len(parts) != 3:
            raise ValueError("invalid request line")
        return parts[0], parts[1], parts[2]

    @staticmethod
    def _parse_target(target: str) -> tuple[str, int]:
        if target.startswith("["):
            host, _, rest = target[1:].partition("]")
            if not rest.startswith(":"):
                raise ValueError("missing port")
            port_text = rest[1:]
        else:
            host, sep, port_text = target.rpartition(":")
            if not sep:
                raise ValueError("missing port")
        port = int(port_text)
        if not host or port <= 0 or port > 65535:
            raise ValueError("invalid target")
        return host, port

    def _send_response(self, status: int, reason: str) -> None:
        body = f"{status} {reason}\n".encode("ascii")
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        self.request.sendall(header + body)

    def _relay(self, client: socket.socket, upstream: socket.socket) -> None:
        sel = selectors.DefaultSelector()
        sel.register(client, selectors.EVENT_READ, upstream)
        sel.register(upstream, selectors.EVENT_READ, client)
        with sel:
            while True:
                events = sel.select(self.config.timeout_seconds)
                if not events:
                    return
                for key, _mask in events:
                    source = key.fileobj
                    target = key.data
                    data = source.recv(65536)
                    if not data:
                        return
                    target.sendall(data)


class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()

    ConnectProxyHandler.config = ProxyConfig(timeout_seconds=args.timeout_seconds)
    with ThreadingTCPServer((args.host, args.port), ConnectProxyHandler) as server:
        print(f"pmrobot HTTP CONNECT proxy listening on {args.host}:{args.port}", flush=True)
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
