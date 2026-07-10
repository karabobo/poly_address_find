"""Small dependency-free WebSocket client for public streaming endpoints."""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
from dataclasses import dataclass
from urllib.parse import urlparse


class WebSocketClientError(RuntimeError):
    pass


@dataclass
class SimpleWebSocketClient:
    url: str
    timeout: float = 20.0
    proxy_url: str = ""

    def __post_init__(self) -> None:
        self._sock: socket.socket | ssl.SSLSocket | None = None

    def __enter__(self) -> "SimpleWebSocketClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def connect(self) -> None:
        parsed = urlparse(self.url)
        if parsed.scheme not in {"ws", "wss"}:
            raise WebSocketClientError(f"unsupported websocket scheme: {parsed.scheme}")
        host = parsed.hostname or ""
        if not host:
            raise WebSocketClientError("missing websocket host")
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        raw_sock = self._open_tcp_socket(host, port, secure=parsed.scheme == "wss")
        sock: socket.socket | ssl.SSLSocket = raw_sock
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw_sock, server_hostname=host)
        sock.settimeout(self.timeout)

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "User-Agent: pm-robot/0.1\r\n"
            "\r\n"
        )
        sock.sendall(request.encode("ascii"))
        response = _read_http_headers(sock)
        status_line, headers = _parse_http_response(response)
        if " 101 " not in f" {status_line} ":
            raise WebSocketClientError(f"websocket handshake failed: {status_line}")
        expected_accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
        ).decode("ascii")
        actual_accept = headers.get("sec-websocket-accept", "")
        if actual_accept and actual_accept != expected_accept:
            raise WebSocketClientError("websocket handshake accept key mismatch")
        self._sock = sock

    def close(self) -> None:
        sock = self._sock
        if not sock:
            return
        try:
            self._send_frame(0x8, b"")
        except (OSError, WebSocketClientError):
            pass
        self._sock = None
        try:
            sock.close()
        except OSError:
            pass

    def send_text(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def send_pong(self, payload: bytes = b"") -> None:
        self._send_frame(0xA, payload)

    def recv_text(self, *, timeout: float | None = None) -> str:
        sock = self._require_sock()
        old_timeout = sock.gettimeout()
        if timeout is not None:
            sock.settimeout(timeout)
        try:
            while True:
                opcode, payload = self._read_frame()
                if opcode == 0x1:
                    return payload.decode("utf-8", errors="replace")
                if opcode == 0x2:
                    return payload.decode("utf-8", errors="replace")
                if opcode == 0x8:
                    raise WebSocketClientError("websocket closed by peer")
                if opcode == 0x9:
                    self.send_pong(payload)
                    continue
                if opcode == 0xA:
                    continue
        except socket.timeout as exc:
            raise TimeoutError("websocket receive timed out") from exc
        finally:
            if timeout is not None:
                sock.settimeout(old_timeout)

    def _open_tcp_socket(self, host: str, port: int, *, secure: bool) -> socket.socket:
        proxy = self.proxy_url or _proxy_from_environment(host, secure=secure)
        if not proxy:
            return socket.create_connection((host, port), timeout=self.timeout)
        proxy_parsed = urlparse(proxy)
        proxy_host = proxy_parsed.hostname or ""
        proxy_port = proxy_parsed.port or 8080
        if not proxy_host:
            raise WebSocketClientError("invalid proxy url")
        raw_sock = socket.create_connection((proxy_host, proxy_port), timeout=self.timeout)
        connect_request = (
            f"CONNECT {host}:{port} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Proxy-Connection: Keep-Alive\r\n"
        )
        if proxy_parsed.username or proxy_parsed.password:
            user = proxy_parsed.username or ""
            password = proxy_parsed.password or ""
            token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
            connect_request += f"Proxy-Authorization: Basic {token}\r\n"
        connect_request += "\r\n"
        raw_sock.sendall(connect_request.encode("ascii"))
        response = _read_http_headers(raw_sock)
        status_line, _headers = _parse_http_response(response)
        if " 200 " not in f" {status_line} ":
            raw_sock.close()
            raise WebSocketClientError(f"proxy CONNECT failed: {status_line}")
        return raw_sock

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        sock = self._require_sock()
        mask = os.urandom(4)
        length = len(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.extend([0x80 | 126])
            header.extend(length.to_bytes(2, "big"))
        else:
            header.extend([0x80 | 127])
            header.extend(length.to_bytes(8, "big"))
        masked = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))
        sock.sendall(bytes(header) + mask + masked)

    def _read_frame(self) -> tuple[int, bytes]:
        header = _read_exact(self._require_sock(), 2)
        first, second = header[0], header[1]
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = int.from_bytes(_read_exact(self._require_sock(), 2), "big")
        elif length == 127:
            length = int.from_bytes(_read_exact(self._require_sock(), 8), "big")
        mask = _read_exact(self._require_sock(), 4) if masked else b""
        payload = _read_exact(self._require_sock(), length) if length else b""
        if masked:
            payload = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))
        return opcode, payload

    def _require_sock(self) -> socket.socket | ssl.SSLSocket:
        if self._sock is None:
            raise WebSocketClientError("websocket is not connected")
        return self._sock


def _read_exact(sock: socket.socket | ssl.SSLSocket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise WebSocketClientError("socket closed while reading")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_http_headers(sock: socket.socket | ssl.SSLSocket) -> bytes:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise WebSocketClientError("socket closed while reading HTTP headers")
        data.extend(chunk)
        if len(data) > 65536:
            raise WebSocketClientError("HTTP headers too large")
    return bytes(data)


def _parse_http_response(raw: bytes) -> tuple[str, dict[str, str]]:
    text = raw.decode("iso-8859-1", errors="replace")
    lines = text.split("\r\n")
    status_line = lines[0] if lines else ""
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return status_line, headers


def _proxy_from_environment(host: str, *, secure: bool) -> str:
    if _host_in_no_proxy(host):
        return ""
    keys = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy") if secure else ("HTTP_PROXY", "http_proxy")
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return ""


def _host_in_no_proxy(host: str) -> bool:
    no_proxy = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    host = host.lower()
    for raw in no_proxy.split(","):
        item = raw.strip().lower()
        if not item:
            continue
        if item == "*" or item == host:
            return True
        if item.startswith(".") and host.endswith(item):
            return True
        if host.endswith(f".{item}"):
            return True
    return False
