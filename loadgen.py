#!/usr/bin/env python3
"""
rediska eviction load generator.

Floods the server with `key:N` writes forever while periodically re-reading a
small set of `hot:0..H` keys to keep them warm. With an LRU policy the cold
flood gets evicted and key count plateaus in Grafana; the hot keys survive.
With noeviction, writes start getting rejected (OOM) and the counter climbs.

No external dependencies — speaks RESP over a raw socket, so it doesn't rely on
redis-cli's HELLO/COMMAND handshake.

Usage:
    python3 loadgen.py --port 7379
    python3 loadgen.py --port 7379 --value-size 200 --sleep 0.001
"""
import argparse
import socket
import time


def encode(*parts):
    """Encode a command as a RESP array of bulk strings."""
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out.append(f"${len(b)}\r\n".encode())
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


def read_reply(f):
    """Read one RESP reply. Enough of the protocol for SET/GET/errors."""
    line = f.readline()
    if not line:
        raise ConnectionError("connection closed by server")
    t = line[:1]
    if t in (b"+", b"-", b":"):
        return line.rstrip(b"\r\n")
    if t == b"$":
        n = int(line[1:])
        if n == -1:
            return b"$-1"  # nil bulk
        data = f.read(n + 2)  # value + trailing CRLF
        return data[:-2]
    # arrays etc. aren't expected here; return the header line as-is
    return line.rstrip(b"\r\n")


def main():
    ap = argparse.ArgumentParser(description="rediska eviction load generator")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7379)
    ap.add_argument("--value-size", type=int, default=50, help="bytes per value")
    ap.add_argument("--hot-keys", type=int, default=10, help="number of always-warm keys")
    ap.add_argument("--warm-every", type=int, default=200, help="re-GET hot keys every N sets")
    ap.add_argument("--sleep", type=float, default=0.0, help="seconds between sets (throttle)")
    ap.add_argument("--report-every", type=int, default=1000, help="print a status line every N sets")
    args = ap.parse_args()

    s = socket.create_connection((args.host, args.port))
    f = s.makefile("rb")
    value = "x" * args.value_size

    # seed the hot keys once
    for h in range(args.hot_keys):
        s.sendall(encode("SET", f"hot:{h}", value))
        read_reply(f)
    print(f"seeded {args.hot_keys} hot keys; flooding cold keys (Ctrl-C to stop)")

    n = 0
    oom = 0
    start = time.time()
    try:
        while True:
            s.sendall(encode("SET", f"key:{n}", value))
            reply = read_reply(f)
            if reply.startswith(b"-"):  # error reply, e.g. OOM under noeviction
                oom += 1
                if oom == 1 or oom % args.report_every == 0:
                    print(f"[set #{n}] rejected: {reply.decode(errors='replace')}")
            n += 1

            if args.warm_every and n % args.warm_every == 0:
                for h in range(args.hot_keys):
                    s.sendall(encode("GET", f"hot:{h}"))
                    read_reply(f)

            if n % args.report_every == 0:
                rate = n / (time.time() - start)
                print(f"sets={n} oom={oom} rate={rate:.0f}/s")

            if args.sleep:
                time.sleep(args.sleep)
    except KeyboardInterrupt:
        print(f"\nstopped after {n} sets, {oom} rejected")
    finally:
        f.close()
        s.close()


if __name__ == "__main__":
    main()