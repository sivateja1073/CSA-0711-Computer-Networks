import selectors
import socket
import threading
import time
import json

class MultiProtocolServer:
    def __init__(self, host="127.0.0.1", tcp_port=9001, udp_port=9002):
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.sel = selectors.DefaultSelector()
        self.running = False
        self.logs = []
        self.lock = threading.Lock()

    def start(self):
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((self.host, self.tcp_port))
        self.tcp_sock.listen(32)
        self.tcp_sock.setblocking(False)
        self.sel.register(self.tcp_sock, selectors.EVENT_READ, data="TCP_ACCEPT")

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind((self.host, self.udp_port))
        self.udp_sock.setblocking(False)
        self.sel.register(self.udp_sock, selectors.EVENT_READ, data="UDP_READ")

        self.running = True
        print(f"[SERVER] Listening: TCP={self.host}:{self.tcp_port}, UDP={self.host}:{self.udp_port}")
        while self.running:
            events = self.sel.select(timeout=0.1)
            for key, _ in events:
                if key.data == "TCP_ACCEPT":
                    conn, addr = key.fileobj.accept()
                    conn.setblocking(False)
                    self.sel.register(conn, selectors.EVENT_READ, data={"type": "TCP_CLIENT", "addr": addr})
                elif key.data == "UDP_READ":
                    data, addr = key.fileobj.recvfrom(4096)
                    self._handle_udp(data, addr)
                elif isinstance(key.data, dict) and key.data.get("type") == "TCP_CLIENT":
                    self._handle_tcp(key.fileobj, key.data["addr"])

    def _handle_tcp(self, conn, addr):
        try:
            raw = conn.recv(4096)
            if not raw:
                self.sel.unregister(conn)
                conn.close()
                return
            recv_ts = time.perf_counter()
            payload = json.loads(raw.decode("utf-8"))
            ack_pkt = {
                "protocol": "TCP",
                "client_id": payload["client_id"],
                "status": "ACK_RECEIVED",
                "echo": payload["message"],
                "server_recv_ts": recv_ts,
            }
            conn.sendall(json.dumps(ack_pkt).encode("utf-8"))
            with self.lock:
                self.logs.append(("TCP", payload["client_id"], addr, payload["message"], "ACK_SENT"))
        except Exception:
            self.sel.unregister(conn)
            conn.close()

    def _handle_udp(self, raw, addr):
        recv_ts = time.perf_counter()
        payload = json.loads(raw.decode("utf-8"))
        # Optional echo for RTT measurement; UDP itself has no transport-level ACK
        resp = {
            "protocol": "UDP",
            "client_id": payload["client_id"],
            "status": "NO_TRANSPORT_ACK (Connectionless)",
            "echo": payload["message"],
            "server_recv_ts": recv_ts,
        }
        self.udp_sock.sendto(json.dumps(resp).encode("utf-8"), addr)
        with self.lock:
            self.logs.append(("UDP", payload["client_id"], addr, payload["message"], "DATAGRAM_RX"))

    def stop(self):
        self.running = False
        self.sel.close()
        self.tcp_sock.close()
        self.udp_sock.close()


def run_client(client_id, proto, server_ip, port, messages):
    results = []
    if proto == "TCP":
        t0 = time.perf_counter()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_ip, port))
        conn_time_ms = (time.perf_counter() - t0) * 1000.0
        conn_status = f"CONNECTED (3-way handshake {conn_time_ms:.3f} ms)"
        for msg in messages:
            tx = time.perf_counter()
            pkt = json.dumps({"client_id": client_id, "message": msg}).encode("utf-8")
            sock.sendall(pkt)
            ack = json.loads(sock.recv(4096).decode("utf-8"))
            tx_ms = (time.perf_counter() - tx) * 1000.0
            results.append({
                "client_id": client_id, "server_ip": server_ip, "port": port,
                "protocol": "TCP", "conn_status": conn_status,
                "msg_sent": msg, "msg_received": ack["echo"],
                "tx_time_ms": tx_ms, "ack_status": ack["status"],
            })
        sock.close()
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1.0)
        conn_status = "CONNECTIONLESS (No handshake)"
        for msg in messages:
            tx = time.perf_counter()
            pkt = json.dumps({"client_id": client_id, "message": msg}).encode("utf-8")
            sock.sendto(pkt, (server_ip, port))
            try:
                resp = json.loads(sock.recvfrom(4096)[0].decode("utf-8"))
                tx_ms = (time.perf_counter() - tx) * 1000.0
                rx_msg = resp["echo"]
                ack_st = resp["status"]
            except socket.timeout:
                tx_ms = 1000.0
                rx_msg = "<TIMEOUT / LOST>"
                ack_st = "UNACKNOWLEDGED"
            results.append({
                "client_id": client_id, "server_ip": server_ip, "port": port,
                "protocol": "UDP", "conn_status": conn_status,
                "msg_sent": msg, "msg_received": rx_msg,
                "tx_time_ms": tx_ms, "ack_status": ack_st,
            })
        sock.close()
    return results


if __name__ == "__main__":
    srv = MultiProtocolServer("127.0.0.1", 9001, 9002)
    t_srv = threading.Thread(target=srv.start, daemon=True)
    t_srv.start()
    time.sleep(0.15)

    all_results = []
    threads = []
    lock = threading.Lock()

    def worker(cid, proto, port, msgs):
        res = run_client(cid, proto, "127.0.0.1", port, msgs)
        with lock:
            all_results.extend(res)

    specs = [
        ("Client-T1", "TCP", 9001, ["Hello Server", "Reliable Packet 2", "Final TCP Msg"]),
        ("Client-T2", "TCP", 9001, ["Order #101", "Order #102", "Order #103"]),
        ("Client-U1", "UDP", 9002, ["Sensor Temp=28C", "Sensor Temp=29C", "Sensor Temp=30C"]),
        ("Client-U2", "UDP", 9002, ["VoIP Frame 1", "VoIP Frame 2", "VoIP Frame 3"]),
    ]
    for args in specs:
        th = threading.Thread(target=worker, args=args)
        threads.append(th)
        th.start()
    for th in threads:
        th.join()

    srv.stop()
    print("\n=== CLIENT-SERVER TRANSMISSION LOG ===")
    for r in all_results:
        print(f"{r['client_id']} | {r['protocol']} | {r['server_ip']}:{r['port']} | "
              f"{r['conn_status']} | Sent='{r['msg_sent']}' | Recv='{r['msg_received']}' | "
              f"Time={r['tx_time_ms']:.3f} ms | {r['ack_status']}")

    tcp_times = [r["tx_time_ms"] for r in all_results if r["protocol"] == "TCP"]
    udp_times = [r["tx_time_ms"] for r in all_results if r["protocol"] == "UDP"]
    print("\n=== TCP vs UDP COMPARISON REPORT ===")
    print(f"TCP: msgs={len(tcp_times)}, avg_time={sum(tcp_times)/len(tcp_times):.3f} ms, "
          f"min={min(tcp_times):.3f} ms, max={max(tcp_times):.3f} ms, reliability=100% (ACK)")
    print(f"UDP: msgs={len(udp_times)}, avg_time={sum(udp_times)/len(udp_times):.3f} ms, "
          f"min={min(udp_times):.3f} ms, max={max(udp_times):.3f} ms, reliability=Best-effort (No ACK)")