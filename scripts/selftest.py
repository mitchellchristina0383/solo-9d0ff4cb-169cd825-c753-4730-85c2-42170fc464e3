#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次；
  7. 为节点设置持久化内容字节配额，状态页展示已用/剩余字节；
  8. 并发占额与切流交错：恰好一份余量只被并发请求占用一次，
     其余请求被后端原子拒绝（507），代理明确报告 REJECTED_QUOTA（未受理），
     不改投其他节点、不误记为已回执；多字节内容按 UTF-8 字节数计费；
  9. 节点健康但配额已满时切流被 409 拦截，旧路由保持；
 10. 调低配额保留旧记录，直写后端也被原子拒绝，配额持久化在 meta 表；
 11. 慢写入已送达后调低配额，回执仍由原节点返回；取消配额后恢复受理。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id:
                if w["status"] == expect:
                    return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                           {"payload": self.payload}, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: list[subprocess.Popen] = []
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn("backend-a", ROOT / "backend",
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn("backend-b", ROOT / "backend",
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        spawn("proxy", ROOT / "proxy", {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        # 等代理把 PENDING 行落库（请求已送达 A、回执未归）
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A", f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        _, s3 = http("GET", f"{proxy}/api/status")
        row = next(w for w in s3["writes"] if w["id"] == timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        # 直接打开后端 SQLite 核对持久化（后端各自保存写入的硬性要求）
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

        print("\n== 场景 7：为 B 设置字节配额，状态页展示已用/剩余字节 ==")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        used_b = sum(len(r["payload"].encode("utf-8")) for r in rec_b["records"])
        code, qs = http("POST", f"{proxy}/api/nodes/B/quota", {"bytes": used_b + 200})
        check(code == 200 and qs.get("quota_bytes") == used_b + 200,
              "通过代理为 B 设置字节配额",
              f"quota={used_b + 200}B used={qs.get('used_bytes')}B")
        _, s = http("GET", f"{proxy}/api/status")
        qb = s["nodes"]["B"]["quota"]
        check(qb["quota_bytes"] == used_b + 200 and qb["used_bytes"] == used_b
              and qb["remaining_bytes"] == 200,
              "状态页（网页数据源）显示 B 的已用/剩余字节",
              f"used={qb['used_bytes']}B remaining={qb['remaining_bytes']}B")
        code, _ = http("POST", f"{proxy}/api/nodes/B/quota", {"bytes": -1})
        check(code == 400, "非法配额（负数）被 400 拒绝")

        print("\n== 场景 8：切流到 B 后并发占额——同一份余量只被占用一次 ==")
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["active"] == "B", "B 有余量时切流成功")
        mb = "中文字符✓"  # 4 个 CJK + 1 个符号 = 15 个 UTF-8 字节
        mb_bytes = len(mb.encode("utf-8"))
        code, w_mb = http("POST", f"{proxy}/api/writes?delay=0", {"payload": mb})
        _, s = http("GET", f"{proxy}/api/status")
        qb = s["nodes"]["B"]["quota"]
        check(code == 201 and qb["used_bytes"] == used_b + mb_bytes,
              "多字节内容按 UTF-8 字节数计入已用",
              f"{len(mb)} 字符 = {mb_bytes}B, used={qb['used_bytes']}B")
        remaining = qb["remaining_bytes"]
        racers = [SlowPost(proxy, 0, "X" * remaining) for _ in range(6)]
        for t in racers:
            t.start()
        for t in racers:
            t.join(timeout=15)
        oks = [t for t in racers if t.result and t.result[0] == 201]
        rejs = [t for t in racers if t.result and t.result[0] == 507]
        check(len(oks) == 1 and len(rejs) == 5,
              "6 个并发写入竞争恰好一份余量：1 个受理、5 个未受理",
              f"201={len(oks)} 507={len(rejs)}")
        check(all(t.result[1].get("status") == "REJECTED_QUOTA"
                  and t.result[1].get("accepted") is False
                  and not t.result[1].get("uncertain") for t in rejs),
              "未受理响应明确标记 REJECTED_QUOTA 且 accepted=false")
        _, s = http("GET", f"{proxy}/api/status")
        qb = s["nodes"]["B"]["quota"]
        check(qb["used_bytes"] == qb["quota_bytes"] and qb["remaining_bytes"] == 0,
              "余量未被重复占用：已用恰好等于配额",
              f"used={qb['used_bytes']}B quota={qb['quota_bytes']}B")
        rej_ids = {t.result[1]["id"] for t in rejs}
        st_map = {w["id"]: w["status"] for w in s["writes"]}
        check(all(st_map.get(i) == "REJECTED_QUOTA" for i in rej_ids),
              "代理记录中未受理写入为 REJECTED_QUOTA（未误记为已回执）")
        _, rec_a2 = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b2 = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a2 = {r["id"] for r in rec_a2["records"]}
        ids_b2 = {r["id"] for r in rec_b2["records"]}
        check(not (rej_ids & (ids_a2 | ids_b2)), "未受理写入在两个后端都不存在")
        check(len(ids_a2) == len(ids_a), "代理没有向 A 改投任何写入（A 记录数不变）")
        check(len(ids_b2) == len(ids_b) + 2, "B 只多了多字节写入与竞额赢家共 2 条")

        print("\n== 场景 9：B 健康但配额已满 -> 切流保持旧路由 ==")
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw["active"] == "A", "切回 A（A 不限额）")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["nodes"]["B"]["healthy"] is True, "B 仍然健康（/healthz 200）")
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "A"
              and "quota" in str(blocked.get("reason", "")),
              "B 配额满时切流被 409 拦截，现役仍为 A",
              str(blocked.get("reason", ""))[:80])
        _, s = http("GET", f"{proxy}/api/status")
        check(any(ev["kind"] == "SWITCH_BLOCKED" and "quota" in ev["message"]
                  for ev in s["events"]),
              "事件日志记录配额满导致的切流拦截")
        code, w_stay = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "stays-on-a"})
        check(code == 201 and w_stay["node"] == "A", "拦截后新写入仍走旧路由 A")

        print("\n== 场景 10：调低配额保留旧记录，直写后端也被原子拒绝 ==")
        _, rec_b_before = http("GET", f"{proxy}/api/nodes/B/records")
        n_b_before = len(rec_b_before["records"])
        code, qs = http("POST", f"{proxy}/api/nodes/B/quota", {"bytes": 10})
        check(code == 200 and qs["remaining_bytes"] < 0,
              "配额调低到远低于已用，剩余为负",
              f"remaining={qs.get('remaining_bytes')}B")
        _, rec_b_after = http("GET", f"{proxy}/api/nodes/B/records")
        check(len(rec_b_after["records"]) == n_b_before,
              "调低配额后旧记录全部保留", f"records={n_b_before}")
        code, direct = http("POST", f"http://127.0.0.1:{ports['B']}/writes",
                            {"payload": "x"})
        check(code == 507 and direct.get("error") == "quota_exceeded",
              "直接写后端也被 507 拒绝（容量判断在后端原子完成）")
        _, rec_b_after2 = http("GET", f"{proxy}/api/nodes/B/records")
        check(len(rec_b_after2["records"]) == n_b_before, "被拒写入未落库")
        con = sqlite3.connect(work / "b.db")
        row = con.execute("SELECT value FROM meta WHERE key='quota_bytes'").fetchone()
        con.close()
        check(row is not None and int(row[0]) == 10,
              "配额已持久化到后端 SQLite 的 meta 表")

        print("\n== 场景 11：慢写入已送达后调低配额，回执仍由原节点返回 ==")
        slow = SlowPost(proxy, 1.5, "slow-before-quota-cut")
        slow.start()
        deadline = time.time() + 5
        slow_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row_w in s["writes"]:
                if row_w["status"] == "PENDING" and row_w["target_node"] == "A":
                    slow_id = row_w["id"]
                    break
            if slow_id:
                break
            time.sleep(0.1)
        check(bool(slow_id), "慢写入已送达 A 并落库（PENDING）")
        code, qs = http("POST", f"{proxy}/api/nodes/A/quota", {"bytes": 0})
        check(code == 200, "在途期间把 A 配额调为 0")
        slow.join(timeout=10)
        check(slow.result and slow.result[0] == 201 and slow.result[1]["node"] == "A",
              "已送达的慢写入不受配额影响，仍由原节点 A 回执")
        code, rej = http("POST", f"{proxy}/api/writes?delay=0",
                         {"payload": "after-quota-zero"})
        check(code == 507 and rej.get("status") == "REJECTED_QUOTA",
              "配额为 0 后新写入未受理（REJECTED_QUOTA）")
        _, rec_a3 = http("GET", f"{proxy}/api/nodes/A/records")
        check(any(r["id"] == slow_id for r in rec_a3["records"]),
              "调低配额后 A 的旧记录（含慢写入）仍保留")
        code, qs = http("POST", f"{proxy}/api/nodes/A/quota", {"bytes": None})
        check(code == 200 and qs.get("unlimited") is True, "取消 A 配额限制")
        code, w_ok = http("POST", f"{proxy}/api/writes?delay=0",
                          {"payload": "quota-cleared"})
        check(code == 201 and w_ok["status"] == "ACKED", "取消限制后写入恢复受理")
        http("POST", f"{proxy}/api/nodes/B/quota", {"bytes": None})

    finally:
        print("\n== 清理进程 ==")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
