"""Uplink envelope, heartbeat semantics, and the local recorder.

No broker and no network: the MQTT client is replaced with a stub that records
what would have been published. Run with:

    python tests/test_uplink.py
"""

import gzip
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from itri_agent import config as cfgmod            # noqa: E402
from itri_agent.bridge import Bridge, FLAG_CHANGE, FLAG_HEARTBEAT  # noqa: E402
from itri_agent.recorder import Recorder, read_recording          # noqa: E402

for _s in (sys.stdout, sys.stderr):      # cp950 console cannot print CJK
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

ok_n = fail_n = 0


def check(label, cond, extra=""):
    global ok_n, fail_n
    if cond:
        ok_n += 1
        print(f"  [OK]   {label} {extra}")
    else:
        fail_n += 1
        print(f"  [FAIL] {label} {extra}")


def section(t):
    print(f"\n=== {t} ===")


class FakeClient:
    """Stands in for paho. Records publishes; never touches a socket."""

    def __init__(self):
        self.sent = []
        self.fail = False

    def publish(self, topic, payload, qos=0, retain=False):
        class Info:
            rc = 0
        if self.fail:
            Info.rc = 4
        else:
            self.sent.append((topic, payload))
        return Info()

    def username_pw_set(self, *a, **k): pass
    def will_set(self, *a, **k): pass
    def reconnect_delay_set(self, *a, **k): pass
    def connect_async(self, *a, **k): pass
    def loop_start(self): pass
    def loop_stop(self): pass
    def disconnect(self): pass


def make_bridge(**overrides):
    cfg = cfgmod.defaults()
    cfg.update(overrides)
    cred = {"robot_id": "carA", "mqtt_username": "carA",
            "mqtt_password": "x", "mqtt": {"host": "h", "port": 1883}}
    b = Bridge(cfg, cred)
    b.up = FakeClient()
    b.uplink_up = True
    return b


# --------------------------------------------------------------- envelope

section("1. 每一批都帶完整信封")

b = make_bridge()
b.offer("chassis/bat", 1000.0, 55)
b._flush()
topic, payload = b.up.sent[0]
env = json.loads(payload)
check("主題改為 samples", topic == "fleet/carA/samples", f"-> {topic}")
check("帶 schema 版本", env.get("v") == 1, f"-> v={env.get('v')}")
check("帶 robot_id", env.get("id") == "carA")
check("帶 boot_id", bool(env.get("boot")), f"-> {env.get('boot')}")
check("帶 seq", env.get("seq") == 1, f"-> {env.get('seq')}")
check("帶批次時間戳", isinstance(env.get("ts"), (int, float)))
check("資料在 b 裡", env["b"][0][0] == "chassis/bat")

b.offer("chassis/bat", 1001.0, 56)
b._flush()
check("seq 遞增", json.loads(b.up.sent[-1][1])["seq"] == 2)

section("2. 舊伺服器相容:同時發到 raw")

legacy = [t for t, _ in b.up.sent if t.endswith("/raw")]
check("也發到 fleet/carA/raw", len(legacy) == 2, f"-> {len(legacy)} 筆")

b2 = make_bridge(legacy_raw_topic=False)
b2.offer("t", 1.0, 1)
b2._flush()
check("關掉後只發 samples",
      all(t.endswith("/samples") for t, _ in b2.up.sent),
      f"-> {[t for t, _ in b2.up.sent]}")

section("3. 送不出去時 seq 不會被消耗")

b3 = make_bridge(legacy_raw_topic=False)
b3.offer("t", 1.0, 1)
b3.up.fail = True
b3._flush()
check("失敗後 seq 仍為 0", b3.seq == 0, f"-> {b3.seq}")
check("資料退回緩衝", len(b3.buffer) == 1, f"-> {len(b3.buffer)}")
b3.up.fail = False
b3._flush()
check("重送後 seq=1", json.loads(b3.up.sent[-1][1])["seq"] == 1)

# -------------------------------------------------------------- heartbeat

section("4. ★ on_change_only 的語意歧義:心跳把兩種情況分開")

b = make_bridge(on_change_only=True, heartbeat_s=10, max_rate_hz=1000)
t0 = 1000.0
b.offer("s/temp", t0, 25.0)
b.offer("s/temp", t0 + 1, 25.0)          # 值相同,還在心跳間隔內
b.offer("s/temp", t0 + 2, 25.0)
check("值沒變就不重複送", b.relayed == 1, f"-> relayed={b.relayed}")
check("計為 skipped_same", b.skipped_same == 2, f"-> {b.skipped_same}")

b.offer("s/temp", t0 + 11, 25.0)         # 超過 heartbeat_s
check("超過心跳間隔就重送一次", b.relayed == 2, f"-> {b.relayed}")
check("心跳計數 = 1", b.heartbeats == 1)
rows = list(b.buffer)
check("第一筆標記為變化", rows[0][3] == FLAG_CHANGE, f"-> {rows[0][3]}")
check("心跳那筆標記為 heartbeat", rows[1][3] == FLAG_HEARTBEAT, f"-> {rows[1][3]}")

b.offer("s/temp", t0 + 12, 30.0)
check("值真的變了 -> 標記為變化", list(b.buffer)[-1][3] == FLAG_CHANGE)

section("5. 三個時鐘分開記錄")

b = make_bridge(on_change_only=True, heartbeat_s=0, max_rate_hz=1000)
b.offer("s/a", 100.0, 1)
b.offer("s/a", 200.0, 1)                 # 感測器還活著,只是值沒變
check("last_seen 有前進到 200", b._seen_at["s/a"] == 200.0, f"-> {b._seen_at['s/a']}")
check("last_changed 停在 100", b._changed_at["s/a"] == 100.0,
      f"-> {b._changed_at['s/a']}")
check("last_value 保留", b._last["s/a"] == 1)
check("★ 被過濾掉也算 seen —— 這就是「值沒變」與「感測器死了」的差別",
      b._seen_at["s/a"] > b._changed_at["s/a"])

section("6. 停止產出的 topic 會被算成 stale")

b = make_bridge(heartbeat_s=10)
b.offer("s/dead", time.time() - 300, 1)
b.offer("s/live", time.time(), 1)
st = b.stats()
check("stale_topics = 1", st["stale_topics"] == 1, f"-> {st['stale_topics']}")
check("stats 帶 boot_id", bool(st["boot_id"]))
check("stats 帶 seq", "seq" in st)

# --------------------------------------------------------------- recorder

section("7. 本機完整錄製")

tmp = Path(tempfile.mkdtemp(prefix="itri_rec_"))
try:
    r = Recorder(str(tmp), rate_hz=0, rotate_mb=1, max_gb=1)
    for i in range(500):
        r.write("/scan/ranges/len", 1000.0 + i * 0.01, 360)
    r.stop()
    files = sorted(tmp.glob("*.jsonl.gz"))
    check("有寫出檔案", len(files) >= 1, f"-> {len(files)} 個")
    rows = list(read_recording(str(files[0])))
    check("全速模式一筆都不漏", len(rows) == 500, f"-> {len(rows)}")
    check("內容正確", rows[0]["n"] == "/scan/ranges/len" and rows[0]["v"] == 360)
    check("有 gzip 壓縮", files[0].read_bytes()[:2] == b"\x1f\x8b")

    section("8. 取樣模式")

    tmp2 = Path(tempfile.mkdtemp(prefix="itri_rec2_"))
    r2 = Recorder(str(tmp2), rate_hz=2.0)
    for i in range(100):
        r2.write("/t", 1000.0 + i * 0.1, i)     # 來源 10 Hz,錄 2 Hz
    r2.stop()
    n = sum(len(list(read_recording(str(f)))) for f in tmp2.glob("*.jsonl.gz"))
    check("10 Hz 來源以 2 Hz 錄下 約 20 筆", 18 <= n <= 22, f"-> {n} 筆")
    shutil.rmtree(tmp2, ignore_errors=True)

    section("9. 截斷的檔案仍讀得到前面的內容(斷電情境)")

    tmp3 = Path(tempfile.mkdtemp(prefix="itri_rec3_"))
    good = tmp3 / "x.jsonl"
    good.write_text('{"t":1,"n":"a","v":1}\n{"t":2,"n":"a","v":2}\n{"t":3,"n":"a"',
                    encoding="utf-8")
    rows = list(read_recording(str(good)))
    check("讀到兩筆完整的,截斷那筆丟掉", len(rows) == 2, f"-> {len(rows)}")
    shutil.rmtree(tmp3, ignore_errors=True)

    section("10. 錄製掛掉不能拖垮轉發")

    r3 = Recorder(str(tmp), rate_hz=0)
    r3._disabled = True
    r3.write("/t", 1.0, 1)
    check("停用後 write 直接返回,不丟例外", r3.samples == 0)

    section("11. 錄製接在過濾之前(真正的 raw)")

    tmp4 = Path(tempfile.mkdtemp(prefix="itri_rec4_"))
    b = make_bridge(on_change_only=True, max_rate_hz=1.0,
                    record={"enabled": True, "dir": str(tmp4), "rate_hz": 0,
                            "rotate_mb": 64, "max_gb": 1, "compress": False})
    for i in range(20):
        b.ingest("s/x", 1000.0 + i * 0.01, 7)   # 50 Hz、值都一樣 -> 上行只送 1 筆
    b.recorder.stop()
    recorded = sum(len(list(read_recording(str(f))))
                   for f in tmp4.glob("*.jsonl"))
    check("上行只送 1 筆", b.relayed == 1, f"-> {b.relayed}")
    check("★ 本機錄下全部 20 筆", recorded == 20, f"-> {recorded}")
    check("節流與去重確實把 19 筆擋掉",
          b.skipped_rate + b.skipped_same == 19,
          f"-> rate={b.skipped_rate} same={b.skipped_same}")
    shutil.rmtree(tmp4, ignore_errors=True)
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print(f"\n{'=' * 46}\n通過 {ok_n} / {ok_n + fail_n}\n{'=' * 46}")
sys.exit(1 if fail_n else 0)
