"""Exercise the ROS 2 path without ROS installed.

Real rclpy is not available on Windows, so this fakes the two things the
collector actually touches: message objects (which expose
get_fields_and_field_types) and the flatten/filter/buffer plumbing behind them.
It proves the shape of the data and the rate/dedup behaviour; it does not
prove that rclpy subscription or QoS matching works on a real robot.
"""
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from itri_agent.ros2 import flatten

ok = fail = 0


def check(label, cond, extra=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  [OK]   {label} {extra}")
    else:
        fail += 1
        print(f"  [FAIL] {label} {extra}")


class Msg:
    """Stands in for a ROS message: attributes plus a field-type map."""
    def __init__(self, **kw):
        self._f = {k: type(v).__name__ for k, v in kw.items()}
        for k, v in kw.items():
            setattr(self, k, v)

    def get_fields_and_field_types(self):
        return self._f


print("=== 1. 攤平巢狀訊息(像 /odom)===")
odom = Msg(
    header=Msg(stamp=Msg(sec=1786, nanosec=500), frame_id="odom"),
    twist=Msg(twist=Msg(linear=Msg(x=0.62, y=0.0, z=0.0),
                        angular=Msg(x=0.0, y=0.0, z=0.11))),
    pose=Msg(pose=Msg(position=Msg(x=3.2, y=-1.4, z=0.0))),
)
out = {}
flatten("/odom", odom, out)
for k in sorted(out)[:5]:
    print(f"       {k} = {out[k]}")
print(f"       … 共 {len(out)} 個純量")
check("巢狀欄位攤成路徑", out.get("/odom/twist/twist/linear/x") == 0.62)
check("frame_id 這種字串也保留", out.get("/odom/header/frame_id") == "odom")
check("深層 z 角速度", out.get("/odom/twist/twist/angular/z") == 0.11)

print("\n=== 2. 大陣列不會炸開(像 /scan 的 ranges)===")
scan = Msg(angle_min=-3.14, ranges=[1.0] * 360, intensities=[0.0] * 360)
out = {}
flatten("/scan", scan, out, max_array=8)
print(f"       產生 {len(out)} 個項目:{sorted(out)}")
check("360 個 float 沒有變成 360 筆", len(out) <= 4, f"-> {len(out)}")
check("只記長度", out.get("/scan/ranges/len") == 360)
check("純量欄位仍保留", out.get("/scan/angle_min") == -3.14)

print("\n=== 3. 短陣列會展開 ===")
out = {}
flatten("/wheels", Msg(rpm=[10.0, 11.5, 9.8, 10.2]), out, max_array=8)
check("4 個元素展開", len([k for k in out if "/rpm/" in k]) == 4, f"-> {sorted(out)}")

print("\n=== 4. NaN / Inf 不會進資料庫 ===")
out = {}
flatten("/imu", Msg(x=float("nan"), y=float("inf"), z=1.5), out)
check("NaN 被丟掉", "/imu/x" not in out)
check("Inf 被丟掉", "/imu/y" not in out)
check("正常值保留", out.get("/imu/z") == 1.5)

print("\n=== 5. 位元組陣列只記長度 ===")
out = {}
flatten("/cam", Msg(data=b"\x00" * 100000, width=640), out)
check("影像 data 不展開", out.get("/cam/data/len") == 100000)
check("width 保留", out.get("/cam/width") == 640)

print("\n=== 6. 遞迴深度上限 ===")
deep = Msg(a=Msg(b=Msg(c=Msg(d=Msg(e=Msg(f=Msg(g=Msg(h=1))))))))
out = {}
flatten("/deep", deep, out)
check("超過深度就停,不會無限遞迴", len(out) <= 1, f"-> {out}")

print("\n=== 7. 共用的節流與去重(Bridge.offer)===")
from itri_agent.bridge import Bridge

cfg = {
    "local": {"host": "127.0.0.1", "port": 1883, "username": None, "password": None},
    "source": "ros2", "include": [], "exclude": [],
    "max_rate_hz": 2.0, "on_change_only": True, "deadband": 0.0,
    "max_payload_bytes": 8192, "publish_hz": 1.0, "max_batch": 500,
    "buffer_max": 1000, "map": {}, "ros_max_array": 8,
}
cred = {"robot_id": "test", "mqtt_username": "test", "mqtt_password": "x",
        "mqtt": {"host": "127.0.0.1", "port": 1883}}
b = Bridge(cfg, cred)

t = time.time()
b.offer("/odom/twist/twist/linear/x", t, 0.5)          # 收
b.offer("/odom/twist/twist/linear/x", t + 0.1, 0.5)    # 太快 -> 略過
b.offer("/odom/twist/twist/linear/x", t + 1.0, 0.5)    # 夠久但沒變 -> 略過
b.offer("/odom/twist/twist/linear/x", t + 2.0, 0.9)    # 變了 -> 收
s = b.stats()
print(f"       seen={s['seen']} relayed={s['relayed']} "
      f"skipped_rate={s['skipped_rate']} skipped_same={s['skipped_same']}")
check("四筆只轉了兩筆", s["relayed"] == 2)
check("有一筆因頻率被擋", s["skipped_rate"] == 1)
check("有一筆因重複被擋", s["skipped_same"] == 1)

print("\n=== 8. 欄位對應在 ROS 路徑上也能用 ===")
cfg2 = dict(cfg, map={"battery": "/battery_state/percentage",
                      "v": "/odom/twist/twist/linear/x"})
b2 = Bridge(cfg2, cred)
b2.offer("/battery_state/percentage", time.time(), 87.5)
b2.offer("/odom/twist/twist/linear/x", time.time(), 0.62)
check("battery 對應到儀表板欄位", b2._mapped.get("battery") == 87.5)
check("v 對應到儀表板欄位", b2._mapped.get("v") == 0.62)

print(f"\n{'='*48}\n通過 {ok} / {ok+fail}\n{'='*48}")
sys.exit(1 if fail else 0)
