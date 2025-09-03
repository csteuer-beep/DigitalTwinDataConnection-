# filepath: mqtt_job_processor.py

import json
import time
import random
import logging
import paho.mqtt.client as mqtt
from datetime import datetime

from functions import convert_record_to_body, safe_post_with_retry

# ---- globale Zustände ----
state = {
    "last_status": None,
    "last_ts": None,
    "job_start": None,
    "setup_time": 0.0,
    "prod_time": 0.0,
    "delay_time": 0.0,
    "job_active": False,
    "job_id": "",
    "order_no": 1,
    "filter_sum": 0.0,
    "filter_cnt": 0,
}

SETUP = {"PENDING"}
PROD = {"STARTED", "PARTIAL"}
DELAY = {"ERROR", "INTERRUPTED"}
FINISH = {"FINISHED", "ABORTED"}
ACTIVE = {"PENDING", "STARTED", "PARTIAL"}

# ---- helper ----
def now_iso():
    return datetime.utcnow().isoformat()

def normalize_status(payload: dict) -> dict:
    status = str(
        payload.get("status") or payload.get("Status") or payload.get("state")
        or payload.get("State") or payload.get("event") or payload.get("Event") or ""
    ).strip().upper()
    ts = payload.get("timestamp") or payload.get("Timestamp") or payload.get("Time") or now_iso()
    job = str(payload.get("job") or payload.get("Job") or "").strip()
    return {"status": status, "timestamp": ts, "job": job}

def avg_filterzustand(payload: dict):
    ts = payload.get("Time") or payload.get("timestamp") or payload.get("Timestamp") or now_iso()
    try:
        _ = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        ts = now_iso()

    f = (
        payload.get("Filterzustand")
        or payload.get("Filter_Status")
        or payload.get("filterStatus")
        or (payload.get("ProcessData") or {}).get("Filterzustand")
    )
    try:
        f_num = float(f)
    except (TypeError, ValueError):
        f_num = None

    if state["job_active"] and f_num is not None:
        state["filter_sum"] += f_num
        state["filter_cnt"] += 1

def ts_id(dt=None):
    dt = dt or datetime.now()
    pad = lambda n: str(n).zfill(2)
    return (
        str(dt.year) +
        pad(dt.month) +
        pad(dt.day) + "-" +
        pad(dt.hour) +
        pad(dt.minute) +
        pad(dt.second)
    )

def accumulate(msg: dict):
    status = msg["status"]
    ts_str = msg["timestamp"]
    try:
        now = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000
    except Exception:
        now = time.time() * 1000

    job_id_in = msg.get("job", "")

    print("Job Startet" if not state["job_active"] and status in ACTIVE else "Job Update", status, ts_str, job_id_in)

    # start job
    if not state["job_active"] and status in ACTIVE:
        state["job_active"] = True
        state["job_start"] = now
        state["setup_time"] = state["prod_time"] = state["delay_time"] = 0
        state["filter_sum"] = state["filter_cnt"] = 0
        if job_id_in:
            state["job_id"] = job_id_in

    if state["last_status"] is None or state["last_ts"] is None:
        state["last_status"] = status
        state["last_ts"] = now
        return

    dt = max(0, (now - state["last_ts"]) / 1000)
    if dt > 24 * 3600:
        dt = 0

    if state["last_status"] in SETUP:
        state["setup_time"] += dt
    elif state["last_status"] in PROD:
        state["prod_time"] += dt
    elif state["last_status"] in DELAY:
        state["delay_time"] += dt

    state["last_status"] = status
    state["last_ts"] = now

    uniq = random.randint(0, 999)
    record_id = f"Record{ts_id()}-{uniq}"

    # job finished
    if status in FINISH and state["job_active"] and state["job_start"]:
        filter_avg = (
            (state["filter_sum"] / state["filter_cnt"]) if state["filter_cnt"] > 0 else None
        )

        job_id = state["job_id"] or str(state["order_no"])
        record = {
            "KeyName": record_id,
            "StartDate": datetime.fromtimestamp(state["job_start"] / 1000).isoformat(),
            "OperationNumber": job_id, #was before called ProductionOrderNumber
            "SetupTime": round(state["setup_time"], 3),
            "ProductionTime": round(state["prod_time"], 3),
            "DelayTime": round(state["delay_time"], 3),
            "ProducedQuantity": 1,
            "GoodQuantity": 1,
            "Factors": {
                "FilterzustandAvg": round(filter_avg, 3) if filter_avg is not None else None
            }
        }

        body = convert_record_to_body({"ProductionOperationRecords": record})
        safe_post_with_retry(body, topic="topic1") #"aas_submodel")

        # reset
        state.update({
            "job_active": False,
            "last_status": None,
            "last_ts": None,
            "job_start": None,
            "job_id": "",
            "setup_time": 0.0,
            "prod_time": 0.0,
            "delay_time": 0.0,
            "filter_sum": 0.0,
            "filter_cnt": 0,
        })
        state["order_no"] += 1


# ---- MQTT callbacks ----
def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe("Publish/Job/Status", qos=2)
    client.subscribe("Publish/Job/Processdata", qos=2)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid JSON on {msg.topic}: {e}")
        return

    if msg.topic.endswith("/Status"):
        status_msg = normalize_status(payload)
        accumulate(status_msg)
    elif msg.topic.endswith("/Processdata"):
        avg_filterzustand(payload)


# ---- Main ----
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("127.0.0.1", 1883, 60)
    client.loop_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
