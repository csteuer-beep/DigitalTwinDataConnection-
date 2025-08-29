import requests
import time
import logging
import json


# filepath: convert_record.py

def convert_record_to_body(record_json: dict) -> dict:
    """
    Konvertiert eine JSON-Struktur mit ProductionOperationRecords
    in die gewünschte SubmodelElementCollection-Struktur (mit Factors als dynamische Properties).
    """
    r = record_json["ProductionOperationRecords"]

    # dynamische Factor-Properties generieren
    factor_values = [
        {
            "modelType": "Property",
            "idShort": key,         # Name dynamisch übernehmen
            "value": value,
            "valueType": "xs:float" # Annahme: alle Faktoren sind float
        }
        for key, value in r["Factors"].items()
    ]

    return {
        "modelType": "SubmodelElementCollection",
        "idShort": r["KeyName"],
        "value": [
            {"modelType": "Property", "idShort": "StartDate", "value": r["StartDate"], "valueType": "xs:dateTime"},
            {"modelType": "Property", "idShort": "ProductionOrderNumber", "value": r["OperationNumber"], "valueType": "xs:int"},
            {"modelType": "Property", "idShort": "SetupTime", "value": r["SetupTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "ProductionTime", "value": r["ProductionTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "DelayTime", "value": r["DelayTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "ProducedQuantity", "value": r["ProducedQuantity"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "GoodQuantity", "value": r["GoodQuantity"], "valueType": "xs:float"},
            {
                "modelType": "SubmodelElementCollection",
                "idShort": "Factors",
                "value": factor_values
            },
        ],
    }





def post_submodel_element(body: dict, topic: str) -> dict:
    """
    Sendet eine POST-Request mit dem gegebenen JSON-Body an die festgelegte URL.
    """
    url = f"http://127.0.0.1:1880/test/{topic}" #f"http://url:port/submodels/{topic}/submodel-elements"
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=body, headers=headers)
    response.raise_for_status()
    return response.json()

def safe_post_with_retry(body, topic, max_retries=3, delay=3, log_path="C:\\Users\\Public\\Documents\\OEE_CSVs\\log.txt"):
    for attempt in range(1, max_retries + 1):
        try:
            response = post_submodel_element(body, topic)
            return response
        except Exception as e:
            logging.error(f"POST fehlgeschlagen (Versuch {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                logging.error(f"POST endgültig fehlgeschlagen für body: {body} und topic: {topic}")
                with open(log_path, "a", encoding="utf-8") as f:
                    log_entry = {"body": body, "topic": topic}
                    f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")