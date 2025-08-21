
def convert_record_to_body(record_json: dict) -> dict:
    """
    Konvertiert eine JSON-Struktur mit ProductionOperationRecords
    in die gewünschte SubmodelElementCollection-Struktur.
    """
    r = record_json["ProductionOperationRecords"]

    return {
        "modelType": "SubmodelElementCollection",
        "idShort": r["KeyName"],
        "value": [
            {"modelType": "Property", "idShort": "StartDate", "value": r["StartDate"], "valueType": "xs:dateTime"},
            {"modelType": "Property", "idShort": "OperationNumber", "value": r["OperationNumber"], "valueType": "xs:int"},
            {"modelType": "Property", "idShort": "SetupTime", "value": r["SetupTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "ProductionTime", "value": r["ProductionTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "DelayTime", "value": r["DelayTime"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "ProducedQuantity", "value": r["ProducedQuantity"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "GoodQuantity", "value": r["GoodQuantity"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "Factor1", "value": r["Factor1"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "Factor2", "value": r["Factor2"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "Factor3", "value": r["Factor3"], "valueType": "xs:float"},
            {"modelType": "Property", "idShort": "Factor4", "value": r["Factor4"], "valueType": "xs:float"},
        ],
    }
