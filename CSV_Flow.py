import pandas as pd
import functions
from pathlib import Path

from functions import post_submodel_element
import json

with open('config.json', 'r') as f:
    config = json.load(f)

topic = config['topic_csv']

# --- Hilfsfunktion ---
def get_or_default(value, default=None):
    return value if pd.notna(value) else default

# --- CSV Pfad ---
base_path = Path(r"C:\\Users\\Public\\Documents\\OEE_CSVs")
file_csv = base_path / "OEE_Metadaten_one_line.csv" #OEE_Kurz.csv"  # beliebige Datei mit Header + Datensätzen

# --- CSV einlesen ---
try:
    df = pd.read_csv(file_csv, sep=";", dtype=str, encoding="latin1")
except FileNotFoundError as e:
    print(f"Datei nicht gefunden: {e}")
    exit(1)

# --- Funktion: Mapping CSV → ProductionOperationRecords ---
def map_to_record(row: pd.Series) -> dict:
    try:
        quantity = (float(get_or_default(row.get("Gutmenge"), 0)) +
                    float(get_or_default(row.get("A+N"), 0)))
    except ValueError:
        quantity = None

    key_name = f"ProductionOperationRecords CSV {row.get('Datum', '')}{row.get('Von Uhrzeit', '')}"

    return {
        "ProductionOperationRecords": {
            "KeyName": key_name,
            "StartDate": get_or_default(row.get("Datum")),
            "OperationNumber": get_or_default(row.get("Auftrag")),
            "SetupTime": get_or_default(row.get("Bandwechselzeit")),
            "ProductionTime": get_or_default(row.get("Arbeitszeit")),
            "DelayTime": get_or_default(row.get("Stillstandszeit nicht beeinflussbar")),
            "ProducedQuantity": quantity,
            "GoodQuantity": get_or_default(row.get("Gutmenge")),
            "Factors": {
                "Factor1": get_or_default(row.get("Breite")),
                "Factor2": get_or_default(row.get("Dicke")),
                "Factor3": get_or_default(row.get("ZAHNFORM")),
                "Factor4": get_or_default(row.get("SCHRAENKUNG"))
            }
        }
    }

# --- Verarbeitung ---
records = []
for _, row in df.iterrows():
    record = map_to_record(row)
    records.append(record)

for i, record in enumerate(records):
    #print(functions.convert_record_to_body(record))
    functions.safe_post_with_retry(functions.convert_record_to_body(record), topic)
    print(functions.convert_record_to_body(record))
    #post_submodel_element(functions.convert_record_to_body(record), topic)


# --- Ausgabe ---
#print(json.dumps(records, indent=2, ensure_ascii=False))
#post_submodel_element(records, "topic1")
