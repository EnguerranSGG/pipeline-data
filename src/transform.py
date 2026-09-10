import os
import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

def get_latest_raw_file() -> Path:
    json_files = list(RAW_DIR.glob("weather_*.json"))

    if not json_files:
        raise FileNotFoundError("Aucun fichier JSON trouvé dans data/raw")

    return max(json_files, key=lambda file: file.stat().st_mtime)


def transform_weather():
    input_file = get_latest_raw_file()

    with open(input_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    current = data["current"]

    weather = {
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "observation_time": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
    }

    dataframe = pd.DataFrame([weather])

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_file = PROCESSED_DIR / "weather.csv"

    dataframe.to_csv(
        output_file,
        index=False,
    )

    print(f"Fichier source : {input_file}")
    print(f"Fichier transformé : {output_file}")
    print(dataframe)

    return str(output_file)


if __name__ == "__main__":
    transform_weather()