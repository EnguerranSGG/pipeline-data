import json
from datetime import datetime
from pathlib import Path

import requests


API_URL = "https://api.open-meteo.com/v1/forecast"

RAW_DIR = Path("data/raw")


def extract_weather():
    params = {
        "latitude": 50.6292,
        "longitude": 3.0573,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
        ],
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_file = RAW_DIR / f"weather_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    print(f"Données enregistrées dans : {output_file}")

    return str(output_file)


if __name__ == "__main__":
    extract_weather()