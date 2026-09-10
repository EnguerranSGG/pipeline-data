import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
PROCESSED_FILE = DATA_DIR / "processed" / "weather.csv"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://airflow:airflow@localhost:5434/weather",
)


def load_weather():
    dataframe = pd.read_csv(PROCESSED_FILE)

    engine = create_engine(DATABASE_URL)

    with engine.begin() as connection:
        for _, row in dataframe.iterrows():
            connection.execute(
                text("""
                    INSERT INTO weather (
                        latitude,
                        longitude,
                        observation_time,
                        temperature,
                        humidity,
                        wind_speed
                    )
                    VALUES (
                        :latitude,
                        :longitude,
                        :observation_time,
                        :temperature,
                        :humidity,
                        :wind_speed
                    )
                    ON CONFLICT (
                        latitude,
                        longitude,
                        observation_time
                    )
                    DO NOTHING
                """),
                {
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "observation_time": row["observation_time"],
                    "temperature": row["temperature"],
                    "humidity": row["humidity"],
                    "wind_speed": row["wind_speed"],
                },
            )

    print(f"{len(dataframe)} observation(s) traitée(s).")


if __name__ == "__main__":
    load_weather()