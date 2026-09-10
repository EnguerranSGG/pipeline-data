import pandas as pd


def test_weather_columns():
    dataframe = pd.DataFrame([
        {
            "latitude": 50.638,
            "longitude": 3.045,
            "observation_time": "2026-09-10T08:00",
            "temperature": 14.1,
            "humidity": 75,
            "wind_speed": 10.1,
        }
    ])

    expected_columns = [
        "latitude",
        "longitude",
        "observation_time",
        "temperature",
        "humidity",
        "wind_speed",
    ]

    assert list(dataframe.columns) == expected_columns


def test_weather_values():
    dataframe = pd.DataFrame([{
        "latitude": 50.638,
        "longitude": 3.045,
        "observation_time": "2026-09-10T08:00",
        "temperature": 14.1,
        "humidity": 75,
        "wind_speed": 10.1,
    }])

    # Aucune valeur ne doit être manquante
    assert dataframe.isna().sum().sum() == 0

    # L'humidité doit être comprise entre 0 et 100 %
    assert dataframe["humidity"].between(0, 100).all()

    # Coordonnées géographiques valides
    assert dataframe["latitude"].between(-90, 90).all()
    assert dataframe["longitude"].between(-180, 180).all()

    # La vitesse du vent ne peut pas être négative
    assert (dataframe["wind_speed"] >= 0).all()