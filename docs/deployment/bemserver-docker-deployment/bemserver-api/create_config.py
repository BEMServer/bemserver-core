#!/usr/bin/env python

import os


def main():
    BEMSERVER_CORE_SETTINGS_FILE = os.getenv(
        "BEMSERVER_CORE_SETTINGS_FILE", "/config/bemserver-core-settings.py"
    )
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "SQLALCHEMY_DATABASE_URI",
        "postgresql+psycopg2://user:password@localhost:5432/bemserver",
    )
    WEATHER_DATA_CLIENT_API_KEY = os.getenv("WEATHER_DATA_CLIENT_API_KEY", "apikey")

    with open(BEMSERVER_CORE_SETTINGS_FILE, "w") as fd:
        fd.write(f'SQLALCHEMY_DATABASE_URI="{SQLALCHEMY_DATABASE_URI}"\n')
        fd.write(f'WEATHER_DATA_CLIENT_API_KEY="{WEATHER_DATA_CLIENT_API_KEY}"\n')
    # print(f'SQLALCHEMY_DATABASE_URI="{SQLALCHEMY_DATABASE_URI}"')
    # print(f'WEATHER_DATA_CLIENT_API_KEY="{WEATHER_DATA_CLIENT_API_KEY}"')


if __name__ == "__main__":
    main()
