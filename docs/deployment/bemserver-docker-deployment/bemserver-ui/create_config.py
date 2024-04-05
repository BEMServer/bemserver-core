#!/usr/bin/env python

import os
import secrets


def main():
    BEMSERVER_UI_SETTINGS_FILE = os.getenv(
        "BEMSERVER_UI_SETTINGS_FILE", "/config/bemserver-ui.cfg"
    )
    BEMSERVER_API_HOST = os.getenv("BEMSERVER_API_HOST", "localhost:5000")
    BEMSERVER_API_USE_SSL = os.getenv("BEMSERVER_API_USE_SSL", "0") != "0"
    SECRET_KEY = secrets.token_bytes(32).hex()

    with open(BEMSERVER_UI_SETTINGS_FILE, "w") as fd:
        fd.write(f'BEMSERVER_API_HOST="{BEMSERVER_API_HOST}"\n')
        fd.write(f"BEMSERVER_API_USE_SSL={BEMSERVER_API_USE_SSL}\n")
        fd.write(f'SECRET_KEY="{SECRET_KEY}"\n')

    print(f"Config '{BEMSERVER_UI_SETTINGS_FILE}' created.")

if __name__ == "__main__":
    main()
