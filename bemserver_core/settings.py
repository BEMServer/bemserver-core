"""Default configuration"""

DEFAULT_CONFIG = {
    # SQLAlchemy parameters
    "SQLALCHEMY_DATABASE_URI": "",
    # Unit definitions
    "UNIT_DEFINITION_FILES": [],
    # Weather data client config
    "WEATHER_DATA_CLIENT_API_URL": "https://api.oikolab.com/weather",
    "WEATHER_DATA_CLIENT_API_KEY": "",
    # SMTP config
    "SMTP_ENABLED": False,
    "SMTP_FROM_ADDR": "",
    "SMTP_HOST": "localhost",
}
