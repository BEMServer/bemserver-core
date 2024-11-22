from celery.schedules import crontab

# PostgreSQL
# TODO: edit
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg://username:password@localhost:5432/bemserver"
)

# Celery
timezone = "Europe/Paris"
CELERY_CONFIG = {
    "timezone": timezone,
    "beat_schedule": {
        "cleanup": {
            "task": "Cleanup",
            "schedule": crontab(minute="*/5"),
        },
        "check_missing": {
            "task": "CheckMissing",
            "schedule": crontab(minute="0", hour="2", day_of_month="*"),
            "kwargs": {
                "timezone": timezone,
                "min_completeness_ratio": 0.5,
                "period": "day",
                "period_multiplier": 1,
            },
        },
        "check_outliers": {
            "task": "CheckOutliers",
            "schedule": crontab(minute="0", hour="2", day_of_month="*"),
            "kwargs": {
                "timezone": timezone,
                "min_correctness_ratio": 0.5,
                "period": "day",
                "period_multiplier": 1,
            },
        },
    },
}
