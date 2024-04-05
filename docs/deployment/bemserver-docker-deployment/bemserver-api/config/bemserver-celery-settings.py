beat_schedule = {
    "service_id": {  # Unique identifier of your choice
        "task": "ServiceName",  # Task name of the service
        "schedule": 5,  # Scheduling interval in seconds
        "args": ("arg_1", "args_2"),  # Task arguments
        "kwargs": {"kwarg1": "val1", "kwarg2": "val2"},  # Task keyword arguments
    },
}
