docker compose up -d timescaledb pgadmin
docker compose run wait-for-pg
docker compose exec timescaledb /install/create_db.sh
docker compose up -d redis-stack
docker compose build bemserver-api
docker compose run --rm --entrypoint="" --env USER=nonroot bemserver-api /home/nonroot/install/install.sh
docker compose exec timescaledb /install/create_hypertable.sh
docker compose run wait-for-redis
docker compose up -d bemserver-api
docker compose run wait-for-bemserver-api
docker compose build bemserver-ui
docker compose run --rm --entrypoint="" --env USER=nonroot bemserver-ui /home/nonroot/install/install.sh
docker compose up -d bemserver-ui
docker compose up -d celery-worker celery-beat celery-flower
