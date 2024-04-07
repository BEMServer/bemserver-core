FROM timescale/timescaledb:2.14.2-pg16

COPY create_db.sh /install/create_db.sh
RUN sed -i 's/\r$//g' /install/create_db.sh # process the line endings of the shell scripts, which converts Windows line endings to UNIX line endings

COPY create_hypertable.sh /install/create_hypertable.sh
RUN sed -i 's/\r$//g' /install/create_hypertable.sh
