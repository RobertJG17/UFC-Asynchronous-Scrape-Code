FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 aiohttp bs4

WORKDIR /app
COPY migration-postgres.py migration-postgres.py
COPY /static/csvfiles /static/csvfiles

ENTRYPOINT [ "python3", "migration-postgres.py" ]