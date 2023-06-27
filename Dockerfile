FROM python:3.11-slim-buster

WORKDIR /app

COPY requirements.txt ./

RUN pip3 install -r requirements.txt

COPY src /app

CMD [ "python", "/app/get_datatypes_and_formats_bcodmo_files.py" ]
