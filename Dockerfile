FROM python:3.11-slim-buster

WORKDIR /app/src

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt

# COPY ./src /src

# CMD [ "python3", "./get_datatypes_and_formats_bcodmo_files.py" ]
