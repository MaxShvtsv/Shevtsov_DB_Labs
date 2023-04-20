FROM python:3.10.1-slim-buster

WORKDIR /lab_1_db_test/app

RUN pip install --upgrade pip
COPY requirements.txt /lab_1_db_test/app/requirements.txt
RUN pip install -r requirements.txt

COPY . /lab_1_db_test/app

CMD ["python", "main.py"]