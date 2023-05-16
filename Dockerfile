FROM python:3.10.1-slim-buster

WORKDIR /usr/src/

RUN pip install --upgrade pip
COPY requirements.txt /usr/src/requirements.txt
RUN pip install -r requirements.txt

COPY . /usr/src/

CMD ["python", "main.py"]