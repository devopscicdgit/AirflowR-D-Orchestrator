FROM python:3.12-slim

WORKDIR /app

RUN apt update && apt install -y gcc python3-dev

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "api.py"]