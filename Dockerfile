FROM python:latest

WORKDIR /app

COPY app.py requirements.txt /app/
COPY data /app/data

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["python", "app.py"]
