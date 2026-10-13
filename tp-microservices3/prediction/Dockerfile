FROM python:3.12-slim

WORKDIR /app

# Dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie uniquement du script
COPY app.py .

CMD ["python", "app.py"]