FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY spcs_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spcs_app/fastapi_app.py .

EXPOSE 8501

CMD ["python", "fastapi_app.py"]
