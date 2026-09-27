ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY upb2mqtt.py .

CMD ["python3", "-u", "/app/upb2mqtt.py"]
