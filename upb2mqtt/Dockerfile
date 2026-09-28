ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py *.html ./

# Run directly rather than under the base image's s6-overlay /init: s6 resets
# the environment for CMD, which hides SUPERVISOR_TOKEN from the app.
ENTRYPOINT ["python3", "-u", "/app/upb2mqtt.py"]
