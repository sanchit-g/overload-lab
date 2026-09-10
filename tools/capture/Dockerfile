FROM python:3.12-slim
RUN pip install --no-cache-dir requests==2.32.3 matplotlib==3.9.2
WORKDIR /app
COPY capture.py .
ENTRYPOINT ["python", "/app/capture.py"]
