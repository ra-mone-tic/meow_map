FROM python:3.11-slim
WORKDIR /app
COPY server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY server /app
COPY data /app/data
COPY logs /app/logs
ENV PYTHONUNBUFFERED=1
# По умолчанию — API; воркер запускается отдельной командой
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]