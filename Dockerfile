FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8001
EXPOSE 8002

CMD ["python", "main.py"]

