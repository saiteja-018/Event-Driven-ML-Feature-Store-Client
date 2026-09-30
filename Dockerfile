FROM python:3.10-slim

WORKDIR /app

# Install netcat for the run_app.sh script
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x run_app.sh

CMD ["./run_app.sh"]
