FROM python:3.9-slim

# Install dependencies (TA-Lib requires C libraries)
RUN apt-get update && apt-get install -y build-essential wget

# Install Python TA-Lib binary (easiest method) or compile it
RUN pip install ta-lib pandas ib_insync pytz numpy

WORKDIR /app
COPY ib_vwap_strategy_cloud.py .

CMD ["python", "VWAP_RSI_multiple.py"]