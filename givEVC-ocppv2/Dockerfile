ARG BUILD_FROM=python:3.11-alpine
FROM ${BUILD_FROM}

RUN apk add --no-cache python3 py3-pip

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app/ ./
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
