ARG BUILD_FROM=python:3.11-alpine
FROM ${BUILD_FROM}

RUN apk add --no-cache python3 py3-pip tzdata su-exec

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app/ ./
COPY run.sh /run.sh
RUN chmod +x /run.sh

RUN addgroup -S givevc && adduser -S givevc -G givevc
RUN chown -R givevc:givevc /app

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
