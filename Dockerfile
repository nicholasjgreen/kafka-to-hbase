FROM python:3 as build

WORKDIR /app

RUN pip3 install --upgrade pip setuptools wheel

COPY . .

RUN python3 setup.py bdist_wheel

FROM python:3-slim

WORKDIR /dist

COPY --from=build /app/dist/kafka_to_hbase-1.0-py3-none-any.whl .

RUN buildDeps='build-essential python3-dev' && \
    set -x && \
    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
    pip3 install ./kafka_to_hbase-1.0-py3-none-any.whl && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge -y --auto-remove $buildDeps

CMD ["kafka-to-hbase"]