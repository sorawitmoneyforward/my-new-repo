FROM python:3.10-slim-bullseye

# Java (Spark 実行に必須) + ps コマンド追加
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    procps \
 && rm -rf /var/lib/apt/lists/*

# arm64 / amd64 両方に対応するため JAVA_HOME を自動検出
RUN echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))" >> /etc/profile

# デフォルト環境変数設定
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# uv インストール
RUN pip install uv

WORKDIR /workspace/databricks

COPY pyproject.toml uv.lock /workspace/databricks/
RUN uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt
RUN pip install -r requirements.txt

COPY . /workspace/databricks/
