# Cashflow Lakehouse dev environment — JDK 17 (official) + Python 3
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]