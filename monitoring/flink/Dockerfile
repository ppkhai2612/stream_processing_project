FROM flink:1.19.1-scala_2.12-java11

# Prometheus reporter — not bundled in /opt for this image, fetch it directly.
# This one belongs on the system classpath (/opt/flink/lib) since it's loaded
# by Flink's own metrics subsystem via the parent classloader, no per-job
# classloader involved.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.19.1/flink-metrics-prometheus-1.19.1.jar /opt/flink/lib/

# Kafka source connector for the DataStream API, downloaded to a *separate*
# directory (not /opt/flink/lib). Deliberately kept off the system classpath:
# the Kafka source's operator coordinator (FLIP-27) is deserialized on the
# JobManager using the job's own attached-jar classloader (`flink run -j`),
# not the system classpath — and if the same jar is *also* on the system
# classpath, Kafka's classes end up loaded twice by two different
# classloaders, causing `X is not an instance of X` errors (e.g.
# ByteArrayDeserializer vs Deserializer) at runtime. Keeping it in exactly
# one place, attached only via -j, avoids that duplication.
# `flink run -j` only accepts a single jar, so the connector + its
# kafka-clients dependency are merged into one bundle below.
RUN mkdir -p /opt/flink/connector-jars
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar /opt/flink/connector-jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/flink/connector-jars/

# PyFlink runtime — needed so `flink run -py` (submitting this repo's Python
# job) can build the JobGraph. Version pinned to match the cluster (1.19.1).
# apache-flink's pemja dependency compiles a native extension against JDK
# headers, which the base image's JRE-only /opt/java/openjdk lacks — install
# a full JDK just to provide headers for this one build step (Flink itself
# keeps using the JRE at /opt/java/openjdk at runtime).
USER root
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
        openjdk-11-jdk-headless build-essential && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    pip3 install --no-cache-dir --timeout 120 --retries 10 apache-flink==1.19.1 clickhouse-connect && \
    apt-get purge -y openjdk-11-jdk-headless build-essential python3-dev && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN python3 <<'EOF'
import zipfile

sources = [
    "/opt/flink/connector-jars/flink-connector-kafka-3.2.0-1.19.jar",
    "/opt/flink/connector-jars/kafka-clients-3.4.0.jar",
]
seen = set()
with zipfile.ZipFile("/opt/flink/connector-jars/flink-kafka-connector-bundle.jar", "w") as out:
    for src in sources:
        with zipfile.ZipFile(src) as zin:
            for item in zin.namelist():
                if item in seen:
                    continue
                seen.add(item)
                out.writestr(item, zin.read(item))
EOF
RUN rm /opt/flink/connector-jars/flink-connector-kafka-3.2.0-1.19.jar \
       /opt/flink/connector-jars/kafka-clients-3.4.0.jar