FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
RUN wget -q -O /jmx_prometheus_httpserver.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/0.16.1/jmx_prometheus_httpserver-0.16.1-jar-with-dependencies.jar
COPY config.yml /etc/jmx-exporter/config.yml
EXPOSE 5556
CMD ["java", "-jar", "/jmx_prometheus_httpserver.jar", "5556", "/etc/jmx-exporter/config.yml"]