FROM grafana/grafana:latest
ENV GF_INSTALL_PLUGINS=grafana-clickhouse-datasource
COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards