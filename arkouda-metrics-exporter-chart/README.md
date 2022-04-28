The arkouda-metrics-exporter-chart is installed as follows:

```
helm install arkouda-metrics-exporter arkouda-metrics-exporter-chart/
```

# Prometheus Scrape Targets

```
    scrape_configs:
      - job_name: prometheus
        static_configs:
          - targets:
            - localhost:9090
      - job_name: arkouda
        static_configs:
          - targets: [arkouda-metrics-exporter.arkouda:5080]
            labels:
              arkouda_instance: "arkouda"
              launch_method: "Kubernetes"
              project: 'ace ventura'
      - job_name: external-arkouda
        static_configs:
          - targets: [external-arkouda-metrics-exporter.arkouda:8090]
            labels:
              arkouda_instance: "external-arkouda"
              launch_method: "Slurm"
              project: 'ace ventura'

      - job_name: slurm
        static_configs:
          - targets: [prometheus-slurm-exporter.monitoring:9090]:
```
