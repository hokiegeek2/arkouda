# Docker Build

From the Arkouda project home, execute the following commands to build the arkouda-metrics-server Docker image:

```
# Package Arkouda client and server
python -m build
tar --exclude="*/*" -tf dist/arkouda*.gz
export ARKOUDA_DIST_FOLDER=arkouda-2022.4.15+231.g093ec76c

# Move the tar.gz file to the Docker build directory
mv dist/arkouda-2022.4.15+231.g093ec76c.tar.gz arkouda.tar.gz

# Specify version
export VERSION=0.4.2

# Build Docker imae
docker build --build-arg ARKOUDA_DIST_FOLDER=$ARKOUDA_DIST_FOLDER -f arkouda-metrics-exporter \
       -t hokiegeek2/arkouda-metrics-exporter:$VERSION .
```

# Kubernetes Deployment

There are two means of deploying arkouda-metrics-exporter on Kubernetes: standalone via the [arkouda-metrics-exporter-chart](https://github.com/hokiegeek2/arkouda/tree/k8s-enterprise/arkouda-metrics-exporter-chart) (for non-k8s Arkouda deployments) or embedded within the [multilocale-dynamic-arkouda-server-chart](https://github.com/hokiegeek2/arkouda/tree/k8s-enterprise/multilocale-dynamic-arkouda-server-chart) for Arkouda-on-k8s

## Configuring arkouda-metrics-exporter

As detailed in the arkouda-metrics-exporter-chart [values.yaml](https://github.com/hokiegeek2/arkouda/blob/k8s-enterprise/arkouda-metrics-exporter-chart/values.yaml), the Arkouda metrics service endpoint has to be specified to enable the arkouda-metrics-exporter to scrape Arkouda metrics. As shown in the example below, the [arkouda.metrics.service](https://github.com/hokiegeek2/arkouda/blob/b262be476953ab8e342d39fc6b3227134c3d12e7/arkouda-metrics-exporter-chart/values.yaml#L100) section features the k8s service name and port for the Arkouda metrics endpoint. Note: the service name is standard Kubernetes notation of <service name>.<namespace>. Accordingly, the metrics service endpoint is named arkouda-external-metrics, is deployed in the arkouda namespade, and the metrics endpoint port is 5556.

```
############ Arkouda Metrics Server Configuration ############

metrics:
  server: 
    appName: external-arkouda-metrics-server
    pollingIntervalSeconds: 5
  service:
    name: external-arkouda-metrics-exporter
    port: 8090
arkouda:
  metrics:
    server:
      name: external-arkouda
      namespace: arkouda
    service:
      name: arkouda-external-metrics.arkouda
      port: 5556
```

## Standalone arkouda-metrics-exporter Kubernetes Deployment 

The arkouda-metrics-exporter-chart is installed as follows:

```
helm install arkouda-metrics-exporter arkouda-metrics-exporter-chart/
```
