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

## Standalone arkouda-metrics-export Kubernetes Deployment 

The arkouda-metrics-exporter-chart is installed as follows:

```
helm install arkouda-metrics-exporter arkouda-metrics-exporter-chart/
```
