import os, json
import time
from collections import defaultdict
from dataclasses import dataclass
from prometheus_client import start_http_server, Counter, Gauge, Enum
from arkouda import client

@dataclass
class ArkoudaMetrics:
    
    __slots__ = ('exportPort', 'pollingInterval', 'metricsCache','numberOfRequests',
                 'numberOfConnections', '_updateMetric', 'updateCache')
    
    exportPort: int
    pollingInterval: int 
    metricsCache: defaultdict 
    numberOfRequests: int 
    numberOfConnections: int

    def __init__(self, exportPort=5080, pollingInterval=5):
        self.exportPort = exportPort
        self.pollingInterval = pollingInterval
        self.metricsCache = defaultdict(lambda:0,{})     

        self.numberOfRequests = Counter('arkouda_number_of_requests', 
                                                    'Number of Arkouda requests')
        self.numberOfConnections = Gauge('arkouda_number_of_connections', 
                                                    'Number of Arkouda connections')

        self._updateMetric = {
            'NUM_REQUESTS': lambda x: self.numberOfRequests.inc(x),
            'NUM_CONNECTIONS': lambda x: self.numberOfConnections.inc(x)
        }

        try:
            client.connect()
        except Exception as e:
            raise EnvironmentError(e)

        if not client.connected:
            raise EnvironmentError('Not connected to Arkouda server')
        
        self._initializeMetrics()
        print('Completed Initialization of Arkouda Exporter')
    
    def _initializeMetrics(self) -> None:
        results = json.loads(client.generic_msg(cmd='metrics', 
                                                args='MetricType.COUNT ALL'))

        for result in results:
            metricName = result['name']
            metricValue = self._getMetricValue(metricName,result['value'])
            self._updateMetric[metricName](metricValue)
            self._updateCache(metricName,metricValue)      
    
    def _getMetricValue(self, name : str, value : int) -> int:
        return value - self.metricsCache[name]
    
    def _updateCache(self, name : str, value : int) -> None:
        cachedValue = self.metricsCache[name]
        self.metricsCache[name] = cachedValue + value
    
    def run_metrics_loop(self):
        while True:
            self.fetch()
            time.sleep(self.pollingInterval)

    def fetch(self):
        results = json.loads(client.generic_msg(cmd='metrics', args='MetricType.COUNT ALL'))
        
        for result in results:
            metricName = result['name']
            metricValue = self._getMetricValue(metricName,result['value'])
            self._updateMetric[metricName](metricValue)
            self._updateCache(metricName,metricValue)
            print('metricName: {} metricValue: {}'.format(metricName,metricValue))

def main():
    """Main entry point"""

    pollingInterval = int(os.getenv("POLLING_INTERVAL_SECONDS", "5"))
    exportPort = int(os.getenv("EXPORT_PORT", "5080"))

    metrics = ArkoudaMetrics(
        exportPort=exportPort,
        pollingInterval=pollingInterval
    )
    start_http_server(exportPort)
    metrics.run_metrics_loop()

if __name__ == "__main__":
    main()