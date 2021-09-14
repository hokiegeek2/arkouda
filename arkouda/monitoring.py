import os, json, time
from enum import Enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Union
from prometheus_client import start_http_server, Counter, Gauge
import arkouda as ak
from arkouda import client


class MetricType(Enum):
    ALL = 'ALL'
    MEMORY_USED = 'MEMORY_USED'
    PCT_MEMORY_USED = 'PCT_MEMORY_USED'
    NUM_REQUESTS = 'NUM_REQUESTS'
    NUM_CONNECTIONS = 'NUM_CONNECTIONS'

    def __str__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricType object to JSON.
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricType object to JSON.
        """
        return self.value
    
class MetricScope(Enum):
    GLOBAL = 'GLOBAL'
    LOCALE = 'LOCALE'

    def __str__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricType object to JSON.
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricType object to JSON.
        """
        return self.value

@dataclass(frozen=True)
class Label():

    name: str
    value: Union[float,int,str]
    
    def __init__(self, name : str, value : Union[float,int]) -> None:
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'value', value)


@dataclass(frozen=True)
class Metric():
    __slots__ = ('scope', 'type_', 'value', 'labels')

    scope: MetricScope
    type_: MetricType
    value: Union[float,int]
    labels: List[Label]
    
    def __init__(self, scope : MetricScope, type_ : MetricType, 
                 value : Union[float,int], labels : List[Label]=None) -> None:
        object.__setattr__(self, 'scope', scope)
        object.__setattr__(self, 'type_', type_)
        object.__setattr__(self, 'value', value)
        object.__setattr__(self, 'labels', labels)


def asMetric(value : Dict[str,Union[float,int,str]]) -> Metric:
    scope = MetricScope(value ['scope'])
                    
    if scope == MetricScope.LOCALE:
        labels = [Label('locale_name',value=value['locale_name']),
              Label('locale_num',value=value['locale_num'])]
    else:
        labels = None

    return Metric(scope=MetricScope(value['scope']),
              type_=MetricType(value['name']),
              value=value['value'],
              labels=labels)    

class ArkoudaMetrics:
    
    __slots__ = ('exportPort', 'pollingInterval', 'metricsCache','numberOfRequests',
                 'numberOfConnections', '_updateMetric', 'memoryUsed', 'pctMemoryUsed',
                 '_updateGlobalScopedMetric', '_updateLocaleScopedMetric', 'registry')
    
    exportPort: int
    pollingInterval: int 
    metricsCache: defaultdict 
    numberOfRequests: Counter 
    numberOfConnections: Gauge
    memoryUsed: Gauge

    def __init__(self, exportPort=5080, pollingInterval=5):
        self.exportPort = exportPort
        self.pollingInterval = pollingInterval
        self.metricsCache = defaultdict(lambda:0,{})     
    
        self.numberOfRequests = Gauge('arkouda_number_of_requests', 
                                                    'Number of Arkouda requests')
        self.numberOfConnections = Gauge('arkouda_number_of_connections', 
                                                    'Number of Arkouda connections')
        self.memoryUsed = Gauge('arkouda_memory_used_per_locale', 
                                'Memory used by Arkouda on each locale',
                                labelnames=['locale_name','locale_num'])
        self.pctMemoryUsed = Gauge('arkouda_pct_memory_used_per_locale', 
                                   'Percent memory used by Arkouda on each locale',
                                   labelnames=['locale_name','locale_num'])

        self._updateMetric = {
            MetricType.NUM_REQUESTS: lambda x: self.numberOfRequests.set(x.value),
            MetricType.NUM_CONNECTIONS: lambda x: self.numberOfConnections.set(x.value),
            MetricType.MEMORY_USED: \
                       lambda x: self.memoryUsed.labels(locale_name=x.labels[0].value,
                                                    locale_num=x.labels[1].value).set(x.value),
            MetricType.PCT_MEMORY_USED: \
                       lambda x: self.pctMemoryUsed.labels(locale_name=x.labels[0].value,
                                                    locale_num=x.labels[1].value).set(x.value)  
        } 

        try:
            ak.connect()
        except Exception as e:
            raise EnvironmentError(e)

        if not client.connected:
            raise EnvironmentError('Not connected to Arkouda server')
        
        self._initializeMetrics()
        print('Completed Initialization of Arkouda Exporter')
    
    def _initializeMetrics(self) -> None:
        config = ak.get_config()

        for locale in config['LocaleConfigs']:
            locale_num = locale['id']
            locale_name = locale['name']
            self.memoryUsed.labels(locale_name,locale_num)
            self.pctMemoryUsed.labels(locale_name,locale_num)

    def run_metrics_loop(self):
        while True:
            self.fetch()
            time.sleep(self.pollingInterval)

    def fetch(self):
        metrics = json.loads(client.generic_msg(cmd='metrics', args=str(MetricType.ALL)), 
                             object_hook=asMetric)      
        for metric in metrics:
            self._updateMetric[metric.type_](metric)

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