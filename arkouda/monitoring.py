import os, json, time
from enum import Enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Union
import numpy as np
from prometheus_client import start_http_server, Counter, Gauge
import arkouda as ak
from arkouda import client, logger
from arkouda.logger import LogLevel

logger = logger.getArkoudaLogger(name='Arkouda Monitoring', logFormat='%(message)s', 
                                 logLevel=LogLevel.DEBUG)

class MetricCategory(Enum):
    ALL = 'ALL'
    NUM_REQUESTS = 'NUM_REQUESTS'
    RESPONSE_TIME = 'RESPONSE_TIME'
    SERVER = 'SERVER'
    SYSTEM = 'SYSTEM'

    def __str__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricCategory object to JSON.
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricCategory object to JSON.
        """
        return self.value
    
class MetricScope(Enum):
    GLOBAL = 'GLOBAL'
    LOCALE = 'LOCALE'

    def __str__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricScope object to JSON.
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a MetricScope object to JSON.
        """
        return self.value

class MetricKey():
    __slots__ = ('name', 'category')
    
    name: str
    category: MetricCategory

@dataclass(frozen=True)
class Label():

    name: str
    value: Union[bool,float,int,str]
    
    def __init__(self, name : str, value : Union[bool,float,int,str]) -> None:
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'value', value)


@dataclass(frozen=True)
class Metric():
    __slots__ = ('name', 'category', 'scope', 'value', 'timestamp', 'labels')

    name: str
    category: MetricCategory
    scope: MetricScope
    value: Union[float,int]
    timestamp: np.datetime64
    labels: List[Label]
    
    def __init__(self, name : str, category : MetricCategory, scope : MetricScope, 
                 value : Union[float,int], timestamp : np.datetime64, 
                 labels : List[Label]=None) -> None:
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'category', category)
        object.__setattr__(self, 'scope', scope)
        object.__setattr__(self, 'value', value)
        object.__setattr__(self, 'timestamp', timestamp)
        object.__setattr__(self, 'labels', labels)


def asMetric(value : Dict[str,Union[float,int,str]]) -> Metric:
    scope = MetricScope(value ['scope'])
                    
    if scope == MetricScope.LOCALE:
        labels = [Label('locale_name',value=value['locale_name']),
              Label('locale_num',value=value['locale_num'])]
    else:
        labels = None

    return Metric(name=value['name'], 
                  category=MetricCategory(value['category']),
                  scope=MetricScope(value['scope']),
                  value=value['value'],
                  timestamp=np.datetime64(value['timestamp']),
                  labels=labels)    

class ArkoudaMetrics:
    
    __slots__ = ('exportPort', 'pollingInterval', 'metricsCache','totalNumberOfRequests',
                 'numberOfRequestsPerCommand','numberOfConnections', '_updateMetric', 
                 'responseTimesPerCommand', 'memoryUsedPerLocale', 'pctMemoryUsedPerLocale',
                 'updateGlobalScopedMetric', '_updateLocaleScopedMetric', 'registry')
    
    exportPort: int
    pollingInterval: int 
    metricsCache: defaultdict 
    totalNumberOfRequests: Counter 
    numberOfRequestsPerCommand: Counter 
    numberOfConnections: Gauge
    responseTimesPerCommand: Gauge
    memoryUsedPerLocale: Gauge
    pctMemoryUsedPerLocale: Gauge

    def __init__(self, exportPort=5080, pollingInterval=5):
        self.exportPort = exportPort
        self.pollingInterval = pollingInterval
        self.metricsCache = defaultdict(lambda:0,{})     

        self.numberOfConnections = Gauge('arkouda_number_of_connections', 
                                                    'Number of Arkouda connections')
        self.totalNumberOfRequests = Gauge('arkouda_total_number_of_requests', 
                                                    'Total number of Arkouda requests')   
        self.numberOfRequestsPerCommand = Gauge('arkouda_number_of_requests_per_command', 
                                                'Total number of Arkouda requests per command',
                                                labelnames=['command'])       
        self.responseTimesPerCommand = Gauge('arkouda_response_times_per_command', 
                                                'Response times of Arkouda commands',
                                                labelnames=['command'])         
        self.memoryUsedPerLocale = Gauge('arkouda_memory_used_per_locale', 
                                'Memory used by Arkouda on each locale',
                                labelnames=['locale_name','locale_num'])
        self.pctMemoryUsedPerLocale = Gauge('arkouda_pct_memory_used_per_locale', 
                                   'Percent of total memory used by Arkouda on each locale',
                                   labelnames=['locale_name','locale_num'])

        self._updateMetric = {
            MetricCategory.NUM_REQUESTS: lambda x: self._updateNumberOfRequests(x),
            MetricCategory.RESPONSE_TIME: lambda x: self._updateResponseTimes(x),
            MetricCategory.SERVER: lambda x: self.numberOfConnections.set(int(x.value)),
            MetricCategory.SYSTEM: lambda x: self._updateSystemMetrics(x)
        }

        try:
            ak.connect('localhost',5556)
        except Exception as e:
            raise EnvironmentError(e)

        if not client.connected:
            raise EnvironmentError('Not connected to Arkouda server')
        
        self._initializeMetrics()
        print('Completed Initialization of Arkouda Exporter')        
    
    def _updateNumberOfRequests(self, metric : Metric) -> None:
        metricName = metric.name
        
        if metricName == 'total':
            self.totalNumberOfRequests.set(metric.value)
        else:
            self.numberOfRequestsPerCommand.labels(command=metricName).set(metric.value)
            
    def _updateResponseTimes(self, metric : Metric) -> None:
        self.responseTimesPerCommand.labels(command=metric.name).set(metric.value)
            
    def _updateSystemMetrics(self, metric : Metric) -> None:
        metricName = metric.name
        
        if metricName == 'arkouda_memory_used_per_locale':
            self.memoryUsedPerLocale.labels(locale_name=metric.labels[0].value,
                                            locale_num=metric.labels[1].value).set(metric.value)
        else:
            self.pctMemoryUsedPerLocale.labels(locale_name=metric.labels[0].value,
                                            locale_num=metric.labels[1].value).set(metric.value)            
        
    
    def _initializeMetrics(self) -> None:
        config = ak.get_config()

        for locale in config['LocaleConfigs']:
            locale_num = locale['id']
            locale_name = locale['name']
            self.memoryUsedPerLocale.labels(locale_name,locale_num)
            self.pctMemoryUsedPerLocale.labels(locale_name,locale_num)

    def run_metrics_loop(self):
        while True:
            self.fetch()
            time.sleep(self.pollingInterval)

    def fetch(self):
        metrics = json.loads(client.generic_msg(cmd='metrics', args=str(MetricCategory.ALL)), 
                             object_hook=asMetric)      
        for metric in metrics:
            self._updateMetric[metric.category](metric)
            logger.debug('UPDATED METRIC {}'.format(metric))

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