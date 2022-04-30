import os, json, time
from enum import Enum
from typing import cast, Dict, List, Optional, Union
import numpy as np # type: ignore
from datetime import datetime
from dateutil import parser # type: ignore
from dataclasses import dataclass
from prometheus_client import start_http_server, Gauge, Info # type: ignore
import arkouda as ak
from arkouda import client, logger
from arkouda.logger import LogLevel
from types import SimpleNamespace as Namespace
from functools import reduce
from operator import add

logger = logger.getArkoudaLogger(name='Arkouda Monitoring', logFormat='%(message)s', 
                                 logLevel=LogLevel.DEBUG)

class MetricCategory(Enum):
    ALL = 'ALL'
    NUM_REQUESTS = 'NUM_REQUESTS'
    RESPONSE_TIME = 'RESPONSE_TIME'
    SERVER = 'SERVER'
    SERVER_INFO = 'SERVER_INFO'
    SYSTEM = 'SYSTEM'
    PER_USER_NUM_REQUESTS = 'PER_USER_NUM_REQUESTS'

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
    REQUEST = 'REQUEST'
    USER = 'USER'

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
                 labels : Optional[List[Label]]=None) -> None:
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'category', category)
        object.__setattr__(self, 'scope', scope)
        object.__setattr__(self, 'value', value)
        object.__setattr__(self, 'timestamp', timestamp)
        object.__setattr__(self, 'labels', labels)

@dataclass(frozen=True)
class LocaleInfo():

    __slots__ = ('server_name', 'locale_name', 'locale_id', 'locale_hostname',
                 'number_of_processing_units', 'physical_memory', 
                 'max_number_of_tasks')

    server_name: str
    locale_name: str
    locale_id: str
    locale_hostname: str
    number_of_processing_units: int
    physical_memory: int
    max_number_of_tasks: int
    
    def __init__(self, server_name : str, locale_name : str, locale_id : str, 
                 locale_hostname: str, number_of_processing_units : int, 
                 physical_memory : int, max_number_of_tasks : int) -> None:
        object.__setattr__(self, 'server_name', server_name)
        object.__setattr__(self, 'locale_name', locale_name)
        object.__setattr__(self, 'locale_id', locale_id)
        object.__setattr__(self, 'locale_hostname', locale_hostname)
        object.__setattr__(self, 'number_of_processing_units', 
                           number_of_processing_units)
        object.__setattr__(self, 'physical_memory', physical_memory)
        object.__setattr__(self, 'max_number_of_tasks', max_number_of_tasks)


@dataclass(frozen=True)
class ServerInfo():

    __slots__ = ('server_name', 'server_hostname', 'server_port', 'version', 
                 'total_physical_memory', 'total_number_of_processing_units',
                 'number_of_locales', 'locales')
    server_name: str
    server_hostname: str
    server_port: int
    version: str
    total_physical_memory: int
    total_number_of_processing_units: int
    number_of_locales: int
    locales: List[LocaleInfo]
    
    def __init__(self, server_name : str, server_hostname : str, server_port : int, 
                 version : str, locales : List[LocaleInfo]) -> None:
        object.__setattr__(self, 'server_name', server_name)
        object.__setattr__(self, 'server_hostname', server_hostname)
        object.__setattr__(self, 'server_port', server_port)
        object.__setattr__(self, 'version', version)
        object.__setattr__(self, 'locales', locales)
        object.__setattr__(self, 'number_of_locales', len(locales))
        object.__setattr__(self, 'total_number_of_processing_units',
                        reduce(add,[loc.number_of_processing_units for loc in locales]))
        object.__setattr__(self, 'total_physical_memory',
                        reduce(add,[loc.physical_memory for loc in locales]))

class ArkoudaMetrics:
    
    __slots__ = ('arkoudaMetricsHost', 'arkoudaMetricsPort', 'serverName', 'exportPort', 
                 'pollingInterval','totalNumberOfRequests',
                 'numberOfRequestsPerCommand','numberOfConnections', '_updateMetric', 
                 'responseTimesPerCommand', 'memoryUsedPerLocale', 'pctMemoryUsedPerLocale',
                 'systemMemoryPerLocale', 'systemProcessingUnitsPerLocale', 'reportedTimestamp',
                 'arkoudaServerInfo','perUserTotalNumberOfRequests','perUserNumberOfRequestsPerCommand')
    
    arkoudaMetricsHost: str
    arkoudaMetricsPort: int
    serverName: str
    exportPort: int
    pollingInterval: int 
    totalNumberOfRequests: Gauge 
    numberOfRequestsPerCommand: Gauge 
    numberOfConnections: Gauge
    responseTimesPerCommand: Gauge
    memoryUsedPerLocale: Gauge
    pctMemoryUsedPerLocale: Gauge
    systemMemoryPerLocale: Gauge
    systemProcessingUnitsPerLocale: Gauge
    reportedTimestamp: Gauge
    arkoudaServerInfo: Info
    perUserTotalNumberOfRequests: Gauge
    perUserNumberOfRequestsPerCommand: Gauge

    def __init__(self, arkoudaMetricsHost : str, arkoudaMetricsPort : int, exportPort : int=5080, 
                 serverName : str='arkouda-metrics', pollingInterval : int=5) -> None:
        self.arkoudaMetricsHost = arkoudaMetricsHost
        self.arkoudaMetricsPort = arkoudaMetricsPort
        self.serverName =  serverName 
        self.exportPort = exportPort
        self.pollingInterval = pollingInterval

        self.numberOfConnections = Gauge('arkouda_number_of_connections', 
                                         'Number of Arkouda connections',
                                         labelnames=['arkouda_server_name'])
        self.reportedTimestamp = Gauge('arkouda_reported_timestamp', 
                                         'Metric timestamp as reported by Arkouda',
                                         labelnames=['arkouda_server_name'])
        self.totalNumberOfRequests = Gauge('arkouda_total_number_of_requests', 
                                           'Total number of Arkouda requests', 
                                           labelnames=['arkouda_server_name'])   
        self.numberOfRequestsPerCommand = Gauge('arkouda_number_of_requests_per_command', 
                                            'Total number of Arkouda requests per command',
                                            labelnames=['command', 
                                                        'arkouda_server_name'])       
        self.responseTimesPerCommand = Gauge('arkouda_response_times_per_command', 
                                    'Response times of Arkouda commands',
                                    labelnames=['command', 
                                                'arkouda_server_name'])         
        self.memoryUsedPerLocale = Gauge('arkouda_memory_used_per_locale', 
                                'Memory used by Arkouda on each locale',
                                labelnames=['locale_name',
                                            'locale_num', 
                                            'locale_hostname',
                                            'arkouda_server_name'])
        self.pctMemoryUsedPerLocale = Gauge('arkouda_pct_memory_used_per_locale', 
                                'Percent of total memory used by Arkouda on each locale',
                                labelnames=['locale_name',
                                            'locale_num', 
                                            'locale_hostname',
                                            'arkouda_server_name'])
        self.systemMemoryPerLocale = Gauge('arkouda_memory_per_locale', 
                                          'System memory on each locale host',
                                          labelnames=['locale_name',
                                                      'locale_num',
                                                      'locale_hostname', 
                                                      'arkouda_server_name'])
        self.systemProcessingUnitsPerLocale = Gauge('arkouda_processing_units_per_locale', 
                                          'System memory on each locale host',
                                          labelnames=['locale_name',
                                                      'locale_num', 
                                                      'locale_hostname',
                                                      'arkouda_server_name'])
        self.perUserTotalNumberOfRequests = Gauge('arkouda_per_user_total_number_of_requests',
                                                  'Total number of Arkouda requests per user',
                                            labelnames=['arkouda_server_name',
                                                       'user'])
        self.perUserNumberOfRequestsPerCommand = Gauge('arkouda_per_user_number_of_requests_per_command',
                                                  'Total number of Arkouda requests per command, per user',
                                            labelnames=['command',
                                                        'arkouda_server_name',
                                                        'user'])

        self._updateMetric = {
            MetricCategory.NUM_REQUESTS: lambda x: self._updateNumberOfRequests(x),
            MetricCategory.RESPONSE_TIME: lambda x: self._updateResponseTimes(x),
            MetricCategory.SERVER: lambda x: self._updateServerMetrics(x),
            MetricCategory.SYSTEM: lambda x: self._updateSystemMetrics(x)
        }

        try:
            ak.connect(self.arkoudaMetricsHost,self.arkoudaMetricsPort)
        except Exception as e:
            raise EnvironmentError(e)

        if not client.connected:
            raise EnvironmentError('Not connected to Arkouda server')

        self._initializeServerInfo()
        print('Completed Initialization of Arkouda Metrics Exporter')        

    def asMetric(self, value : Dict[str,Union[float,int]]) -> Metric:
        scope = MetricScope(value ['scope'])
        labels : Optional[List[Label]]
        
        if scope == MetricScope.LOCALE:
            labels = [Label('locale_name',value=value['locale_name']),
                      Label('locale_num',value=value['locale_num']),
                      Label('locale_hostname',value=value['locale_hostname'])]
        elif scope == MetricScope.USER:
            labels = [Label('user',value=value['user'])]
        else:
            labels = None

        return Metric(name=str(value['name']), 
                  category=MetricCategory(value['category']),
                  scope=MetricScope(value['scope']),
                  value=value['value'],
                  timestamp=parser.parse(cast(str,value['timestamp'])),
                  labels=labels)    
    
    def _updateNumberOfRequests(self, metric : Metric) -> None:
        metricName = metric.name
        metricScope = metric.scope

        if metricScope == MetricScope.USER:
            print(f'METRIC {metric}')

        if metricName == 'total':
            self.totalNumberOfRequests.labels(arkouda_server_name=self.serverName).set(metric.value)
        else:
            self.numberOfRequestsPerCommand.labels(command=metricName, 
                                               arkouda_server_name=self.serverName).set(metric.value)
            
    def _updateResponseTimes(self, metric : Metric) -> None:
        self.responseTimesPerCommand.labels(command=metric.name, 
                                               arkouda_server_name=self.serverName).set(metric.value)

    def _updateServerMetrics(self, metric : Metric) -> None:
        self.numberOfConnections.labels(arkouda_server_name=self.serverName).set(metric.value)   
        
    def _updateReportedTimestamp(self, value : datetime) -> None:     
        self.reportedTimestamp.labels(arkouda_server_name=self.serverName).set(value)
            
    def _updateSystemMetrics(self, metric : Metric) -> None:
        metricName = metric.name
        
        if metricName == 'arkouda_memory_used_per_locale':
            self.memoryUsedPerLocale.labels(locale_name=metric.labels[0].value,
                                            locale_num=metric.labels[1].value,
                                            locale_hostname=metric.labels[2].value,
                                            arkouda_server_name=self.serverName).set(metric.value)
        else:
            self.pctMemoryUsedPerLocale.labels(locale_name=metric.labels[0].value,
                                            locale_num=metric.labels[1].value,
                                            locale_hostname=metric.labels[2].value,
                                            arkouda_server_name=self.serverName).set(metric.value)            
    
    def _initializeServerInfo(self) -> None: 
        info = json.loads(client.generic_msg(cmd='metrics', args=str(MetricCategory.SERVER_INFO)), 
                          object_hook=lambda x: Namespace(**x))    
        
        localeInfos = [LocaleInfo(
                           server_name=self.serverName,
                           locale_name=loc.name,
                           locale_id=loc.id,
                           locale_hostname=loc.hostname,
                           number_of_processing_units=loc.number_of_processing_units,
                           physical_memory=loc.physical_memory,
                           max_number_of_tasks=loc.max_number_of_tasks) for loc in info.locales]
        
        serverInfo = ServerInfo(
                           server_name=self.serverName,
                           server_hostname=info.hostname,
                           server_port=info.server_port,
                           version=info.version,
                           locales=localeInfos)
        self.arkoudaServerInfo = Info('arkouda_server_information', 
                                      'Arkouda server and locales configuration information')
        self.arkoudaServerInfo.info({'arkouda_server_name': serverInfo.server_name,
                                     'arkouda_server_hostname': serverInfo.server_hostname,
                                     'arkouda_version': serverInfo.version,
                                     'arkouda_number_of_locales': str(serverInfo.number_of_locales)})

    def run_metrics_loop(self):
        while True:
            self.fetch()
            time.sleep(self.pollingInterval)

    def fetch(self):
        metrics = json.loads(client.generic_msg(cmd='metrics', args=str(MetricCategory.ALL)), 
                             object_hook=self.asMetric)

        if len(metrics) > 0:
            self._assignTimestamp(metrics)
             
        for metric in metrics:
            self._updateMetric[metric.category](metric)
            logger.debug('UPDATED METRIC {}'.format(metric))

    def _assignTimestamp(self, metrics : List[Metric]) -> None:
            self.reportedTimestamp.labels(arkouda_server_name=self.serverName).set(
                metrics[0].timestamp.timestamp())        

def main():

    arkoudaMetricsHost = os.getenv("METRICS_SERVICE_NAME", "localhost")
    arkoudaMetricsPort = int(os.getenv("METRICS_SERVICE_PORT", "5556"))
    pollingInterval = int(os.getenv("POLLING_INTERVAL_SECONDS", "5"))
    exportPort = int(os.getenv("EXPORT_PORT", "5080"))
    serverName = os.getenv("ARKOUDA_SERVER_NAME","arkouda-metrics")

    metrics = ArkoudaMetrics(
        arkoudaMetricsHost=arkoudaMetricsHost,
        arkoudaMetricsPort=arkoudaMetricsPort,
        serverName=serverName,
        exportPort=exportPort,
        pollingInterval=pollingInterval
    )

    start_http_server(exportPort)
    metrics.run_metrics_loop()

if __name__ == "__main__":
    main()
