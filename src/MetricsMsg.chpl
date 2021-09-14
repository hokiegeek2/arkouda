module MetricsMsg {
    use ServerConfig;
    use Reflection;
    use ServerErrors;
    use Logging;    
    use List;
    use IO;
    use Map;
    use MultiTypeSymbolTable;
    use MultiTypeSymEntry;
    use Message;
    use Memory.Diagnostics;

    enum MetricType{NUM_REQUESTS,NUM_CONNECTIONS,MEMORY_USED,PCT_MEMORY_USED,ALL};
    enum MetricScope{GLOBAL,LOCALE};

    private config const logLevel = ServerConfig.logLevel;
    const mLogger = new Logger(logLevel);
    
    var countMetrics = new map(string, int);

    proc getCount(metric: string) : int {
        if !countMetrics.contains(metric) {
            countMetrics.add(metric,0);
            return 0;
        }
        
        return countMetrics.getValue(metric);
    }
        
    proc setCount(metric: string, count: int) {
        countMetrics.addOrSet(metric,count);
    }
    
    proc incrementCount(metric: string, count: int=1) {
        var current = getCount(metric);
        setCount(metric, current+1);
    }
    
    proc decrementCount(metric: string, count: int=1) {
        var current = getCount(metric);
        if current >= 1 {
            setCount(metric, current-1);    
        } else {
            setCount(metric,0);
        }
    }
    
    proc exportAllMetrics() throws {
        var mMetrics = getMemoryMetrics();
        var cMetrics = getCounterMetrics();
        
        var metrics = new list(owned Metric?);
         
        for cMetric in cMetrics {
            metrics.append(cMetric);
        }
        
        for mMetric in mMetrics {
            metrics.append(mMetric);
        }
        return metrics.toArray();
    }
    
    proc getCounterMetrics() throws {
        var metrics: [0..countMetrics.size-1] owned Metric?;
         
        for (i,item) in zip(0..countMetrics.size-1,countMetrics.items()){
            metrics[i] = new Metric(name=item[0],value=item[1]);
        }
        return metrics;    
    }    
  
    proc getMemoryMetrics() throws {
        var loc = 0;
        var metrics = new list(owned LocaleMetric?);

        for loc in Locales {
            var used = memoryUsed():int;
            var total = here.physicalMemory();

            metrics.append(new LocaleMetric(name="MEMORY_USED",
                             locale_num=loc.id,
                             locale_name=loc.name,
                             value=used));
            metrics.append(new LocaleMetric(name="PCT_MEMORY_USED",
                             locale_num=loc.id,
                             locale_name=loc.name,
                             value=used/total * 100.0000));                            
        }
        return metrics.toArray();
    }
        
    class Metric {
        var name: string;
        var scope: MetricScope = MetricScope.GLOBAL;
        var value: real;
    }
    
    class LocaleMetric : Metric {
        var locale_num: int;
        var locale_name: string;

        proc init(name: string, scope: MetricScope=MetricScope.LOCALE, value: real,
                           locale_num: int, locale_name: string) {
            super.init(name=name,scope=scope,value=value);
            this.locale_num = locale_num;
            this.locale_name = locale_name;
        }
    }

    proc metricsMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {       
        var metricType = payload.splitMsgToTuple(1)[0]:MetricType;
            
        mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'metricType: %s'.format(metricType));
        var metrics: string;

        select metricType {
            when MetricType.ALL {
                metrics = "%jt".format(exportAllMetrics());
            }
            when MetricType.MEMORY_USED {
                metrics = "%jt".format(getMemoryMetrics());
            }
            otherwise {
                throw getErrorWithContext(getLineNumber(),getModuleName(),getRoutineName(),
                      'Invalid MetricType', 'IllegalArgumentError');
            }
        }

        mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'metrics %t'.format(metrics));
        return new MsgTuple(metrics, MsgType.NORMAL);        
    }
}