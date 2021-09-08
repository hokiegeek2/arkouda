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

    enum CountType{NUM_REQUESTS,NUM_CONNECTIONS};
    
    enum MetricType{COUNT};

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
    
    proc exportCountMetrics() throws {
        var metrics: [0..countMetrics.size-1] CounterMetric;
         
        for (i,item) in zip(0..countMetrics.size-1,countMetrics.items()){
            metrics[i] = new CounterMetric(item[0],item[1]);
        }
        return metrics;
    }
    
    
    class Metric {
        var name: string;
    }
    
    record CounterMetric {
        var name: string;
        var value: int;
    }    

    proc metricsMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {       
        var (metricType, metric) = payload.splitMsgToTuple(2);
        
        mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'metricType: %s metric: %s'.format(metricType,metric));
        if metric == 'ALL' {
            var metrics = exportCountMetrics();

            mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'metrics %t'.format(metrics));
            return new MsgTuple("%jt".format(metrics), MsgType.NORMAL);        
        } else { 
            var countMetric = new CounterMetric(name=metric,value=getCount(metric));
            mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'metric %t'.format(countMetric));
            return new MsgTuple("%jt".format(countMetric), MsgType.NORMAL);
        }
    }
}