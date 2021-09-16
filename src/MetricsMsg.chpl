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
    use DateTime;

    enum MetricCategory{ALL,NUM_REQUESTS,RESPONSE_TIME,SYSTEM,SERVER};
    enum MetricScope{GLOBAL,LOCALE};

    private config const logLevel = ServerConfig.logLevel;
    const mLogger = new Logger(logLevel);
    
    var serverMetrics = new CounterTable();
    
    var requestMetrics = new CounterTable();
    
    class CounterTable {
        var counts = new map(string, int);
       
        proc get(metric: string) : int {
            if !this.counts.contains(metric) {
                this.counts.add(metric,0);
                return 0;
            } else {
                return this.counts.getValue(metric);
            }
        }   
        
        proc set(metric: string, count: int) {
            this.counts.addOrSet(metric,count);
        }
    
        proc increment(metric: string, increment: int=1) {
            var current = this.get(metric);
            
            // Set new metric value to current plus the increment
            this.set(metric, current+increment);
        }
    
        proc decrement(metric: string, increment: int=1) {
            var current = this.get(metric);
            
            /*
             * Set new metric value to current minus the increment 
             */
            if current >= increment {
                this.set(metric, current-increment);    
            } else {
                this.set(metric,0);
            }
        }   
        
        proc items() {
            return this.counts.items();
        }
        
        proc size() {
            return this.counts.size;
        }
        
        proc total() {
            var count = 0;
            
            for item in this.items() {
                count += item[1];
            }
            
            return count; 
        }
    }
    
    proc exportAllMetrics() throws {        
        var metrics = new list(owned Metric?);

        metrics.extend(getRequestMetrics());
        metrics.extend(getSystemMetrics());
        metrics.extend(getServerMetrics());

        return metrics.toArray();
    }
    
    proc getServerMetrics() throws {
        var metrics: list(owned Metric?);
         
        for item in serverMetrics.items(){
            metrics.append(new Metric(name=item[0], category=MetricCategory.SERVER, 
                                          value=item[1]));
        }
        return metrics;    
    }    

    proc getRequestMetrics() throws {
        var metrics = new list(owned Metric?);

        for item in requestMetrics.items() {
            metrics.append(new Metric(name=item[0], category=MetricCategory.NUM_REQUESTS,
                                          value=item[1]));
        }
        
        metrics.append(new Metric(name='total', category=MetricCategory.NUM_REQUESTS, 
                                          value=requestMetrics.total()));
        return metrics;
    }

    proc getSystemMetrics() throws {
        var loc = 0;
        var metrics = new list(owned Metric?);

        for loc in Locales {
            var used = memoryUsed():int;
            var total = here.physicalMemory();

            metrics.append(new LocaleMetric(name="memory_used",
                             category=MetricCategory.SYSTEM,
                             locale_num=loc.id,
                             locale_name=loc.name,
                             value=used):Metric);
            metrics.append(new LocaleMetric(name="percent_used",
                             category=MetricCategory.SYSTEM,
                             locale_num=loc.id,
                             locale_name=loc.name,
                             value=used/total * 100.0000):Metric);                            
        }
        return metrics;
    }
        
    class Metric {
        var name: string;
        var category: MetricCategory;
        var scope: MetricScope;
        var timestamp: datetime;
        var value: real;
        
        proc init(name: string, category: MetricCategory, 
                                         scope: MetricScope=MetricScope.GLOBAL, value: real) {
            this.name = name;
            this.category = category;
            this.scope = scope;
            this.timestamp = datetime.now();
            this.value = value;
        }
    }
    
    class LocaleMetric : Metric {
        var locale_num: int;
        var locale_name: string;

        proc init(name: string, category: MetricCategory, scope: MetricScope=MetricScope.LOCALE, 
                         value: real, locale_num: int, locale_name: string) {
            super.init(name=name, category=category, scope=scope, value=value);
            this.locale_num = locale_num;
            this.locale_name = locale_name;
        }
    }

    proc metricsMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {       
        var category = payload.splitMsgToTuple(1)[0]:MetricCategory;
            
        mLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            'category: %s'.format(category));
        var metrics: string;

        select category {
            when MetricCategory.ALL {
                metrics = "%jt".format(exportAllMetrics());
            }
            when MetricCategory.NUM_REQUESTS {
                metrics = "%jt".format(getRequestMetrics());
            }
            when MetricCategory.SERVER {
                metrics = "%jt".format(getServerMetrics());
            }
            when MetricCategory.SYSTEM {
                metrics = "%jt".format(getSystemMetrics());
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