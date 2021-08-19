module ExternalSystem {
    use URL;
    use Curl;
    use FileIO;
    use Logging;
    use ServerConfig;

    private config const logLevel = ServerConfig.logLevel;
    const esLogger = new Logger(logLevel);

    enum ChannelType{STDOUT,FILE,HTTP,HTTPS};

    extern const CURLOPT_VERBOSE:CURLoption;
    extern const CURLOPT_USERNAME:CURLoption;
    extern const CURLOPT_PASSWORD:CURLoption;
    extern const CURLOPT_USE_SSL:CURLoption;
    extern const CURLOPT_SSLCERT:CURLoption;
    extern const CURLOPT_SSLKEY:CURLoption;
    extern const CURLOPT_KEYPASSWD:CURLoption;
    extern const CURLOPT_SSLCERTTYPE:CURLoption;
    extern const CURLOPT_CAPATH:CURLoption;
    extern const CURLOPT_CAINFO:CURLoption;

    class Channel {
        proc write(content : string) throws {
            throw new owned Error("All derived classes must implement output");
        }
    }

    class FileChannel : Channel {
        var filePath: string;
        var append: bool;
        
        override proc write(content : string) throws {
            if append {
                appendFile(filePath, content);
            } else {
                writeToFile(filePath, content);
            }
        }
    }
    
    class HttpChannel : Channel {
        var url: string;
        var ssl: bool = false;
        var sslKey: string;
        var sslCert: string;
        var sslCacert: string;
        var sslCapath: string;
        var sslKeyPasswd: string;
        
        override proc write(content : string) throws {
            var writer = openUrlWriter(url);
            var str:bytes;

            if ssl {
                Curl.setopt(writer, CURLOPT_USE_SSL, true);
                Curl.setopt(writer, CURLOPT_SSLCERT, sslCert);
                Curl.setopt(writer, CURLOPT_SSLKEY, sslKey);
                Curl.setopt(writer, CURLOPT_KEYPASSWD, sslKeyPasswd);
                Curl.setopt(writer, CURLOPT_CAINFO, sslCacert);
                Curl.setopt(writer, CURLOPT_CAPATH, '/etc/kubernetes/ssl');
            } 
            
            while(writer.readline(str)) {
                write(str);
            }           
        }
    }    
    
    class ChannelParams {
        var content: string;
    }
    
    class FileChannelParams : ChannelParams {
        var filePath: string;
        var appendFile: bool = false;
        
        proc init(content: string, path: string) {
            super.init(content);
            filePath = path;
        }
    }
    
    proc getExternalChannel(channelType: ChannelType, cParams: borrowed ChannelParams) throws {
    
        select(channelType) {
            when ChannelType.FILE {
                var fcParams = cParams: FileChannelParams;
                return new FileChannel(fcParams.filePath);
            }
            otherwise {
                throw new owned Error("Invalid channelType");
            }
        }
    }

}