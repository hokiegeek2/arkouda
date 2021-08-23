module ExternalSystem {
    use Curl;
    use URL;
    use FileIO;
    use Logging;
    use ServerConfig;

    private config const logLevel = ServerConfig.logLevel;
    const esLogger = new Logger(logLevel);
    
    /*
     * libcurl C constants required to configure the Curl core
     * of HttpChannel objects.
     */
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
    extern const CURLOPT_URL:CURLoption;
    extern const CURLOPT_HTTPHEADER:CURLoption;
    extern const CURLOPT_POSTFIELDS:CURLoption;
    extern const CURLOPT_CUSTOMREQUEST:CURLoption;  

    /*
     * Enum describing the type of channel used to write to an
     * external system.
     */
    enum ChannelType{STDOUT,FILE,HTTP};
    
    /*
     * Enum describing the request type used to write to an
     * external system via HTTP.
     */
    enum HttpRequestType{POST,PUT,PATCH};

    /*
     * Enum describing the request format used to write to an
     * external system via HTTP.
     */
    enum HttpRequestFormat{TEXT,JSON,MULTIPART};    

    /*
     * Base class defining the Arkouda Channel interface
     */
    class Channel {
        proc write(content : string) throws {
            throw new owned Error("All derived classes must implement write");
        }
    }

    /*
     *
     */
    class FileChannel : Channel {
        var path: string;
        var append: bool;
        
        proc init(params: FileChannelParams) {
            super.init();
            this.path = params.path;
            this.append = params.append;
        }
        
        override proc write(content : string) throws {
            if append {
                appendFile(path, content);
            } else {
                writeToFile(path, content);
            }
        }
    }
    
    class HttpChannel : Channel {

        var url: string;
        var requestType: HttpRequestType;
        var requestFormat: HttpRequestFormat;
        var ssl: bool = false;
        var sslKey: string;
        var sslCert: string;
        var sslCacert: string;
        var sslCapath: string;
        var sslKeyPasswd: string;
        var verbose: bool;
        
        proc init(params: HttpChannelParams) {
            super.init();
            this.url = params.url;
            this.requestType = params.requestType;
            this.requestFormat = params.requestFormat;
            this.ssl = params.ssl;

            if this.ssl {
                this.sslKey = params.sslKey;
                this.sslCert = params.sslCert;
                this.sslCacert = params.sslCacert;
                this.sslCapath = params.sslCapath;
                this.sslKeyPasswd = params.sslKeyPasswd;
            }

            this.verbose = params.verbose;
        }
        
        proc configureSsl(channel) throws {
            Curl.setopt(channel, CURLOPT_USE_SSL, this.ssl);
            Curl.setopt(channel, CURLOPT_SSLCERT, this.sslCert);
            Curl.setopt(channel, CURLOPT_SSLKEY, this.sslKey);
            Curl.setopt(channel, CURLOPT_KEYPASSWD, this.sslKeyPasswd);
            Curl.setopt(channel, CURLOPT_CAINFO, this.sslCacert);
            Curl.setopt(channel, CURLOPT_CAPATH, this.sslCapath);        
        }
        
        proc generateHeader(channel, format: HttpRequestFormat) throws {
            var args = new Curl.slist();

            select(format) {     
                when HttpRequestFormat.JSON {
                    args.append("Accept: application/json");
                    args.append("Content-Type: application/json");                    
                }     
                when HttpRequestFormat.TEXT {
                    args.append("Accept: text/plain");
                    args.append("Content-Type: text/plain; charset=UTF-8");
                } 
                otherwise {
                    throw new Error("Unsupported HttpFormat");
                }
                
            }
            Curl.easySetopt(channel, CURLOPT_HTTPHEADER, args);  
            return args;
        }
        
        override proc write(content : string) throws {
            var curl = Curl.easyInit();

            Curl.easySetopt(curl, CURLOPT_URL, this.url);
            
            if this.verbose {
                Curl.easySetopt(curl, CURLOPT_VERBOSE, true);
            }

            if this.ssl {
                configureSsl(curl);
            } 
            
            var args = generateHeader(curl, this.requestFormat);

            Curl.easySetopt(curl, CURLOPT_POSTFIELDS, content);
            Curl.easySetopt(curl, CURLOPT_CUSTOMREQUEST, 'POST');

            var ret = Curl.easyPerform(curl);

            writeln("perform returned ", ret);

            args.free();
            Curl.easyCleanup(curl);           
        }
    }    
    
    /*
     * Encapsulates config parameters needed to open and write to
     * a channel connected to an external system.
     */
    class ChannelParams {
      var channelType: ChannelType;
    }
    
    /*
     * Encapsulates config parameters needed to open and write to
     * a channel connected to a file.
     */   
    class FileChannelParams : ChannelParams {
        var path: string;
        var append: bool;
        
        proc init(channelType: ChannelType, path: string, append: bool=false) {
            super.init(channelType);
            this.path = path;
            this.append = append;
        }
    }

    /*
     * Encapsulates config parameters needed to open and write to
     * a HTTP or HTTPS connection.
     */     
    class HttpChannelParams : ChannelParams {
        var url: string;
        var requestType: HttpRequestType;
        var requestFormat: HttpRequestFormat;
        var verbose: bool;
        var ssl: bool = false;
        var sslKey: string;
        var sslCert: string;
        var sslCacert: string;
        var sslCapath: string;
        var sslKeyPasswd: string;
        
        proc init(channelType: ChannelType, url: string, requestType: HttpRequestType,
                    requestFormat: HttpRequestFormat, verbose: bool=false, ssl: bool=false, 
                    sslKey: string, sslCert: string, sslCacert: string, sslCapath: string, 
                          sslKeyPasswd: string) {
            super.init(channelType);
            this.url = url;
            this.requestType = requestType;
            this.requestFormat = requestFormat;
            this.verbose = verbose;
            this.ssl = ssl;
            if this.ssl {
                this.sslKey = sslKey;
                this.sslCert = sslCert;
                this.sslCacert = sslCacert;
                this.sslCapath = sslCapath;
                this.sslKeyPasswd = sslKeyPasswd;
            }
        }
    }
    
    /*
     * Factory function used to retrieve a Channel based upon ChannelParams.
     */
    proc getExternalChannel(params: borrowed ChannelParams) : Channel throws {
        const channelType = params.channelType;

        select(channelType) {
            when ChannelType.FILE {
                return new FileChannel(params: FileChannelParams);
            } 
            when ChannelType.HTTP {
                return new HttpChannel(params: HttpChannelParams);
            }
            otherwise {
                throw new owned Error("Invalid channelType");
            }
        }
    }

}