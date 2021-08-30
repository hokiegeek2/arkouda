module ExternalSystem {
    use Curl;
    use URL;
    use Reflection;
    use FileIO;
    use Logging;
    use ServerConfig;
    use ServerErrors

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
     * Enum describing the external system Arkouda will connect to.
     */
    enum SystemType{KUBERNETES,REDIS,CONSUL,NONE};

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
     * Base class defining the Arkouda Channel interface consisting of a
     * write method that writes a payload to an external system.
     */
    class Channel {
        proc write(payload : string) throws {
            throw new owned Error("All derived classes must implement write");
        }
    }

    /*
     * The FileChannel class writes a payload out to a file, either by appending
     * or overwriting an existing file or creating and writing to a new file.
     */
    class FileChannel : Channel {
        var path: string;
        var append: bool;
        
        proc init(params: FileChannelParams) {
            super.init();
            this.path = params.path;
            this.append = params.append;
        }
        
        override proc write(payload: string) throws {
            if append {
                appendFile(path, payload);
            } else {
                writeToFile(path, payload);
            }
        }
    }
    
    /*
     * The HttpChannel class writes a payload out to an HTTP/S endpoint
     * in a configurable format via a configurable request type.
     */
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
                    if this.requestType == HttpRequestType.PATCH {
                        args.append('Content-Type: application/json-patch+json');
                    } else {
                        args.append("Content-Type: application/json");    
                    }               
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
        
        /*
         * Writes the payload out to an HTTP/S endpoint in a format specified
         * by the requestFormat instance attribute via the request type 
         * specified in the requestType instance attribute.
         */
        override proc write(payload : string) throws {
            var curl = Curl.easyInit();

            Curl.easySetopt(curl, CURLOPT_URL, this.url);
            
            if this.verbose {
                Curl.easySetopt(curl, CURLOPT_VERBOSE, true);
            }

            if this.ssl {
                configureSsl(curl);
            } 
            
            var args = generateHeader(curl, this.requestFormat);

            Curl.easySetopt(curl, CURLOPT_POSTFIELDS, payload);
            Curl.easySetopt(curl, CURLOPT_CUSTOMREQUEST, this.requestType:string);

            esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                      "Configured HttpChannel for type %s format %s".format(
                      this.requestType, this.requestFormat));

            esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                      "Executing Http request with payload %s".format(payload));

            var ret = Curl.easyPerform(curl);
            
            if ret == 0 {
                esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                    "Successfully executed Http request with payload %s".format(payload));
            } else {
                esLogger.error(getModuleName(),getRoutineName(),getLineNumber(),
                    "Error in executing Http request, return code: %i".format(ret));
                throw getErrorWithContext(getLineNumber(),getRoutineName(),getModuleName(),
                    "Http request with payload %s returned error code %i".format(payload,ret),
                    "ExternalSystemError");
            }

            args.free();
            Curl.easyCleanup(curl);     
            
            esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                       "Closed HttpChannel");      
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
    
    private proc generateEndpointUrl() : string throws {
        var k8sHost = ServerConfig.getEnv('K8S_HOST');
        var namespace = ServerConfig.getEnv('NAMESPACE');
        return '%s/api/v1/namespaces/%s/endpoints'.format(k8sHost,namespace);
    }

    private proc generateExternalServiceUrl() : string throws {
        var k8sHost = ServerConfig.getEnv('K8S_HOST');
        var namespace = ServerConfig.getEnv('NAMESPACE');
        return '%s/api/v1/namespaces/%s/services'.format(k8sHost,namespace);
    }
    
    /*
     * Registers Arkouda with Kubernetes by creating a Kubernetes Service and Endpoint 
     * which together enable service discovery of an Arkouda instance deployed outside
     * of Kubernetes.
     */
    proc registerWithKubernetes() throws {
        var serviceUrl = generateExternalServiceUrl();
        
        var channel = getExternalChannel(new HttpChannelParams(
                                         channelType=ChannelType.HTTP,
                                         url=generateExternalServiceUrl(),
                                         requestType=HttpRequestType.POST,
                                         requestFormat=HttpRequestFormat.JSON,
                                         verbose=false,
                                         ssl=true,
                                         sslKey=ServerConfig.getEnv('KEY_FILE'),
                                         sslCert=ServerConfig.getEnv('CERT_FILE'),
                                         sslCacert=ServerConfig.getEnv('CACERT_FILE'),
                                         sslCapath='',
                                         sslKeyPasswd=ServerConfig.getEnv('KEY_PASSWD')));

        var servicePayload = '{"apiVersion": "v1","kind": "Service","metadata": \
                             {"name": "%s"},"spec": {"ports": [{"port": %i,"protocol": \
                             "TCP","targetPort": %i}]}}'.format(
                                          ServerConfig.getEnv('EXTERNAL_SERVICE_NAME'),
                                          ServerConfig.getEnv('EXTERNAL_SERVICE_PORT'):int,
                                          ServerConfig.getEnv('EXTERNAL_SERVICE_TARGET_PORT'):int);

        esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "Registering service via payload %s and url %s".format(
                                         servicePayload,serviceUrl));

        channel.write(servicePayload);
        
        esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "Registered service via payload %s and url %s".format(
                                         servicePayload,serviceUrl));
                                              
        var endpointUrl = generateEndpointUrl();                                 
        var endpointPayload = '{"kind": "Endpoints","apiVersion": "v1","metadata": {"name": "%s"}, \
                                "subsets": [{"addresses": [{"ip": "%s"}],"ports": [\
                                {"port": %i, "protocol": "TCP"}]}]}'.format(
                                                    ServerConfig.getEnv('ENDPOINT_NAME'),
                                                    ServerConfig.getConnectHostIp(),
                                                    ServerConfig.getEnv('ENDPOINT_PORT'):int);
        
        channel = getExternalChannel(new HttpChannelParams(
                                         channelType=ChannelType.HTTP,
                                         url=generateEndpointUrl(),
                                         requestType=HttpRequestType.POST,
                                         requestFormat=HttpRequestFormat.JSON,
                                         verbose=false,
                                         ssl=true,
                                         sslKey=ServerConfig.getEnv('KEY_FILE'),
                                         sslCert=ServerConfig.getEnv('CERT_FILE'),
                                         sslCacert=ServerConfig.getEnv('CACERT_FILE'),
                                         sslCapath='',
                                         sslKeyPasswd=ServerConfig.getEnv('KEY_PASSWD')));

        esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "Registering endpoint via payload %s and url %s".format(
                                         endpointPayload,endpointUrl));

        channel.write(endpointPayload);
        
        esLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "Registered endpoint via payload %s and endpointUrl %s".format(
                                         endpointPayload,endpointUrl));
    }
}