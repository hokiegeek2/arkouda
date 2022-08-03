module ServerDaemonMap {
    use Map;
    use ServerDaemon;

    proc getDaemons() : [] shared ArkoudaServerDaemon throws {
        return [new shared BaseServerDaemon():ArkoudaServerDaemon];
    }
    
    private var f = getDaemons;

    var daemonMap = new map(string,  f.type);
    daemonMap.add('ARKOUDA_BASE_DAEMON', getServerDaemons);
    
    proc getArkoudaServerDaemons(daemons : string) {
       return daemonMap.getBorrowed(daemons)();
    }
}