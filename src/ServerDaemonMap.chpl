module ServerDaemonMap {
    use Map;
    use ServerDaemon;

    proc getDaemons() : [] borrowed ArkoudaServerDaemon throws {
        return [new borrowed BaseServerDaemon():ArkoudaServerDaemon];
    }
    
    private var f = getDaemons;

    var daemonMap = new map(string,  f.type);
    daemonMap.add('ARKOUDA_BASE_DAEMON', getServerDaemons);
    
    proc getArkoudaServerDaemons(daemons : string) {
       return daemonMap.getBorrowed(daemons)();
    }
}