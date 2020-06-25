/* arkouda server
backend chapel program to mimic ndarray from numpy
This is the main driver for the arkouda server */

use Security;
use ServerConfig;
use Time only;
use ZMQ only;
use Memory;

use MultiTypeSymbolTable;
use MultiTypeSymEntry;
use MsgProcessing;
use GenSymIO;
use SymArrayDmap;
use ServerErrorStrings;

use Chapel120; // the compatibility layer


proc main() {
    writeln("arkouda server version = ",arkoudaVersion); try! stdout.flush();
    writeln("memory tracking = ", memTrack); try! stdout.flush();
    if (memTrack) {
        writeln("getMemLimit() = ",getMemLimit());
        writeln("bytes of memoryUsed() = ",memoryUsed());
        try! stdout.flush();
    }

    var st = new owned SymTab();
    var shutdownServer = false;
    var serverToken = '';
    var serverMessage = '';

    // create and connect ZMQ socket
    var context: ZMQ.Context;
    var socket : ZMQ.Socket = context.socket(ZMQ.REP);

    // configure token authentication and server startup message accordingly
    if authenticate {
        serverToken = generateToken(32);
        serverMessage = "server listening on %s:%t with token %s".format(serverHostname, 
                                        ServerPort, serverToken);
    } else {
        serverMessage = "server listening on %s:%t".format(serverHostname, ServerPort);
    }

    socket.bind("tcp://*:%t".format(ServerPort));

    writeln(serverMessage); try! stdout.flush();
    
    createServerConnectionInfo();

    var reqCount: int = 0;
    var repCount: int = 0;

    var t1 = new Time.Timer();
    t1.clear();
    t1.start();

    proc sendRepMsg(repMsg: ?t) where t==string || t==bytes {
        repCount += 1;
        if logging {
          if t==bytes {
              writeln("repMsg:"," <binary-data>");
          } else {
            writeln("repMsg:",repMsg);
          }
          try! stdout.flush();
        }
        socket.send(repMsg);
    }

    proc authenticateUser(token : string) throws {
        if token == 'None' || token.isEmpty() {
            throw new owned ErrorWithMsg("Error: access to arkouda requires token");
        }
        else if serverToken != token {
            throw new owned ErrorWithMsg("Error: token %s does not match server token, check with server owner".format(token));
        }
    } 

    proc containsBinaryData(cmdRaw : bytes) : bool {
        return cmdRaw.endsWith(b":array");
    }
    
    proc getCommandStrings(rawCmdString : string) : (string,string,string) {
        var strings = rawCmdString.split(':');
        return (strings[0],strings[1],strings[2]);
    }

    proc shutdown(timeElapsed) {
        shutdownServer = true;
        repCount += 1;
        socket.send("shutdown server (%i req)".format(repCount));
    }
    
    while !shutdownServer {
        // receive requests

        var reqMsgRaw = socket.recv(bytes);

        reqCount += 1;

        var s0 = t1.elapsed();

        const (cmdRaw, payload) = reqMsgRaw.splitMsgToTuple(2);

        // parse requests, execute requests, format responses
        try {
            // first handle the case where we received arbitrary data
            if containsBinaryData(cmdRaw) {
                /*
                Decode the first tuple element which is a string array of bytes
                composed of the username, token, and Arkouda command. Note: if 
                the authenticate flag is false, the token is a None string.
                */
                var cmdStr : string;
                try! {
                   cmdStr = cmdRaw.decode();
                } catch e: DecodeError {
                    if v {
                        writeln("Error: illegal byte sequence in command: ",
                                cmdRaw.decode(decodePolicy.replace));
                        try! stdout.flush();
                    }
                    sendRepMsg(unknownError(""));
                }
                
                const (user,token,cmd) = getCommandStrings(cmdStr);

                if authenticate {
                    authenticateUser(token);
                }

                reqMsgRaw = b' '.join(cmd.encode(),payload); 
                if logging {
                    writeln("reqMsg: ", b"array", " <binary-data>");
                    writeln(">>> %s started at %.17r sec".format("array", s0));
                    try! stdout.flush();
                }
                sendRepMsg(arrayMsg(reqMsgRaw, st));
            }
            else {
                /*
                The received message does not have binary data, so decode entire 
                the entire message and split the decoded string
                */
                var reqMsg: string;
                try! {
                    reqMsg = reqMsgRaw.decode();
                }
                catch e: DecodeError {
                    if v {
                        writeln("Error: illegal byte sequence in command: ",
                                reqMsgRaw.decode(decodePolicy.replace));
                        try! stdout.flush();
                    }
                    sendRepMsg(unknownError(""));
                }
                /*
                Decode the first array element which is a string array of bytes
                composed of the username, token, and Arkouda command. Note: if
                the authenticate flag is false, the token is a None string.
                */
                var messageTokens = reqMsg.split(' ');
                const (user,token,cmd) = getCommandStrings(messageTokens[0]);

                if authenticate {
                    authenticateUser(token);
                }

                reqMsg = ' '.join(messageTokens[1..messageTokens.size-1]);
                reqMsg = ' '.join(cmd, reqMsg);

                if logging {
                    writeln("reqMsg: ", reqMsg);
                    writeln(">>> %s started at %.17r sec".format(cmd, s0));
                    try! stdout.flush();
                }

                // now take care of the case where we send arbitrary data:
                if cmd == "tondarray" {
                    sendRepMsg(tondarrayMsg(reqMsg, st));
                }
                else {
                    // here we know that everything is strings
                    var repMsg: string;

                    if cmd == "shutdown" {
                        shutdown(s0);
                        break;
                    }

                    select cmd
                    {
                        when "segmentLengths"    {repMsg = segmentLengthsMsg(reqMsg, st);}
                        when "segmentedHash"     {repMsg = segmentedHashMsg(reqMsg, st);}
                        when "segmentedEfunc"    {repMsg = segmentedEfuncMsg(reqMsg, st);}
                        when "segmentedIndex"    {repMsg = segmentedIndexMsg(reqMsg, st);}
                        when "segmentedBinopvv"  {repMsg = segBinopvvMsg(reqMsg, st);}
                        when "segmentedBinopvs"  {repMsg = segBinopvsMsg(reqMsg, st);}
                        when "segmentedGroup"    {repMsg = segGroupMsg(reqMsg, st);}
                        when "segmentedIn1d"     {repMsg = segIn1dMsg(reqMsg, st);}
                        when "lshdf"             {repMsg = lshdfMsg(reqMsg, st);}
                        when "readhdf"           {repMsg = readhdfMsg(reqMsg, st);}
                        when "readAllHdf"        {repMsg = readAllHdfMsg(reqMsg, st);}
                        when "tohdf"             {repMsg = tohdfMsg(reqMsg, st);}
                        when "create"            {repMsg = createMsg(reqMsg, st);}
                        when "delete"            {repMsg = deleteMsg(reqMsg, st);}
                        when "binopvv"           {repMsg = binopvvMsg(reqMsg, st);}
                        when "binopvs"           {repMsg = binopvsMsg(reqMsg, st);}
                        when "binopsv"           {repMsg = binopsvMsg(reqMsg, st);}
                        when "opeqvv"            {repMsg = opeqvvMsg(reqMsg, st);}
                        when "opeqvs"            {repMsg = opeqvsMsg(reqMsg, st);}
                        when "efunc"             {repMsg = efuncMsg(reqMsg, st);}
                        when "efunc3vv"          {repMsg = efunc3vvMsg(reqMsg, st);}
                        when "efunc3vs"          {repMsg = efunc3vsMsg(reqMsg, st);}
                        when "efunc3sv"          {repMsg = efunc3svMsg(reqMsg, st);}
                        when "efunc3ss"          {repMsg = efunc3ssMsg(reqMsg, st);}
                        when "reduction"         {repMsg = reductionMsg(reqMsg, st);}
                        when "countReduction"    {repMsg = countReductionMsg(reqMsg, st);}
                        when "countLocalRdx"     {repMsg = countLocalRdxMsg(reqMsg, st);}
                        when "findSegments"      {repMsg = findSegmentsMsg(reqMsg, st);}
                        when "findLocalSegments" {repMsg = findLocalSegmentsMsg(reqMsg, st);}
                        when "segmentedReduction"{repMsg = segmentedReductionMsg(reqMsg, st);}
                        when "segmentedLocalRdx" {repMsg = segmentedLocalRdxMsg(reqMsg, st);}
                        when "arange"            {repMsg = arangeMsg(reqMsg, st);}
                        when "linspace"          {repMsg = linspaceMsg(reqMsg, st);}
                        when "randint"           {repMsg = randintMsg(reqMsg, st);}
                        when "randomNormal"      {repMsg = randomNormalMsg(reqMsg, st);}
                        when "randomStrings"     {repMsg = randomStringsMsg(reqMsg, st);}
                        when "histogram"         {repMsg = histogramMsg(reqMsg, st);}
                        when "in1d"              {repMsg = in1dMsg(reqMsg, st);}
                        when "unique"            {repMsg = uniqueMsg(reqMsg, st);}
                        when "value_counts"      {repMsg = value_countsMsg(reqMsg, st);}
                        when "set"               {repMsg = setMsg(reqMsg, st);}
                        when "info"              {repMsg = infoMsg(reqMsg, st);}
                        when "str"               {repMsg = strMsg(reqMsg, st);}
                        when "repr"              {repMsg = reprMsg(reqMsg, st);}
                        when "[int]"             {repMsg = intIndexMsg(reqMsg, st);}
                        when "[slice]"           {repMsg = sliceIndexMsg(reqMsg, st);}
                        when "[pdarray]"         {repMsg = pdarrayIndexMsg(reqMsg, st);}
                        when "[int]=val"         {repMsg = setIntIndexToValueMsg(reqMsg, st);}
                        when "[pdarray]=val"     {repMsg = setPdarrayIndexToValueMsg(reqMsg, st);}
                        when "[pdarray]=pdarray" {repMsg = setPdarrayIndexToPdarrayMsg(reqMsg, st);}
                        when "[slice]=val"       {repMsg = setSliceIndexToValueMsg(reqMsg, st);}
                        when "[slice]=pdarray"   {repMsg = setSliceIndexToPdarrayMsg(reqMsg, st);}
                        when "argsort"           {repMsg = argsortMsg(reqMsg, st);}
                        when "coargsort"         {repMsg = coargsortMsg(reqMsg, st);}
                        when "concatenate"       {repMsg = concatenateMsg(reqMsg, st);}
                        when "localArgsort"      {repMsg = localArgsortMsg(reqMsg, st);}
                        when "sort"              {repMsg = sortMsg(reqMsg, st);}
                        when "joinEqWithDT"      {repMsg = joinEqWithDTMsg(reqMsg, st);}
                        when "getconfig"         {repMsg = getconfigMsg(reqMsg, st);}
                        when "getmemused"        {repMsg = getmemusedMsg(reqMsg, st);}
                        when "register"          {repMsg = registerMsg(reqMsg, st);}
                        when "attach"            {repMsg = attachMsg(reqMsg, st);}
                        when "unregister"        {repMsg = unregisterMsg(reqMsg, st);}
                        when "connect" {
                            if authenticate {
                                repMsg = "connected to arkouda server tcp://*:%t as user %s with token %s".format(ServerPort,user,token);
                            } else {
                                repMsg = "connected to arkouda server tcp://*:%t".format(ServerPort);
                            }
                        }
                        when "disconnect" {
                            repMsg = "disconnected from arkouda server tcp://*:%t".format(ServerPort);
                        }
                        when "noop" {
                            repMsg = "noop";
                            if v { writeln("no-op"); try! stdout.flush(); }
                        }
                        when "ruok" {
                            repMsg = "imok";
                        }
                        otherwise {
                            repMsg = "Error: unrecognized command: %s".format(reqMsg);
                        }

                    }
                    sendRepMsg(repMsg);
                }
            }
        } catch (e: ErrorWithMsg) {
            sendRepMsg(e.msg);
        } catch {
            sendRepMsg(unknownError(""));
        }
        
        // We must have sent a message back by now

        if (logging && memTrack) {writeln("bytes of memory used after command = ",memoryUsed():uint * numLocales:uint); try! stdout.flush();}

        // end timer for command processing
        if (logging) {writeln("<<< %s took %.17r sec".format(cmdRaw.decode(decodePolicy.replace), t1.elapsed() - s0)); try! stdout.flush();}
    }
    t1.stop();
    deleteServerConnectionInfo();

    writeln("requests = ",reqCount," responseCount = ",repCount," elapsed sec = ",t1.elapsed());
}

proc createServerConnectionInfo() {
    use IO;
    if !serverConnectionInfo.isEmpty() {
        try! {
            var w = open(serverConnectionInfo, iomode.cw).writer();
            w.writef("%s %t\n", serverHostname, ServerPort);
        }
    }
}

proc deleteServerConnectionInfo() {
    use FileSystem;
    if !serverConnectionInfo.isEmpty() {
        try! {
            remove(serverConnectionInfo);
        }
    }
}
