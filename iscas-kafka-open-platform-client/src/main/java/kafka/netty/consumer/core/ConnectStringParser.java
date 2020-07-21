package kafka.netty.consumer.core;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public final class ConnectStringParser {
    private static final int DEFAUL_PORT = 8960;
    private final ArrayList<InetSocketAddress> serverAddress = new ArrayList<>();

    public ConnectStringParser(String connectString){
        if(connectString == null){
            serverAddress.add(InetSocketAddress.createUnresolved("127.0.0.1",DEFAUL_PORT));
        } else {
            String hostsList[] = connectString.split(",",-1);
            for (String host : hostsList){
                int port = DEFAUL_PORT;
                int pidx = host.lastIndexOf(":");
                if(pidx >= 0){
                    if(pidx < host.length() -1){
                        port = Integer.parseInt(host.substring(pidx +1));
                    }
                    host = host.substring(0,pidx);
                }
                serverAddress.add(InetSocketAddress.createUnresolved(host,port));
            }
        }
    }
    public ArrayList<InetSocketAddress> getServerAddress(){
        return serverAddress;
    }
}
