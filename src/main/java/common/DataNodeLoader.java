package common;

import config.ConfigLoader;

import java.net.ServerSocket;
import java.net.Socket;

public class DataNodeLoader {

    private static IRoutingTable routingTable;
    private static IDataNode dataNode;

    public static void main(String[] args) throws Exception {

        validate(args);
        ConfigLoader.init(args[0]);
        String nodeInfo = ConfigLoader.config.nodesMap.get(Integer.parseInt(args[1]));
        String[] ipPort = nodeInfo.split(":");
        String ip = ipPort[0];
        int port = Integer.parseInt(ipPort[1]);
        routingTable = Commons.loadScheme(ConfigLoader.config);
        ServerSocket server = new ServerSocket(port);
        while(true) {
            Socket clientSocket = server.accept();
            Thread th = new ClientWorker(clientSocket, dataNode);
            th.start();
        }
    }

    private static  void validate(String[] args) throws Exception {

        if(args.length !=2 )
            throw new Exception("Please provide two arguments. \n 1) absolute path of config file. 2)Node Id ");

    }
}

