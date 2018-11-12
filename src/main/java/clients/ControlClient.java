package clients;

import common.Commons;
import common.IRoutingTable;
import common.LoadBalance;
import config.ConfigLoader;
import config.DHTConfig;
import socket.IMessageSend;
import socket.MessageSendImpl;

import java.util.Random;
import java.util.Scanner;

import static common.Constants.*;

public class ControlClient {

    private static Scanner sc = new Scanner(System.in);
    private static IMessageSend messageSender = new MessageSendImpl();

    public static void main(String[] args) throws Exception {

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        boolean exit = true;
        IRoutingTable routingTable = Commons.initRoutingTable(config);
        Random r = new Random(config.seed);

        while(exit) {
            System.out.println("Enter \"A\" to add node in DHT scheme " + config.scheme);
            System.out.println("Enter \"D\" to remove node in DHT scheme " + config.scheme);
            System.out.println("Enter \"L\" to load Balance for a node in DHT scheme " + config.scheme);
            System.out.println("Enter \"X\" to exit");
            String input = sc.next();
            int nodeId = r.nextInt(config.nodeIdEnd - config.nodeIdStart);
            nodeId = nodeId %(config.nodeIdEnd - config.nodeIdStart);

            switch (input.toUpperCase().trim()){
                case "A":
                    System.out.println("Enter comma seperated nodeId to add node in DHT scheme " + config.scheme);
                    input = sc.next();
                    String[] ids = input.split(",");
                    for (String id : ids) {
                        int i = Integer.parseInt(id.trim());
                        messageSender.sendMessage(config.nodesMap.get(nodeId), ADD_NODE, i);
                    }
                    break;
                case "D":
                    System.out.println("Enter comma seperated nodeId to delete node in DHT scheme " + config.scheme);
                    input = sc.next();
                    ids = input.split(",");
                    for (String id : ids) {
                        int i = Integer.parseInt(id.trim());
                        messageSender.sendMessage(config.nodesMap.get(nodeId), DELETE_NODE, i);
                    }
                    break;
                case "L":
                    System.out.println("Enter comma seperated node id and factor. newload = oldload* factor in DHT scheme " + config.scheme);
                    input = sc.next();
                    ids = input.split(",");
                    int node = Integer.parseInt(ids[0].trim());
                    double factor = Double.parseDouble(ids[1].trim());
                    if(config.dhtType.toLowerCase().equalsIgnoreCase("centralized"))
                        messageSender.sendMessage(config.proxyIp, LOAD_BALANCE, new LoadBalance(node, factor));
                    else
                        messageSender.sendMessage(config.nodesMap.get(nodeId), LOAD_BALANCE, new LoadBalance(node, factor));
                    break;
                case "X":
                    exit = false;
            }
        }
    }
}
