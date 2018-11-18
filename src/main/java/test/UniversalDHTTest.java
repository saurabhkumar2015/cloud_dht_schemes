package test;

import common.Commons;
import common.IRoutingTable;
import common.LoadBalance;
import common.Payload;
import config.ConfigLoader;
import config.DHTConfig;
import socket.IMessageSend;
import socket.MockMessageSender;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import static common.Constants.*;

public class UniversalDHTTest {

    private static Scanner sc = new Scanner(System.in);
    private static Random r = new Random();

    public static void main(String[] args) throws Exception {

        Commons.messageSender = new MockMessageSender();
        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n" +
                " 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        IRoutingTable routingTable = Commons.initRoutingTable(config);

        IMessageSend messageSender = Commons.messageSender;
        boolean distributed = !"Centralized".equalsIgnoreCase(config.dhtType);

        FileReader f = new FileReader(args[1]);
        BufferedReader bf = new BufferedReader(f);
        String line = bf.readLine();

        boolean exit = true;
        while(exit) {
            System.out.println("Enter \"A\" to add node in DHT scheme ");
            System.out.println("Enter \"D\" to remove node in DHT scheme ");
            System.out.println("Enter \"L\" to load Balance for a node in DHT scheme ");
            System.out.println("Enter \"P\" to print routing table ");
            System.out.println("Enter \"F\" to print write of files ");
            System.out.println("Enter \"X\" to exit");
            String input = sc.next();
            List<Integer> liveNodes = routingTable.getLiveNodes();
            int nodeId = liveNodes.get(r.nextInt(liveNodes.size()));

            switch (input.toUpperCase().trim()){
                case "A":
                    System.out.println("Enter comma seperated nodeId to add node in DHT scheme " + config.scheme);
                    input = sc.next();
                    String[] ids = input.split(",");
                    for (String id : ids) {
                        int i = Integer.parseInt(id.trim());
                        routingTable.addNode(i);
                    }
                    break;
                case "D":
                    System.out.println("Enter comma seperated nodeId to delete node in DHT scheme " + config.scheme);
                    input = sc.next();
                    ids = input.split(",");
                    for (String id : ids) {
                        int i = Integer.parseInt(id.trim());
                        routingTable.deleteNode(i);
                    }
                    break;
                case "L":
                    System.out.println("Enter comma seperated nodeId and factor. newload = oldload* factor in DHT scheme " + config.scheme);
                    input = sc.next();
                    ids = input.split(",");
                    int node = Integer.parseInt(ids[0].trim());
                    double factor = Double.parseDouble(ids[1].trim());
                    routingTable.loadBalance(node, factor);
                    break;
                case "P":
                    routingTable.printRoutingTable();
                    break;
                case "F":
                    System.out.println("::Writing 10 more files in our DHT::");
                    for(int j =0; j<10 ;j++) {
                        String [] splits = line.split("]");
                        if(splits.length > 1 && splits[1].trim().length() > 0) {
                            String fileName = splits[1].trim();
                            for(int i=1 ; i <= config.replicationFactor;i++) {
                                Integer wnodeId = routingTable.getNodeId(fileName, i);
                                if(config.verbose.equalsIgnoreCase("debug")) {
                                    System.out.println("Write "+ fileName + " to "+ wnodeId + " replicaid: " + i);
                                }
                            }
                        }
                        line = bf.readLine();
                    }
                    break;
                case "X":
                    exit = false;
            }

        }
    }
}
