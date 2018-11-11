package clients;

import ceph.CephRoutingTable;
import ceph.EntryPoint;
import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;
import ring.RingRoutingTable;
import socket.MessageSendImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static common.Constants.*;

public class RegularClient {


    private static String scheme;
    private static String dhtType;
    private static IRoutingTable routingTable;
    private static MessageSendImpl messageSender = new MessageSendImpl();
    public static Scanner sc = new Scanner(System.in);


    public static void main(String[] args) throws Exception {

        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        initRegularClient(config);
        FileReader f = new FileReader(args[1]);
        BufferedReader bf = new BufferedReader(f);
        String line = bf.readLine();

        int counter = 1;
        int z= 10;
        while(line != null && line.length() != 0) {

            if(counter++ %z == 0) {
                System.out.println("Paused. Do you want to continue. Press a number");
                z = sc.nextInt();
            }
            String [] splits = line.split("]");
            if(splits.length > 1 && splits[1].trim().length() > 0) {
                String fileName = splits[1].trim();
                for(int i=1 ; i <= config.replicationFactor;i++) {
                    Integer nodeId = routingTable.getNodeId(fileName, i);
                    if(config.verbose.equalsIgnoreCase("debug")) {
                        System.out.println("Write "+ fileName + "to "+ nodeId);
                    }
                    System.out.println();
                    Map<String, Object> content = new HashMap<>();
                    content.put(FILE_NAME, fileName);
                    content.put(REPLICA_ID, i);
                    messageSender.sendMessage(config.nodesMap.get(nodeId), WRITE_FILE,content);
                }
            }
            line = bf.readLine();
        }
        bf.close();
    }

    private static void initRegularClient(DHTConfig config) throws Exception {
        scheme = config.scheme;
        dhtType = config.dhtType;

        switch (scheme) {
            case "RING":
            case "ring":
                routingTable = new RingRoutingTable();
                break;
            case "ELASTIC":
            case "elastic":
                break;
            case "CEPH":
            case "ceph":
                EntryPoint entryPoint = new EntryPoint();
                entryPoint.BootStrapCeph();
                routingTable = CephRoutingTable.getInstance();
                break;
            default:
                throw new Exception("Incompatible DHT schema found!");

        }

    }
}
