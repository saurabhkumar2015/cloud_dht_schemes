package clients;

import ceph.CephRoutingTable;
import ceph.EntryPoint;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;
import config.DHTConfig;
import ring.RingRoutingTable;
import schemes.ElasticDHT.RoutingTable;
import socket.MessageSendImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static common.Constants.*;

/**
 * Regular client to launch a reader from file list
 */
public class RegularClient {

    private static IRoutingTable routingTable;
    private static MessageSendImpl messageSender = new MessageSendImpl();
    private static Scanner sc = new Scanner(System.in);


    public static void main(String[] args) throws Exception {

        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        initRegularClient(config);
        FileReader f = new FileReader(args[1]);
        BufferedReader bf = new BufferedReader(f);
        String line = bf.readLine();

        int counter = 1;
        int z= 2;
        while(line != null && line.length() != 0) {
            if(counter %z == 0) {
                System.out.println("Paused. Do you want to continue? Press a number n to write n files::");
                z = sc.nextInt();
            }
            counter++;
            String [] splits = line.split("]");
            if(splits.length > 1 && splits[1].trim().length() > 0) {
                String fileName = splits[1].trim();
                for(int i=1 ; i <= config.replicationFactor;i++) {
                    Integer nodeId = routingTable.getNodeId(fileName, i);
                    if(config.verbose.equalsIgnoreCase("debug")) {
                        System.out.println("Write "+ fileName + " to "+ nodeId + " replicaid: " + i);
                    }
                    Payload payload = new Payload(fileName, i);
                    messageSender.sendMessage(config.nodesMap.get(nodeId), WRITE_FILE, payload);
                }
            }
            line = bf.readLine();
        }
        bf.close();
    }

    private static void initRegularClient(DHTConfig config) throws Exception {
        String scheme = config.scheme;

        switch (scheme) {
            case "RING":
            case "ring":
                routingTable = new RingRoutingTable();
                break;
            case "ELASTIC":
            case "elastic":
                schemes.ElasticDHT.RoutingTable r = new schemes.ElasticDHT.RoutingTable();
                RoutingTable.GetInstance().getRoutingTable();
                routingTable = r;
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
