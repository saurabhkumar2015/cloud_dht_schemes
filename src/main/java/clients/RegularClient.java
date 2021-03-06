package clients;

import common.Commons;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;
import config.DHTConfig;
import ring.RingRoutingTable;
import schemes.ElasticDHT.ERoutingTable;
import socket.MessageSendImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

import ceph.CephRoutingTable;

import static common.Constants.*;

/**
 * Regular client to launch a reader from file list
 */
public class RegularClient {

    public static IRoutingTable routingTable;
    private static MessageSendImpl messageSender = new MessageSendImpl();
    private static Scanner sc = new Scanner(System.in);

    public static void main(String[] args) throws Exception {

        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path " +
                "\n 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        routingTable = Commons.initRoutingTable(config);
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
                int replicaId = 1;
                for(int i=1 ; i <= config.replicationFactor;i++) {
                    Integer nodeId = routingTable.giveNodeId(fileName, replicaId);
                    while(nodeId <0) nodeId = routingTable.giveNodeId(fileName, ++replicaId);
                    if(config.verbose.equalsIgnoreCase("debug")) {
                        System.out.println("Write "+ fileName + " to node "+ nodeId + " replicaid: " + replicaId);
                    }
                    Payload payload;
                    switch (config.scheme.toUpperCase().trim()) {
                    case "RING":
                        payload = new Payload(fileName, replicaId, ((RingRoutingTable)routingTable).versionNumber);
                        messageSender.sendMessage(config.nodesMap.get(nodeId), WRITE_FILE, payload);
                        break;
                    case "ELASTIC":
                        payload = new Payload(fileName, replicaId, ((ERoutingTable)routingTable).versionNumber);
                        messageSender.sendMessage(config.nodesMap.get(nodeId), WRITE_FILE, payload);
                        break;
                    case "CEPH":
                        payload = new Payload(fileName, replicaId, ((CephRoutingTable)routingTable).getVersionNumber());
                        messageSender.sendMessage(config.nodesMap.get(nodeId), WRITE_FILE, payload);
                        break;
                    default:
                        throw new Exception("Incompatible DHT schema found!");
                }
                replicaId++;
              }
            }
            line = bf.readLine();
        }
        bf.close();
    }
}
