package clients;

import common.Commons;
import common.Constants;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;
import config.DHTConfig;
import ring.RingRoutingTable;
import schemes.ElasticDHT.ERoutingTable;
import socket.MessageSendImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import ceph.CephRoutingTable;
import ceph.HashGenerator;

import static common.Constants.*;

/**
 * Regular client to launch a reader from file list
 */
public class RegularClientDemo {

    public static IRoutingTable routingTable;
    private static MessageSendImpl messageSender = new MessageSendImpl();
    private static Scanner sc = new Scanner(System.in);
    private static Map<Integer,List<String>> map = new HashMap<Integer,List<String>>();

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
        	
        	 String [] splits = line.split("]");
             if(splits.length > 1 && splits[1].trim().length() > 0) {
                 String fileName = splits[1].trim();
                 int pg = HashGenerator.giveInstance().givePlacementGroupIdFromFileName(fileName, ConfigLoader.config.PlacementGroupMaxLimit);
                 if(map.containsKey(pg)) {
                	 List<String> list  = map.get(pg);
                	 list.add(fileName);
                	 map.put(pg, list);
                 }
                 else {
                	 List<String> list = new ArrayList<String>();
                	 list.add(fileName);
                	 map.put(pg, list);
                 }
             }
             line = bf.readLine();
        }
          
       bf.close();
       
   
       while(true) {
    	   System.out.println("Enter \"W\" to write file");
           System.out.println("Enter \"P\" to print routing table");
           
           String in = sc.next();
           
           switch (in.toUpperCase().trim()){
           		
           case "W":
        	   System.out.println("Enter comma seperated placement group and replica id and no.of files");
		       String input = sc.next();
		       String[] ids = input.split(",");
		       int replicaId = Integer.parseInt(ids[1]);
		       List<String> l = map.get(Integer.parseInt(ids[0]));
		       int len = Integer.parseInt(ids[2]);
		       for(int i=0;i<len;i++) {
		    	   Integer nodeId = routingTable.giveNodeId(l.get(i), replicaId);
		    	   Payload payload = new Payload(l.get(i), replicaId, routingTable.getVersionNumber());
		    	   Commons.messageSender.sendMessage(config.nodesMap.get(nodeId), Constants.WRITE_FILE, payload);
		       }
        	   break;
        	   
           case "P":
        	   routingTable.printRoutingTable();
        	   break;
           
           }
		      
       }
       
    }
  }  
    