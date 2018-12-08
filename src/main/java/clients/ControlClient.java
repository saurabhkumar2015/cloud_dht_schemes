package clients;

import common.*;
import config.ConfigLoader;
import config.DHTConfig;
import socket.IMessageSend;
import socket.MessageSendImpl;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import static common.Constants.*;

public class ControlClient {

    private static Scanner sc = new Scanner(System.in);
    private static IMessageSend messageSender = new MessageSendImpl();
    public static IRoutingTable routingTable;

    public static void main(String[] args) throws Exception {

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        //boolean exit = true;
        boolean distributed = !"Centralized".equalsIgnoreCase(config.dhtType);
        routingTable = Commons.initRoutingTable(config);
        Random r = new Random(config.seed);
        String commmandsFile = config.commmandsFileLocations;
        
        List<Integer> liveNodes = routingTable.giveLiveNodes();
        int nodeId = getRandomNode(r, liveNodes);
        if(distributed){
            Map<Integer, List<Integer>> gossipList = ConfigLoader.config.gossipList;
            for(int k =1 ; k <= gossipList.size();k++){
                List<Integer> integers = gossipList.get(k);
                boolean alive = false;
                for(int l =0; l < integers.size();l++) {
                    Integer integer = integers.get(r.nextInt(integers.size()));
                    if(liveNodes.contains((Integer)integer)) {
                        nodeId = integer;
                        alive = true;
                        break;
                    }
                }
                if(alive) break;
            }
        }
        
        try (BufferedReader br = new BufferedReader(new FileReader(commmandsFile))) {
            String command;
            while ((command = br.readLine()) != null) {
            	//System.out.println("line:"+command);
            	String[] ids = command.split(",");
            	String keyWord = ids[0].toUpperCase().trim();
            	if(keyWord.equals("A")){
            		for (int index =1; index < ids.length; index++) {
            			String id = ids[index];
                        int i = Integer.parseInt(id.trim());
                        if(distributed) {
                            DistributedPayload p = new DistributedPayload();
                            p.nodeId = i;
                            p.version = routingTable.getVersionNumber();
                            System.out.println("Which datanode should i send this request to:");
                            try{
                                nodeId = sc.nextInt();
                            }
                            catch (Exception e){
                            }
                            liveNodes = routingTable.giveLiveNodes();
                            if(!liveNodes.contains(nodeId)) {
                                System.out.println("ERROR:: Node id "+nodeId + "is not a live node.");
                                continue;
                            }
                            String node = config.nodesMap.get(nodeId);
                            System.out.print("Add Node "+ id +" sent to DataNode:"+nodeId+ "\n");
                            messageSender.sendMessage(node , ADD_NODE, p);
                        }
                        else{
                            System.out.print("Add Node "+ id +" sent to Proxy:"+config.proxyIp);
                            messageSender.sendMessage(config.proxyIp, ADD_NODE, i);
                        }
                    }
            	}
            	else if(keyWord.equals("D")){
            		for (int index =1; index < ids.length; index++) {
            			String id = ids[index];
                        int i = Integer.parseInt(id.trim());
                        while( i == nodeId){
                            nodeId = getRandomNode(r, liveNodes);
                        }
                        if(distributed) {
                            DistributedPayload p = new DistributedPayload();
                            p.nodeId = i;
                            p.version = routingTable.getVersionNumber();
                            System.out.println("Which datanode should i send this request to:");
                            try{
                                nodeId = sc.nextInt();
                            }
                            catch (Exception e){
                            }
                            liveNodes = routingTable.giveLiveNodes();
                            if(!liveNodes.contains(nodeId)) {
                                System.out.println("ERROR:: Node id "+nodeId + "is not a live node.");
                                continue;
                            }
                            System.out.print("Delete Node "+ id +" sent to DataNode:"+nodeId + "\n");
                            messageSender.sendMessage(config.nodesMap.get(nodeId), DELETE_NODE, p);
                        }
                        else {
                            System.out.print("Delete Node "+ id +" sent to Proxy:"+config.proxyIp);
                            messageSender.sendMessage(config.proxyIp, DELETE_NODE, i);
                        }
                    }
            	}
            	else if (keyWord.equals("L")){
            		int node = Integer.parseInt(ids[1].trim());
                    double factor = Double.parseDouble(ids[2].trim());
                    if(distributed) {
                        DistributedPayload p = new DistributedPayload();
                        p.nodeId = node;
                        p.version = routingTable.getVersionNumber();
                        p.loadFactor = factor;
                        System.out.println("Which datanode should i send this request to:");
                        try{
                            nodeId = sc.nextInt();
                        }
                        catch (Exception e){
                        }
                        liveNodes = routingTable.giveLiveNodes();
                        if(!liveNodes.contains(nodeId)) {
                            System.out.println("ERROR:: Node id "+nodeId + "is not a live node.");
                            continue;
                        }
                        String sNode = config.nodesMap.get(nodeId);
                        System.out.print("Load Balance request for node id " + node+" sent to DataNode:"+nodeId+ "\n");
                        messageSender.sendMessage(sNode, LOAD_BALANCE, p);
                    }
                    else {
                        System.out.print("Load Balance request for node id " + node+" sent to Proxy:"+config.proxyIp);
                        messageSender.sendMessage(config.proxyIp, LOAD_BALANCE, new LoadBalance(node, factor));
                    }
            	}
            	else if (keyWord.equals("P")){
            		Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(Integer.parseInt(ids[1])), Constants.PRINT_REQUEST, null);
            	}
            }
        }
    }

    private static Integer getRandomNode(Random r, List<Integer> liveNodes) {
        return liveNodes.get(r.nextInt(liveNodes.size()));
    }
}

