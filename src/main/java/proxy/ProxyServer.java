package proxy;

import java.io.IOException;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import common.*;
import ceph.CephRoutingTable;
import ceph.EntryPoint;
import config.ConfigLoader;
import config.DHTConfig;
import schemes.ElasticDHT.ERoutingTable;
import socket.MessageSendImpl;
import ring.DataNode;
import ring.RingDHTScheme;
import socket.IMessageSend;
import socket.Request;

public class ProxyServer {
	
	private static String scheme;
	private static DHTConfig config;
	private static IRoutingTable routingTable;
	private static IMessageSend sendMsg = new MessageSendImpl();
	
	/* Bootstrapping the DHT table according to scheme */
	
    public static void initProxy(DHTConfig config) throws Exception {
    	
    	 String scheme = config.scheme;
    	 
         switch (scheme.toUpperCase().trim()) {
             case "RING":
                 RingDHTScheme ring = new RingDHTScheme();
                 DataNode dNode = new DataNode(ring);
                 routingTable = dNode.routingTableObj;
                 break;
			 case "ELASTIC":
                 ERoutingTable.giveInstance().giveRoutingTable();
				 Commons.elasticERoutingTable = ERoutingTable.giveInstance();
				 Commons.elasticOldERoutingTable = ERoutingTable.giveInstance();
                 routingTable = Commons.elasticERoutingTable;
                 break;
             case "CEPH":
                 EntryPoint entryPoint = new EntryPoint();
                 entryPoint.BootStrapCeph();
                 routingTable = CephRoutingTable.giveInstance();
                 break;
             default:
                 throw new Exception("Incompatible DHT schema found!");
         }

    
    }
    
    
    public static void sendUpdatedDHT(int nodeId, String type, double factor) throws Exception {
    	
            List<Integer> liveNodes = routingTable.giveLiveNodes();
            
            for(int id: liveNodes) {
            	System.out.println("Live node "+id);
            	
            		  new Thread() {
            		      public void run() {
            		    	  
            		    	    UpdateRoutingPayload payload = new UpdateRoutingPayload(nodeId, type, routingTable, factor);
            	            	sendMsg.sendMessage(config.nodesMap.get(id), Constants.NEW_VERSION, payload);
            		        
            		      }
            		  }.start();
            	
            }
             
    }
    
    public static void main(String[] argv) throws Exception {
		
    	
    	if(argv.length != 1) throw new Exception("Please specify one arguments. \n 1) Config file absolute path \n");
    	
    	/*loading config file*/
        ConfigLoader.init(argv[0]);
        config = ConfigLoader.config;
        initProxy(config);
    	
    	 
		 Socket socket = null; 
	     ServerSocket server  = null; 
	     InputStream in =  null; 
	     
	     try {
	    	 
		 server = new ServerSocket(5000);
	     while(true) {
	    		
	    	
				socket = server.accept();
				ObjectInputStream ins = new ObjectInputStream(socket.getInputStream());
		        Request message = (Request) ins.readObject();
		        
		        
		        	
			    	if((message.getType()).equals(Constants.ADD_NODE)) {
			    		
			    		Integer nodeId = (Integer) message.getPayload();
	                    System.out.println("Add node " + nodeId);
	                    IRoutingTable updated_routing_table =  routingTable.addNode(nodeId);
	                    routingTable = updated_routing_table;
	                    
	                    sendUpdatedDHT(nodeId, Constants.ADD_NODE, 0);
	                       
	                 }
			    	
				    if((message.getType()).equals(Constants.LOAD_BALANCE)) {
				    		
				    		LoadBalance lb = (LoadBalance) message.getPayload();
				    		int nodeToBeBalanced = lb.nodeId;
				    		double loadFactor = lb.loadFactor;
				    		
				    		System.out.println("DataNode to be loadbalanced "+nodeToBeBalanced);
				    		System.out.println("Load Factor "+loadFactor);
				    		
				    		IRoutingTable updated_routing_table =  routingTable.loadBalance(nodeToBeBalanced, loadFactor);
		                    routingTable = updated_routing_table;
		                
			           	    sendUpdatedDHT(nodeToBeBalanced,Constants.LOAD_BALANCE, lb.loadFactor);
		              }
				    
				    if((message.getType()).equals(Constants.DELETE_NODE)) {
			    		
			    		int nodeToBeDeleted = (Integer) message.getPayload();
			    		
			    		System.out.println("DataNode to be deleted "+nodeToBeDeleted);
			    		
			    		IRoutingTable updated_routing_table =  routingTable.deleteNode(nodeToBeDeleted);
	                    routingTable = updated_routing_table;
	                
		           	    sendUpdatedDHT(nodeToBeDeleted, Constants.DELETE_NODE, 0);
	               }
		       }
		      
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

    }

}
