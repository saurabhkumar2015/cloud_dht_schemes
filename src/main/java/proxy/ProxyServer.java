package proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Map.Entry;

import common.CephPayload;
import common.Constants;
import common.IRoutingTable;

import org.apache.commons.lang3.SerializationUtils;

import ceph.CephRoutingTable;
import ceph.EntryPoint;
import ceph.Node;
import config.ConfigLoader;
import config.DHTConfig;
import socket.MessageSendImpl;
import ring.RingRoutingTable;
import schemes.ElasticDHT.ElasticRoutingTable;
import socket.IMessageSend;
import socket.Request;
import sun.misc.IOUtils;

public class ProxyServer {
	
	private static String scheme;
	private static CephRoutingTable ceph_routing_table;
	private static RingRoutingTable ring_routing_table;
	private static ElasticRoutingTable elastic_routing_table;
	private static IMessageSend sendMsg = new MessageSendImpl();
	
	/* Bootstrapping the DHT table according to scheme */
	
    public static void initProxy(DHTConfig config) throws Exception {
    	
    	//get initial Instances of routing tables or osdMap 
    	scheme = config.scheme;

        switch (scheme) {
            case "RING":
            case "ring":
                break;
            case "ELASTIC":
            case "elastic":
                break;
            case "CEPH":
            case "ceph":
                EntryPoint entryPoint = new EntryPoint();
                entryPoint.BootStrapCeph();
                ceph_routing_table = CephRoutingTable.getInstance();
                break;
            default:
                throw new Exception("Incompatible DHT schema found!");

        }

    
    }
    
    
    /* Sending updated DHT to datanodes */
    
    public static void sendUpdatedDhtToDatanodes(DHTConfig config) {
    	
    	Map<Integer,String> dataNodes = config.nodesMap;
    	
    	if(scheme.equals("CEPH") || scheme.equals("ceph")) {
    		
    		 for (Entry<Integer, String> entry : dataNodes.entrySet())  {
    			 sendMsg.sendMessage(entry.getValue(), Constants.NEW_VERSION, ceph_routing_table);
    		 }
    	          
    	}
    	
    	else if(scheme.equals("RING") || scheme.equals("ring")) {
    		for (Entry<Integer, String> entry : dataNodes.entrySet())  {
   			 sendMsg.sendMessage(entry.getValue(),Constants.NEW_VERSION, ring_routing_table);
   		 	}
    	}
    	
    	else{
    		for (Entry<Integer, String> entry : dataNodes.entrySet())  {
   			 sendMsg.sendMessage(entry.getValue(), Constants.NEW_VERSION, elastic_routing_table);
   		 	}
    	}
    	
    }
    
    
    public static void main(String[] argv) throws Exception {
		
    	
    	if(argv.length != 1) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n");
    	
    	/*loading config file*/
        ConfigLoader.init(argv[0]);
        DHTConfig config = ConfigLoader.config;
        initProxy(config);
    	
    	 
		 Socket socket = null; 
	     ServerSocket server  = null; 
	     InputStream in =  null; 
	     
	     try {
	    	 
	    	while(true) {
	    		
	    		// listening on port 5000
	            // takes input from the client socket 
			   /*  in = socket.getInputStream(); 
			    byte[] bytes = IOUtils.readFully(in, -1, true);
			        
			     Request message = SerializationUtils.deserialize(bytes);*/
	    		
				server = new ServerSocket(5000);
				socket = server.accept(); 
		 
		    	
		    	ObjectInputStream ins = new ObjectInputStream(socket.getInputStream());
		        Request message = (Request) ins.readObject();

		    	if((message.getType()).equals(Constants.ADD_NODE)) {
		    		
		    		Integer nodeId = (Integer) message.getPayload();
                    System.out.println("Add node " + nodeId);
                    CephRoutingTable updated_ceph_routing_table = (CephRoutingTable)ceph_routing_table.addNode(nodeId);
                    ceph_routing_table = updated_ceph_routing_table;
                    
                    Node headNode = ceph_routing_table.mapInstance.FindNodeInOsdMap(nodeId);
                    
	                   Node temp = headNode;
	                   Node temp1 = headNode.nextNode;
	             	   double sum = 0;
	             	   while(temp != null)
	             	   {
	             		   sum = sum + temp.weight;
	             		   temp = temp.nextNode;
	             	   }
	             	   
	             	  String newNodeStr = config.nodesMap.get(headNode.nodeId);
            		  int newNodeClusterId = headNode.clusterId;
            		  double newNodeWt = headNode.weight;
            		  
	             	   while(temp1 != null) {
	             		   
	             		   double weight = temp1.weight;
	             		   String nodeIp = config.nodesMap.get(temp1.nodeId);
	             		   CephPayload payload = new CephPayload(newNodeStr, newNodeClusterId, newNodeWt, sum);
	             		   sendMsg.sendMessage(nodeIp, Constants.MOVE_FILE, payload);
	             		   sum = sum - weight;
	             	   }
                       
                 
		    		
		    	}
		    	
		    	if(message.getType() == Constants.NEW_VERSION) {
		    		
		    		if(scheme.equals("CEPH") || scheme.equals("ceph")) {
		    			
		    			CephRoutingTable updated_ceph_routing_table = (CephRoutingTable)message.getPayload();
		    			
		    			if(ceph_routing_table.VersionNo < updated_ceph_routing_table.VersionNo) {
		    				ceph_routing_table = updated_ceph_routing_table;
		    				sendUpdatedDhtToDatanodes(config);
		    			}
		    			
		    		}
		   
		    		
		    			//compare epoch number & if greater...update dht and send messages to datanodes
		    	
		    	}
	    	}
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

    }

}
