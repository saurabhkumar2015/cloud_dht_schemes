package proxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.SerializationUtils;
import org.json.simple.JSONObject;

import ceph.CephRoutingTable;
import ceph.EntryPoint;
import common.IStrategy;
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
    
	
	/* Bootstrapping the DHT table according to scheme */
	
    public static void initProxy(DHTConfig config) {
    	
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
                ceph_routing_table = CephRoutingTable.getInstance(config.cephMaxClusterSize, -1);
                break;
            default:
                throw new Exception("Incompatible DHT schema found!");

        }

    
    }
    
    
    /* Sending updated DHT to datanodes */
    
    public static void sendUpdatedDhtToDatanodes(DHTConfig config) {
    	
    	IMessageSend sendMsg = new MessageSendImpl();
    	
    	Map<Integer,String> dataNodes = config.nodesMap;
    	
    	if(scheme.equals("CEPH") || scheme.equals("ceph")) {
    		
    		 for (Entry<Integer, String> entry : dataNodes.entrySet())  {
    			 sendMsg.sendMessage(entry.getValue(), "DHT_Update", ceph_routing_table);
    		 }
    	          
    	}
    	
    	else if(scheme.equals("RING") || scheme.equals("ring")) {
    		for (Entry<Integer, String> entry : dataNodes.entrySet())  {
   			 sendMsg.sendMessage(entry.getValue(), "DHT_Update", ring_routing_table);
   		 	}
    	}
    	
    	else{
    		for (Entry<Integer, String> entry : dataNodes.entrySet())  {
   			 sendMsg.sendMessage(entry.getValue(), "DHT_Update", elastic_routing_table);
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
				server = new ServerSocket(5000);
				socket = server.accept(); 
		         // takes input from the client socket 
		        in = socket.getInputStream(); 
		        byte[] bytes = IOUtils.readFully(in, -1, true);
		        
		    	Request message = SerializationUtils.deserialize(bytes);
		    	
		    	if(message.getType() == "DHT_Update") {
		    		
		    		if(scheme.equals("CEPH") || scheme.equals("ceph")) {
		    			
		    			CephRoutingTable updated_ceph_routing_table = (CephRoutingTable)message.getPayload();
		    			
		    			if(ceph_routing_table.VersionNo < updated_ceph_routing_table.VersionNo) {
		    				ceph_routing_table = updated_ceph_routing_table;
		    				sendUpdatedDhtToDatanodes(config);
		    			}
		    			
		    		}
		    		
		    		else if(scheme.equals("RING") || scheme.equals("ring")) {
		    			
		    			RingRoutingTable updated_ring_routing_table = (RingRoutingTable)message.getPayload();
		    			
		    			if(ring_routing_table.version < updated_ring_routing_table.version) {
		    				ring_routing_table = updated_ring_routing_table;
		    				sendUpdatedDhtToDatanodes(config);
		    			}
		    			
		    		}
		    		
		    		else {
		    			
		    			ElasticRoutingTable updated_elastic_routing_table = (ElasticRoutingTable)message.getPayload();
		    			
		    			if(elastic_routing_table.version < updated_elastic_routing_table.version) {
		    				elastic_routing_table = updated_elastic_routing_table;
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
