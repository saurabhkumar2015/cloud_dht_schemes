package proxy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class ProxyWorker{
	
	private static String scheme;
	private static DHTConfig config;
	private static IRoutingTable routingTable;
	private static Socket client;
	DataOutputStream out;
	ObjectInputStream in;
	public static IMessageSend sendMsg = new MessageSendImpl();
	
	public ProxyWorker(IRoutingTable routingTable,Socket client,DHTConfig config, DataOutputStream out, ObjectInputStream in) {
    	this.client = client;
    	this.out = out;
    	this.in = in;
        this.routingTable = routingTable;
        this.config = config;
    }
    
    
    public static void sendUpdatedDHT(int nodeId, String type, double factor) throws Exception {
    	
            List<Integer> liveNodes = routingTable.giveLiveNodes();
            
            LogObject obj = new LogObject(0, liveNodes.size(),System.nanoTime(), 0);
            ProxyServer.map.put(routingTable.getVersionNumber(),obj);
            List<Thread> thrd = new ArrayList<Thread>();
            for(int id: liveNodes) {
            	System.out.println("Live node "+id);
            	
            		Thread t = new Thread() {
            		      public void run() {
            		    	    UpdateRoutingPayload payload = new UpdateRoutingPayload(nodeId, type, routingTable, factor);
            		    	    System.out.println(config.nodesMap.get(id));
            	            	sendMsg.sendMessage(config.nodesMap.get(id), Constants.NEW_VERSION, payload);
            	            	
            		      }
            		  };
            		  thrd.add(t);
            		  t.start();
            		  
            		  
            }
           
            for(int i=0;i<thrd.size();i++)
            	thrd.get(i).join();
             
    }
    
    public void run() {
	     
	     try {
	    	 
	    	 byte[] stream = null;
	    	 
	    	 

	            //ObjectInputStream in = new ObjectInputStream(client.getInputStream());
	            Request message = (Request) in.readObject();
		        	
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
		       
		} catch (Exception e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

    }

}
