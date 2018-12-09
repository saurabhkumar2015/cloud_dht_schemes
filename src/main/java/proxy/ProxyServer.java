package proxy;

import java.io.DataOutputStream;
import java.io.IOException;

import java.io.InputStream;
import java.io.ObjectInputStream;
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

public class ProxyServer {
	
	private static String scheme;
	private static DHTConfig config;
	private static IRoutingTable routingTable;
	public static Map<Long,LogObject> map = new HashMap<Long,LogObject>();
	
	
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
   
    
    @SuppressWarnings("resource")
	public static void main(String[] argv) throws Exception {
		
    	
    	if(argv.length != 1) throw new Exception("Please specify one arguments. \n 1) Config file absolute path \n");
    	
    	/*loading config file*/
        ConfigLoader.init(argv[0]);
        config = ConfigLoader.config;
        initProxy(config);
    	
    	ServerSocket server  = null; 
	    Socket socket=null;
	    server = new ServerSocket(5000);
	    
	    while(true) {
	        	try {
		            socket = server.accept();
		            ProxyWorker w = new ProxyWorker( routingTable,socket,config,new DataOutputStream(socket.getOutputStream()),new ObjectInputStream(socket.getInputStream()));
		            w.run();
	        	}catch(IOException e) {
	        		socket.close();
	        	}
	        }
	     

    }

}
