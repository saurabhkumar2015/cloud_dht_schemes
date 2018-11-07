package proxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.lang3.SerializationUtils;
import org.json.simple.JSONObject;

import common.IStrategy;
import socket.Request;
import sun.misc.IOUtils;

public class ProxyServer {
    public IStrategy strategy;
    
    public static void init() {
    	
    	//get initial Instances of routing tables or osdMap 
    
    }
    
    
    public static void main(String[] argv) {
		
    	
    	 init();
    	 
		 Socket socket = null; 
	     ServerSocket server  = null; 
	     InputStream in =  null; 
	     
	     try {
	    	 
	    	while(true) {
				server = new ServerSocket(5000);
				socket = server.accept(); 
		         // takes input from the client socket 
		        in = socket.getInputStream(); 
		        byte[] bytes = IOUtils.readFully(in, -1, true);
		        
		    	Request message = SerializationUtils.deserialize(bytes);
		    	
		    	System.out.println(message.getType()+" "+message.getPayload());
		    	
		    	if(message.getType() == "DHT_Update") {
		    		
		    		if((message.getPayload().getClass().getName()).contains("json") ) {
		    			JSONObject obj = (JSONObject)message.getPayload();
		    			
		    			//compare epoch number & if greater...update dht and send messages to datanodes
		    			
		    		}
		    	
		    	}
	    	}
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

    }

}
