package socket;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.lang3.SerializationUtils;

import socket.Request;
import sun.misc.IOUtils;

public class Receiver {
	

	public static void main(String[] argv) {
		
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
		    	System.out.println(message.getType()+" "+message.getPayload().getClass());
	    	}
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     }
}
