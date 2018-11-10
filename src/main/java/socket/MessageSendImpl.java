package socket;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class MessageSendImpl implements IMessageSend {
	 
	@Override
	public void sendMessage(String NodeId, String type, Object payload) {
		
		// Wrapping request into Request Object 
		Request request = new Request();
		request.setType(type);
		request.setPayload(payload);
		
		
		Socket socket = null; 
		DataInputStream  input   = null; 
		DataOutputStream out     = null; 
		 
		// Extracting address and port from NodeId
		String[] arr = NodeId.split(":");
		String address = arr[0];
		int port = Integer.parseInt(arr[1]);
		
	    try {
			socket = new Socket(address, port);
			out    = new DataOutputStream(socket.getOutputStream()); 
			
			 byte[] stream = null;
			    // ObjectOutputStream is used to convert a Java object into OutputStream
			 ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 ObjectOutputStream oos = new ObjectOutputStream(baos);
			 oos.writeObject(request);
			 stream = baos.toByteArray();
			 
			 out.write(stream);
			   
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    // close the connection 
        try
        { 
            out.close(); 
            socket.close(); 
        } 
        catch(IOException i) 
        { 
            System.out.println(i); 
        }
	}
}
