package socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import clients.RegularClient;
import common.Constants;
import common.EpochPayload;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MessageSendImpl implements IMessageSend {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void sendMessage(String nodeAddress, String type, Object payload) {

        // Wrapping request into Request Object
        Request request = new Request();
        request.setType(type);
        request.setPayload(payload);
        
        Socket socket = null;
        DataOutputStream out = null;
        DataInputStream input = null;

        // Extracting address and port from NodeId
        String[] arr = nodeAddress.split(":");
        String address = arr[0];
        int port = Integer.parseInt(arr[1]);

        try {
            socket = new Socket(address, port);
            out = new DataOutputStream(socket.getOutputStream());

            byte[] stream = null;
            // ObjectOutputStream is used to convert a Java object into OutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(request);
            stream = baos.toByteArray();
            out.write(stream);
            
            if(type.equals(Constants.WRITE_FILE)) {
	            input = new DataInputStream(socket.getInputStream());
	            ObjectInputStream ois = new ObjectInputStream(input);
	            EpochPayload p = (EpochPayload) ois.readObject();
	          
	            System.out.println("received ack "+p.status);
	            
	            if((p.status).trim().equals("fail"))
	            		RegularClient.routingTable = p.newRoutingTable;
            }
          
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			   try {
				socket.close();
			} catch (IOException e) {

			}
		}
    }
}
