package socket;

import clients.ControlClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import clients.RegularClient;
import common.Constants;
import common.EpochPayload;
import config.ConfigLoader;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
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


            if(type.equals(Constants.WRITE_FILE) || type.equals(Constants.ADD_FILES)) {
            	
	            input = new DataInputStream(socket.getInputStream());
	            ObjectInputStream ois = new ObjectInputStream(input);
	            EpochPayload p = (EpochPayload) ois.readObject();

	            System.out.println("Received ack "+p.status);

	            if((p.status).trim().equals("Fail due to version mismatch"))
	            		RegularClient.routingTable = p.newRoutingTable;
            }
            else if(Arrays.asList(Constants.ADD_NODE, Constants.DELETE_NODE, Constants.LOAD_BALANCE ).contains(type)) {
                if(ConfigLoader.config.dhtType.equalsIgnoreCase("distributed")) {
                    input = new DataInputStream(socket.getInputStream());
                    ObjectInputStream ois = new ObjectInputStream(input);
                    EpochPayload p = (EpochPayload) ois.readObject();

                    System.out.println("received ack " + p.status);
                    ControlClient.routingTable = p.newRoutingTable;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
			   try {
				socket.close();
			} catch (IOException e) {
			}
		}
    }
}
