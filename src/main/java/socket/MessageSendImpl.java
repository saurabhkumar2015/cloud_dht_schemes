package socket;

import clients.ControlClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import clients.RegularClient;
import clients.RegularClientDemo;
import common.Constants;
import common.EpochPayload;
import common.LogObject;
import common.Payload;
import common.UpdateRoutingPayload;
import config.ConfigLoader;
import proxy.ProxyServer;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
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
            
            /*******Message sent at ************************************************************/
            long start = System.nanoTime();
            /***********************************************************************************/
            
            /******************************************For new version logs*********************/
            if(type.equals(Constants.NEW_VERSION)) {
            	input = new DataInputStream(socket.getInputStream());
	            long end = System.nanoTime();
	            ObjectInputStream ois = new ObjectInputStream(input);
	            EpochPayload p = (EpochPayload) ois.readObject();
	            LogObject obj = ProxyServer.map.get((p.newRoutingTable).getVersionNumber());
	            	//System.out.println("count "+obj.count+" livenodes "+obj.liveNodes);
		            obj.count = obj.count+1;
		            ProxyServer.map.put((p.newRoutingTable).getVersionNumber(),obj);
		            
		            if(obj.count == obj.liveNodes) {
		            	//System.out.println("count equalto live  "+obj.count+" livenodes "+obj.liveNodes);
			            BufferedWriter writer = new BufferedWriter(new FileWriter(ConfigLoader.config.logFileForCC,true));
			            writer.write(p.status+","+Long.toString(end-obj.start));
			            writer.newLine();
			            writer.close();
			            ProxyServer.map.remove((p.newRoutingTable).getVersionNumber());
		            }
	            
            }
            
            /*****************************************************************************************/
            
            /*******************************For write logs********************************************/
            if(type.equals(Constants.WRITE_FILE) || type.equals(Constants.ADD_FILES)) {
            	
	            input = new DataInputStream(socket.getInputStream());
	            
	            long end = System.nanoTime();
	            
	            ObjectInputStream ois = new ObjectInputStream(input);
	            EpochPayload p = (EpochPayload) ois.readObject();

	            System.out.println("Received ack "+p.status);
	            
	            if(type.equals(Constants.WRITE_FILE)){
		            BufferedWriter writer = new BufferedWriter(new FileWriter(ConfigLoader.config.logFileForWrite,true));
		            writer.write(((Payload)payload).fileName+","+Long.toString((end-start))+","+p.status);
		            writer.newLine();
		            writer.close();
            	}
	           
	            if((p.status).trim().equals("Fail due to version mismatch")) {
	            		RegularClient.routingTable = p.newRoutingTable;
		    		RegularClientDemo.routingTable = p.newRoutingTable;
	            }
            }
            
            /***************************************************************************************************/
         
            else if(Arrays.asList(Constants.ADD_NODE, Constants.DELETE_NODE, Constants.LOAD_BALANCE ).contains(type)) {
                if(ConfigLoader.config.dhtType.equalsIgnoreCase("distributed")) {
                    input = new DataInputStream(socket.getInputStream());
                    ObjectInputStream ois = new ObjectInputStream(input);
                    EpochPayload p = (EpochPayload) ois.readObject();

                    System.out.println("received new routing table version: " + p.newRoutingTable.getVersionNumber());
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
