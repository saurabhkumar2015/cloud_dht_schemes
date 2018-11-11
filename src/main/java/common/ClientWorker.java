package common;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.IDataNode;
import org.apache.commons.lang3.SerializationUtils;
import socket.Request;
import sun.misc.IOUtils;

import java.io.*;
import java.net.Socket;
import java.util.Map;

import static common.Constants.FILE_NAME;
import static common.Constants.REPLICA_ID;
import static common.Constants.WRITE_FILE;

public class ClientWorker extends Thread {

    private final Socket client;
    private static ObjectMapper mapper = new ObjectMapper();
    private static IDataNode dataNode;

    public ClientWorker(Socket client, IDataNode node) {
        this.client = client;
        dataNode = node;
    }

    @Override
    public void run() {
        PrintWriter out = null;
        try {
            System.out.println("Thread started with name:"+Thread.currentThread().getName());
            out = new PrintWriter(client.getOutputStream(), true);
        } catch (IOException e) {
            System.out.println("in or out failed");
            System.exit(-1);
        }
        try {
                System.out.println("Thread running with name:"+Thread.currentThread().getName());
                //Send data back to client
                out.println("OK");
                InputStream in = client.getInputStream();
                System.out.println("File Write Step1 ");
                byte[] bytes = IOUtils.readFully(in, -1, true);
                Request request = SerializationUtils.deserialize(bytes);
                System.out.println("File Write Step2 "+ request.getType());
                switch(request.getType()) {
                    case WRITE_FILE:
                        Map<String,Object> map = (Map<String,Object>)request.getPayload();
                        System.out.println("File Write"+ map.get(FILE_NAME).toString());
                        dataNode.writeFile(map.get(FILE_NAME).toString(), Integer.parseInt(map.get(REPLICA_ID).toString()));
                    default:
                        throw new Exception("Unsupported message type");
                }
                //Append data to text area
            } catch (Exception e) {
                System.out.println("Read failed");
                e.printStackTrace();
                System.exit(-1);
            }
        }
}
