package common;

import com.fasterxml.jackson.databind.ObjectMapper;
import socket.Request;
import java.io.*;
import java.net.Socket;

import static common.Constants.*;

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
                ObjectInputStream in = new ObjectInputStream(client.getInputStream());
                System.out.println("File Write Step1 ");
                Request request = (Request)in.readObject();
                System.out.println("File Write Step2 "+ request.getType());
                switch(request.getType()) {
                    case WRITE_FILE:
                        Payload p = (Payload)request.getPayload();
                        System.out.println("File Write"+ p.fileName);
                        dataNode.writeFile(p.fileName, p.replicaId);
                        break;
                    case DELETE_FILE:
                        Payload p1 = (Payload)request.getPayload();
                        System.out.println("File Write"+ p1.fileName);
                        dataNode.writeFile(p1.fileName, p1.replicaId);
                        break;
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
