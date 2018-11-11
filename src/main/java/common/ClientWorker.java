package common;

import com.fasterxml.jackson.databind.ObjectMapper;
import datanode.IDataNode;
import socket.Request;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import static common.Constants.WRITE_FILE;

public class ClientWorker extends Thread {

    private final Socket client;
    private static ObjectMapper mapper = new ObjectMapper();

    public ClientWorker(Socket client, IDataNode node) {
        this.client = client;
    }

    @Override
    public void run() {
        String line;
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            System.out.println("Thread started with name:"+Thread.currentThread().getName());
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(client.getOutputStream(), true);
        } catch (IOException e) {
            System.out.println("in or out failed");
            System.exit(-1);
        }

        while (true) {
            try {
                System.out.println("Thread running with name:"+Thread.currentThread().getName());
                line = in.readLine();
                //Send data back to client
                out.println("OK");
                Request request = mapper.readValue(line, Request.class);
                switch(request.getType()) {
                    case WRITE_FILE:


                    default:
                        throw new Exception("Unsupported message type");
                }

                //Append data to text area
            } catch (Exception e) {
                System.out.println("Read failed");
                System.exit(-1);
            }
        }
    }
}
