package common;

import config.ConfigLoader;
import config.DHTConfig;
import socket.Request;

import java.io.*;
import java.net.Socket;

import ceph.CephRoutingTable;

import static common.Constants.*;

public class ClientWorker {

    private static IDataNode dataNode;
    private static int exceptionCount;
    private static boolean debug = true;


    public ClientWorker(IDataNode node) {
        dataNode = node;
        debug = "debug".equalsIgnoreCase(ConfigLoader.config.verbose);
    }

    public void run(Socket client) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(client.getOutputStream(), true);
        } catch (IOException e) {
            System.out.println("in or out failed");
            System.exit(-1);
        }
        try {
            out.println("OK");
            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            Request request = (Request) in.readObject();
            switch (request.getType()) {
                case WRITE_FILE:
                    Payload p = (Payload) request.getPayload();
                    System.out.println("File Write:: " + p.fileName);
                    dataNode.writeFile(p.fileName, p.replicaId);
                    break;
                case DELETE_FILE:
                    Payload p1 = (Payload) request.getPayload();
                    System.out.println("File Write:: " + p1.fileName);
                    dataNode.writeFile(p1.fileName, p1.replicaId);
                    break;
                case ADD_NODE:
                    Integer nodeId = (Integer) request.getPayload();
                    System.out.println("Add node " + nodeId);
                    dataNode.addNode(nodeId);
                    break;
                case DELETE_NODE:
                    Integer nodeId1 = (Integer) request.getPayload();
                    System.out.println("Delete node " + nodeId1);
                    dataNode.deleteNode(nodeId1);
                    break;
                case LOAD_BALANCE:
                    LoadBalance lb = (LoadBalance) request.getPayload();
                    System.out.println("Load Balance    " + lb);
                    dataNode.loadBalance(lb.nodeId, lb.loadFactor);
                    break;
                    
                case MOVE_FILE:
                	CephPayload payload = (CephPayload) request.getPayload();
                	System.out.println("Received move file request from proxy: "+ payload);
                    dataNode.UpdateRoutingTable((IRoutingTable)payload.updated_ceph_routing_table);
                    dataNode.MoveFiles(payload.clusterId, payload.nodeIp, payload.nodeWeight, payload.totalWt, payload.isLoadBalance);
                    break;
                
                case ADD_HASH:
                	String hashRangeToBeAdded = (String) request.getPayload();
                	System.out.println("Received hash range add request: "+ hashRangeToBeAdded);
                	dataNode.addHashRange(hashRangeToBeAdded);
                    break;
                   
                case REMOVE_HASH:
                	String hashRangeToBeDeleted = (String) request.getPayload();
                	System.out.println("Received hash range delete request: "+ hashRangeToBeDeleted);
                	dataNode.deleteFile(hashRangeToBeDeleted);
                    break;
                 
       
                default:
                    throw new Exception("Unsupported message type");
            }
            //Append data to text area
        } catch (Exception e) {
            exceptionCount++;
        }
    }
}
