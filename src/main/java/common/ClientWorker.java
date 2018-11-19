package common;

import config.ConfigLoader;
import config.DHTConfig;
import socket.Request;

import java.io.*;
import java.net.Socket;

import ceph.CephDataNode;
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
        
        try {
            
        	DataOutputStream out = null;
            out = new DataOutputStream(client.getOutputStream());
            
            byte[] stream = null;
            // ObjectOutputStream is used to convert a Java object into OutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            
            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            Request request = (Request) in.readObject();
        
            switch (request.getType()) {
                case WRITE_FILE:
                	Payload p = (Payload) request.getPayload();
                    System.out.println("File Write:: " + p.fileName);
                    
                    if((ConfigLoader.config.scheme).toUpperCase().equals("CEPH")) {
                    	
	                    CephDataNode cephDataNd = (CephDataNode)dataNode;
	                    CephRoutingTable cephRoutingTable = (CephRoutingTable)cephDataNd.cephRtTable;
	                    
	                    System.out.println("DataNode version:: " +cephRoutingTable.VersionNo+ " Regular Client version:: "+ p.versionNumber);
	                    
	                    if(cephRoutingTable.VersionNo > p.versionNumber) {
	                    	System.out.println("Sender's routing table needs to be updated");
	                    	EpochPayload payload = new EpochPayload("fail", cephDataNd.cephRtTable);
	                    	oos.writeObject(payload);
	                        stream = baos.toByteArray();
	                        out.write(stream);

	                    }
	                    else {
	                    	System.out.println("Sender's routing table up to date");
	                    	dataNode.writeFile(p.fileName, p.replicaId);
	                    	EpochPayload payload = new EpochPayload("success", null);
	                    	oos.writeObject(payload);
	                        stream = baos.toByteArray();
	                        out.write(stream);
	                    }
                    }
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
                    
                case NEW_VERSION:
                	EpochPayload payld = (EpochPayload) request.getPayload();
	                System.out.println("Received update routing table request from proxy: "+ payld);
	                dataNode.UpdateRoutingTable((IRoutingTable)payld.newRoutingTable);
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
