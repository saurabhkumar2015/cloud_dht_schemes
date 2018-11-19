package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
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
    private static boolean distributed = false;


    public ClientWorker(IDataNode node) {
        dataNode = node;
        debug = "debug".equalsIgnoreCase(ConfigLoader.config.verbose);
        distributed = "distributed".equalsIgnoreCase(ConfigLoader.config.dhtType);
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
                    if(distributed) gossipNow();
                    break;
                case DELETE_NODE:
                    Integer nodeId1 = (Integer) request.getPayload();
                    System.out.println("Delete node " + nodeId1);
                    dataNode.deleteNode(nodeId1);
                    if(distributed) gossipNow();
                    break;
                case LOAD_BALANCE:
                    LoadBalance lb = (LoadBalance) request.getPayload();
                    System.out.println("Load Balance    " + lb);
                    dataNode.loadBalance(lb.nodeId, lb.loadFactor);
                    if(distributed) gossipNow();
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
                    if(distributed) gossipNow();
	                break;
       
                default:
                    throw new Exception("Unsupported message type");
            }
            //Append data to text area
        } catch (Exception e) {
            exceptionCount++;
        }
    }

    private void gossipNow() {

        SharedGossipDataMessage message = new SharedGossipDataMessage();
        message.setExpireAt(System.currentTimeMillis()+60000);
        message.setTimestamp(System.currentTimeMillis());
        message.setKey(Constants.ROUTING_TABLE);
        message.setPayload(dataNode.getRoutingTable());
        Commons.gossip.gossipSharedData(message);
    }
}
