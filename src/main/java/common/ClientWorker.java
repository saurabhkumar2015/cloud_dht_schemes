package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import socket.Request;

import java.io.*;
import java.net.Socket;

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
                     long dataNodeVersionNo = dataNode.getRoutingTable().getVersionNumber();
                     System.out.println("DataNode versionNumber:: " + dataNodeVersionNo + " Regular Client versionNumber:: " + p.versionNumber);
                     if (dataNodeVersionNo > p.versionNumber) {
                         System.out.println("Sender's routing table needs to be updated");
                         EpochPayload payload = new EpochPayload("fail", dataNode.getRoutingTable());
                         oos.writeObject(payload);
                         stream = baos.toByteArray();
                         out.write(stream);
                     } else {
                         dataNode.writeFile(p.fileName, p.replicaId);
                         EpochPayload payload = new EpochPayload("success", null);
                         oos.writeObject(payload);
                         stream = baos.toByteArray();
                         out.write(stream);
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
                    if(distributed) {
                        gossipNow();
                        System.out.println("Sender's routing table needs to be updated");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }
                    break;
                case DELETE_NODE:
                    Integer nodeId1 = (Integer) request.getPayload();
                    System.out.println("Delete node " + nodeId1);
                    dataNode.deleteNode(nodeId1);
                    if(distributed) {
                        gossipNow();
                        System.out.println("Sender's routing table needs to be updated");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }
                    break;
                case LOAD_BALANCE:
                    LoadBalance lb = (LoadBalance) request.getPayload();
                    System.out.println("Load Balance    " + lb);
                    dataNode.loadBalance(lb.nodeId, lb.loadFactor);
                    if(distributed) {
                        gossipNow();
                        System.out.println("Sender's routing table needs to be updated");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }
                    break;
                    
                 case MOVE_FILE:
                	
                	if((ConfigLoader.config.scheme).toUpperCase().equals("CEPH")) {
	                	CephPayload payload = (CephPayload) request.getPayload();
	                	System.out.println("Received move file request from proxy: "+ payload);
	                    dataNode.UpdateRoutingTable((IRoutingTable)payload.updated_ceph_routing_table);
	                    dataNode.MoveFiles(payload.clusterId, payload.nodeIp, payload.nodeWeight, payload.totalWt, payload.isLoadBalance);
                	}
                    break;
                    
                case MOVE_ON_DELETE:
                	System.out.println("inside client worker move on delete");
                	CephPayload payload = (CephPayload) request.getPayload();
//                	((CephDataNode)dataNode).OnDeleteNodeMoveFile();
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
                	UpdateRoutingPayload payld = (UpdateRoutingPayload) request.getPayload();
	                System.out.println("Received update routing table request from proxy: "+ payld);
	                dataNode.newUpdatedRoutingTable(payld.nodeId, payld.type, payld.newRoutingTable);
                    if(distributed) gossipNow();
	                break;
                default:
                    throw new Exception("Unsupported message type");
            }
        } catch (Exception e) {
            exceptionCount++;
        }
    }

    private void sendRoutingTable(DataOutputStream out, ByteArrayOutputStream baos, ObjectOutputStream oos, String success, IRoutingTable routingTable) throws IOException {
        byte[] stream;
        EpochPayload payload = new EpochPayload(success, routingTable);
        oos.writeObject(payload);
        stream = baos.toByteArray();
        out.write(stream);
    }

    private void gossipNow() throws IOException {
        System.out.println("Please Updated Share data");
        SharedGossipDataMessage message = new SharedGossipDataMessage();
        message.setExpireAt(System.currentTimeMillis()+120000);
        message.setTimestamp(System.currentTimeMillis());
        message.setKey(Constants.ROUTING_TABLE);
        message.setNodeId(Integer.toString(Commons.nodeId));
        message.setPayload(dataNode.getRoutingTable());
        Commons.gossip.gossipSharedData(message);
        System.out.println("Yes Updated Share data");
    }
}
