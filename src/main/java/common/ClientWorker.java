package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import socket.Request;

import java.io.*;
import java.net.Socket;
import java.util.List;

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
                    System.out.println("File Write:: " + p.fileName + "Replica:" + p.replicaId);
                     long dataNodeVersionNo = dataNode.getRoutingTable().getVersionNumber();
                     System.out.println("DataNode versionNumber:: " + dataNodeVersionNo + " Regular Client versionNumber:: " + p.versionNumber);
                     if (dataNodeVersionNo > p.versionNumber) {
                         System.out.println("Sender's routing table needs to be updated");
                         EpochPayload payload = new EpochPayload("Fail due to version mismatch", dataNode.getRoutingTable());
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
                  
                 case ADD_FILES:
                 	
                      @SuppressWarnings("unchecked") 
                     List<Payload> paylds = (List<Payload>) request.getPayload();
                     System.out.println("Received Add files request");
                     long dataNodeVerNo = dataNode.getRoutingTable().getVersionNumber();
                     if(paylds.size() == 0)
                         break;
                     System.out.println("DataNode versionNumber:: " + dataNodeVerNo + " Sender datanode's versionNumber:: " + paylds.get(0).versionNumber);
	                      
                    	 if (dataNodeVerNo > paylds.get(0).versionNumber) {
	                          System.out.println("Sender's routing table needs to be updated");
	                          EpochPayload payload = new EpochPayload("fail", dataNode.getRoutingTable());
	                          oos.writeObject(payload);
	                          stream = baos.toByteArray();
	                          out.write(stream);
	                      } else {
	                          dataNode.writeAllFiles(paylds);
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
                    if(distributed) {
                        IRoutingTable table = dataNode.getRoutingTable().addNode(nodeId);
                        System.out.println("ADD NODE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(nodeId, table, ADD_NODE);
                        gossipNow(ADD_NODE, nodeId);
                        System.out.println("New Version  of Routing Table sent to control client");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }
                    else{
                        System.out.println("Add node " + nodeId);
                        dataNode.addNode(nodeId);
                    }
                    break;
                case DELETE_NODE:
                    Integer nodeId1 = (Integer) request.getPayload();
                    if(distributed) {
                        IRoutingTable table = dataNode.getRoutingTable().deleteNode(nodeId1);
                        System.out.println("DELETE NODE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(nodeId1, table, DELETE_NODE);
                        gossipNow(DELETE_NODE,nodeId1);
                        System.out.println("Sender's routing table needs to be updated");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }
                    break;
                case LOAD_BALANCE:
                    LoadBalance lb = (LoadBalance) request.getPayload();
                    System.out.println("Load Balance    " + lb);
                    if(distributed) {
                        IRoutingTable table  = dataNode.getRoutingTable().loadBalance(lb.nodeId, lb.loadFactor);
                        System.out.println("LOAD BALANCE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(lb.nodeId, table, LOAD_BALANCE);
                        gossipNow(LOAD_BALANCE, lb.nodeId);
                        System.out.println("New Version  of Routing Table sent to control client");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                    }else {
                        dataNode.loadBalance(lb.nodeId, lb.loadFactor);
                    }
                    break;
                    
                 case MOVE_FILE:
                	
                	if((ConfigLoader.config.scheme).toUpperCase().equals("CEPH")) {
	                	CephPayload payload = (CephPayload) request.getPayload();
	                	System.out.println("Received move file request from proxy: "+ payload);
	                    //dataNode.UpdateRoutingTable((IRoutingTable)payload.updated_ceph_routing_table);
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
                	System.out.println("Received hash bucket add request");
                	dataNode.addHashRange(hashRangeToBeAdded);
                    break;

                case REMOVE_HASH:
                	String hashRangeToBeDeleted = (String) request.getPayload();
                	System.out.println("Received hash bucket delete request: ");
                	dataNode.deleteFile(hashRangeToBeDeleted);
                    break;

               case NEW_VERSION:
                	UpdateRoutingPayload payld = (UpdateRoutingPayload) request.getPayload();
	                System.out.println("Received update routing table request from proxy");

                   switch (((ConfigLoader.config.scheme).toUpperCase())) {
                       case "ELASTIC":
                           dataNode.newUpdatedRoutingTable(payld.nodeId, payld.type, payld.newRoutingTable);
                           break;
                       case "CEPH":
                           dataNode.UpdateRoutingTable(payld.newRoutingTable, payld.type);
                           break;
                       case "RING":
                           dataNode.UpdateRoutingTable(payld.newRoutingTable, payld.type);
                           break;
                   }
	                break;
                default:
                    throw new Exception("Unsupported message type");
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionCount++;
        }
    }

    private void updates(Integer nodeId, IRoutingTable table, String type) {

        switch((ConfigLoader.config.scheme).toUpperCase()) {
            case "ELASTIC":
                dataNode.newUpdatedRoutingTable(nodeId, type, table);
            case "CEPH":
                dataNode.UpdateRoutingTable(table, type);
            case "RING":
                dataNode.UpdateRoutingTable(table, type);
        }
    }

    private void sendRoutingTable(DataOutputStream out, ByteArrayOutputStream baos, ObjectOutputStream oos, String success, IRoutingTable routingTable) throws IOException {
        byte[] stream;
        EpochPayload payload = new EpochPayload(success, routingTable);
        oos.writeObject(payload);
        stream = baos.toByteArray();
        out.write(stream);
    }

    private void gossipNow(String type, int nodeId) {
        SharedGossipDataMessage message = new SharedGossipDataMessage();
        message.setExpireAt(System.currentTimeMillis()+120000);
        message.setTimestamp(System.currentTimeMillis());
        message.setKey(Constants.ROUTING_TABLE);
        message.setNodeId(Integer.toString(Commons.nodeId));
        RoutingTableWrapper wrapper = new RoutingTableWrapper();
        wrapper.table = dataNode.getRoutingTable();
        wrapper.type = type;
        wrapper.nodeId = nodeId;
        message.setPayload(wrapper);
        Commons.gossip.gossipSharedData(message);
        System.out.println("GOSSIP MESSAGE SENT WITH NEW VERSION:" + wrapper.table.getVersionNumber());
    }
}
