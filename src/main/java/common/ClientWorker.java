package common;

import ceph.CephDataNode;
import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import ring.DataNode;

import schemes.ElasticDHT.DataNodeElastic;
import schemes.ElasticDHT.ERoutingTable;
import socket.Request;

import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.List;

import static common.Commons.dateFormat;
import static common.Commons.lock;
import static common.Constants.*;

public class ClientWorker extends Thread {

    private static IDataNode dataNode;
    private static Socket client;
    private static int exceptionCount;
    private static boolean debug = true;
    private static boolean distributed = false;
    DataOutputStream out;
    ObjectInputStream in;
    
    public ClientWorker(IDataNode node,Socket client,DataOutputStream out, ObjectInputStream in) {
    	this.client = client;
    	this.out = out;
    	this.in = in;
        dataNode = node;
        debug = "debug".equalsIgnoreCase(ConfigLoader.config.verbose);
        distributed = "distributed".equalsIgnoreCase(ConfigLoader.config.dhtType);
    }
 
    @Override
    public void run() {
        try {
            //DataOutputStream out = null;
            //out = new DataOutputStream(client.getOutputStream());

            byte[] stream = null;
            // ObjectOutputStream is used to convert a Java object into OutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);

            //ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            Request request = (Request) in.readObject();
            
            switch (request.getType()) {
                  
                 case WRITE_FILE:
                    Payload p = (Payload) request.getPayload();
                    long dataNodeVersionNo = dataNode.getRoutingTable().getVersionNumber();
                     if (dataNodeVersionNo > p.versionNumber) {
                         System.out.println("Sender's routing table needs to be updated");
                         System.out.println("DataNode versionNumber:: " + dataNodeVersionNo + " Regular Client versionNumber:: " + p.versionNumber);
                         EpochPayload payload = new EpochPayload("Fail due to version mismatch", dataNode.getRoutingTable());
                         oos.writeObject(payload);
                         stream = baos.toByteArray();
                         out.write(stream);
                     } else {
                    	 	
                    	 if((ConfigLoader.config.scheme.toUpperCase()).equals("CEPH")) {
                    	     List<Integer> list = new ArrayList<Integer>();
                    	     list.add(((CephDataNode)dataNode).hashGenerator.givePlacementGroupIdFromFileName(p.fileName, ConfigLoader.config.PlacementGroupMaxLimit));
                       	  	 list.add(p.replicaId);
                       	 
                    	     if(((CephDataNode)dataNode).fileLckMap.containsKey(list)) {
                    	    	 EpochPayload payload = new EpochPayload("Fail due to lock", null);
                                 oos.writeObject(payload);
                                 stream = baos.toByteArray();
                                 out.write(stream);
                    	     }
                    	     else {
                        	     dataNode.writeFile(p.fileName, p.replicaId);
    	                         EpochPayload payload = new EpochPayload("success", null);
    	                         oos.writeObject(payload);
    	                         stream = baos.toByteArray();
    	                         out.write(stream);
                        	   }
                    	   }
                    	  else {
                    	     dataNode.writeFile(p.fileName, p.replicaId);
	                         EpochPayload payload = new EpochPayload("success", null);
	                         oos.writeObject(payload);
	                         stream = baos.toByteArray();
	                         out.write(stream);
                    	   }
                    }
                     
                    break;
                  
                 case ADD_FILES:
                 	
                      @SuppressWarnings("unchecked") 
                     List<Payload> paylds = (List<Payload>) request.getPayload();
                     System.out.println("Received Add files request");
                     long dataNodeVerNo = dataNode.getRoutingTable().getVersionNumber();
                     if(paylds.size() == 0)
                         break;
                     //System.out.println("DataNode versionNumber:: " + dataNodeVerNo + " Sender datanode's versionNumber:: " + paylds.get(0).versionNumber);
	                      
                    	 if (dataNodeVerNo > paylds.get(0).versionNumber) {
	                          System.out.println("Sender's routing table needs to be updated");
	                          EpochPayload payload = new EpochPayload("Fail due to version mismatch", dataNode.getRoutingTable());
	                          oos.writeObject(payload);
	                          stream = baos.toByteArray();
	                          out.write(stream);
	                      } else {
	                    	 
	                          dataNode.writeAllFiles(paylds);
	                          
	                          if((ConfigLoader.config.scheme.toUpperCase()).equals("CEPH")) {
	                        	
		                          for(int i=0;i<paylds.size();i++) {
		                        	  List<Integer> list = new ArrayList<Integer>();
		                        	  list.add(Integer.parseInt(paylds.get(i).fileName));
		                        	  list.add(paylds.get(i).replicaId);
		                    		  ((CephDataNode)dataNode).fileLckMap.remove(list);
		                    		  
		                    		 
		                    	  }
	                          }
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
                    if(distributed) {
                        lock.lock();
                        DistributedPayload dp = (DistributedPayload) request.getPayload();
                        waitForNewVersion(dp);
                        IRoutingTable table = dataNode.getRoutingTable().addNode(dp.nodeId);
                        System.out.println("ADD NODE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(dp.nodeId, table, ADD_NODE);
                        gossipNow(ADD_NODE, dp.nodeId, 0);
                        System.out.println("New Version  of Routing Table sent to control client");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                        lock.unlock();
                    }
                    else{
                        Integer nodeId = (Integer) request.getPayload();
                        System.out.println("Add node " + nodeId);
                        dataNode.addNode(nodeId);
                    }
                    break;
                case DELETE_NODE:
                    if(distributed) {
                        lock.lock();
                        DistributedPayload dp = (DistributedPayload) request.getPayload();
                        waitForNewVersion(dp);
                        IRoutingTable table = dataNode.getRoutingTable().deleteNode(dp.nodeId);
                        System.out.println("DELETE NODE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(dp.nodeId, table, DELETE_NODE);
                        gossipNow(DELETE_NODE,dp.nodeId,0);
                        System.out.println("Sender's routing table needs to be updated");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                        lock.unlock();
                    }
                    break;
                case LOAD_BALANCE:
                    if(distributed) {
                        lock.lock();
                        DistributedPayload dp = (DistributedPayload) request.getPayload();
                        waitForNewVersion(dp);
                        IRoutingTable table  = dataNode.getRoutingTable().loadBalance(dp.nodeId, dp.loadFactor);
                        System.out.println("LOAD BALANCE ROUTING TABLE UPDATED TO VERSION:" + table.getVersionNumber());
                        updates(dp.nodeId, table, LOAD_BALANCE);
                        gossipNow(LOAD_BALANCE, dp.nodeId, dp.loadFactor);
                        System.out.println("New Version  of Routing Table sent to control client");
                        sendRoutingTable(out, baos, oos, "success", dataNode.getRoutingTable());
                        lock.unlock();
                    }else {
                        Integer nodeId1 = (Integer) request.getPayload();
                        LoadBalance lb = (LoadBalance) request.getPayload();
                        System.out.println("Load Balance    " + lb);
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
                            switch (payld.type.toUpperCase()) {
                                case ADD_NODE:
                                    dataNode.addNode(payld.nodeId);
                                    break;
                                case DELETE_NODE:
                                    dataNode.deleteNode(payld.nodeId);
                                    break;
                                case LOAD_BALANCE:
                                    dataNode.loadBalance(payld.nodeId, payld.factor);
                                    break;
                            }
                            Commons.elasticOldERoutingTable = Commons.elasticERoutingTable;
                            Commons.elasticERoutingTable = (ERoutingTable) payld.newRoutingTable;
                           break;
                       case "CEPH":
                           dataNode.UpdateRoutingTable(payld.newRoutingTable, payld.type);
                           break;
                       case "RING":
                           dataNode.UpdateRoutingTable(payld.newRoutingTable, payld.type);
                           break;
                   }
	     
	                break;
	                
	        case PRINT_REQUEST:
	            switch(ConfigLoader.config.scheme.toUpperCase()) {
                    case "CEPH":
                        ((CephDataNode)dataNode).showDataNodeState();
                        break;
                    case "RING":
                        ((DataNode)dataNode).getRoutingTable().printRoutingTable();
                        break;
                    case "ELASTIC":
                        ((DataNodeElastic)dataNode).getRoutingTable().printRoutingTable();
                        break;
                }
               	   break;
			    
		case FILE_LIST:
	        	System.out.println("Received file list request");
	            switch(ConfigLoader.config.scheme.toUpperCase()) {
                    case "CEPH":
                    	 @SuppressWarnings("unchecked") List<Payload> payls = (List<Payload>) request.getPayload();
                    	 for(int i=0;i<payls.size();i++) {
                    		
                    		List<Integer> list = new ArrayList<Integer>();
             				list.add(Integer.parseInt(payls.get(i).fileName));
             				list.add(payls.get(i).replicaId);
             				((CephDataNode)dataNode).fileLckMap.put(list, true);
                    	 }
                        break;
                    case "RING":
                       
                        break;
                    case "ELASTIC":
                      
                        break;
                }
               	   break;
                default:
                    throw new Exception("Unsupported message type");
            }
          //  client.close();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionCount++;
        }
    }

    private void waitForNewVersion(DistributedPayload dp) throws InterruptedException {
        boolean lock = true;
        Commons.lock.unlock();
        long dnVersion = dataNode.getRoutingTable().getVersionNumber();
        while(dp.version > dnVersion) {
            if(lock){
                System.out.println(dateFormat.format(new Date())+": WAITING FOR NEW VERSION TO ARRIVE!!! " +
                        "CONTROL CLIENT HAS AN UPDATED VERSION:"+dp.version + " DATANODE VERSION:"+ dnVersion);
                lock = false;
            }
            dnVersion = dataNode.getRoutingTable().getVersionNumber();
            Thread.sleep(200L);
        }
        System.out.println(dateFormat.format(new Date())+": NEW VERSION OF ROUTING TABLE DETECTED:" + dnVersion);
        Commons.lock.lock();
    }

    private void updates(Integer nodeId, IRoutingTable table, String type) {

        switch((ConfigLoader.config.scheme).toUpperCase()) {
            case "ELASTIC":
                Commons.elasticOldERoutingTable = Commons.elasticERoutingTable;
                Commons.elasticERoutingTable = (ERoutingTable) table;
                break;
            case "CEPH":
                dataNode.UpdateRoutingTable(table, type);
                break;
            case "RING":
                dataNode.UpdateRoutingTable(table, type);
                break;
        }
    }

    private void sendRoutingTable(DataOutputStream out, ByteArrayOutputStream baos, ObjectOutputStream oos, String success, IRoutingTable routingTable) throws IOException {
        byte[] stream;
        EpochPayload payload = new EpochPayload(success, routingTable);
        oos.writeObject(payload);
        stream = baos.toByteArray();
        out.write(stream);
    }

    private void gossipNow(String type, int nodeId, double factor) {
        SharedGossipDataMessage message = new SharedGossipDataMessage();
        message.setExpireAt(System.currentTimeMillis()+120000);
        message.setNodeId(Integer.toString(dataNode.getNodeId()));
        message.setTimestamp(System.currentTimeMillis());
        message.setKey(Constants.ROUTING_TABLE);
        RoutingTableWrapper wrapper = new RoutingTableWrapper();
        wrapper.table = dataNode.getRoutingTable();
        wrapper.type = type;
        wrapper.nodeId = nodeId;
        wrapper.factor = factor;
        wrapper.originatorNodeId = dataNode.getNodeId();
        message.setPayload(wrapper);
        Commons.gossip.gossipSharedData(message);
        System.out.println(dateFormat.format(new Date())+" GOSSIP MESSAGE SENT WITH NEW VERSION:" + wrapper.table.getVersionNumber());
    }
}
