package ring;

import java.util.LinkedList;

import common.Commons;
import common.Constants;
import common.IDataNode;
import common.IRoutingTable;

public class DataNode implements IDataNode {

    public RingRoutingTable routingTableObj;

    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTableObj;
    }

    public void addNode(int nodeId) {
    	routingTableObj.addNode(nodeId);
    }
	
	public void deleteNode(int nodeId) {
		routingTableObj.deleteNode(nodeId);
	}
	
	public void loadBalance(int nodeId, double loadFraction) {
		routingTableObj.loadBalance(nodeId, loadFraction);
	}
	
	public void MoveFiles(int clusterIdofNewNode,String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance) {
		
	}

	public void UpdateRoutingTable(IRoutingTable ringNewTable) {
		this.routingTableObj = (RingRoutingTable) ringNewTable;
		System.out.println("New version: " +this.routingTableObj.version);
		this.routingTableObj.printRoutingTable();
	}

	public void writeFile(String fileName, int replicaId) {
		
		System.out.println("\nFileName: "+fileName);
		int nId = routingTableObj.getNodeId(fileName, replicaId);
		System.out.println("File written into node id: "+nId+" Replication Id:"+replicaId);

		Commons.messageSender.sendMessage(routingTableObj.physicalTable.get(nId), Constants.WRITE_FILE, Commons.GeneratePayload(fileName, replicaId));

		/*
		LinkedList<Integer> listOfAssociatedHashes =  routingTableObj.modifiedBinarySearch(hashVal);
		for(int start=0; start<routingTableObj.replicationFactor;start++) {
			int nId = routingTableObj.routingMap.get(listOfAssociatedHashes.get(start));
			System.out.println("File written into node id: "+nId+" Replication Id:"+(start+1));
			Commons.messageSender.sendMessage(routingTableObj.physicalTable.get(nId), Constants.ADD_FILE, Commons.GeneratePayload(fileName, (start+1)));
		}*/
	}

	public void deleteFile(String fileName) {
		
		System.out.println("\nFileName: "+fileName);
		int hashVal = (fileName.hashCode())%this.routingTableObj.MAX_HASH;
		System.out.println("FileHash Value:"+hashVal);
		LinkedList<Integer> listOfAssociatedHashes =  routingTableObj.modifiedBinarySearch(hashVal);
		if(listOfAssociatedHashes!=null) {
		for(int start=0; start<routingTableObj.replicationFactor;start++) {
			int nId = routingTableObj.routingMap.get(listOfAssociatedHashes.get(start));
			System.out.println("File deleted from node id: "+nId+" Replication Id:"+(start+1));
			//Commons.messageSender.sendMessage(routingTableObj.physicalTable.get(nId), Constants.DELETE_FILE, Commons.GeneratePayload(fileName, (start+1)));
		}	
		}
	}
	
}
