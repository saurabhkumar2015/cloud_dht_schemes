package ring;

import java.util.LinkedList;

import common.IDataNode;
import common.IRoutingTable;
import common.Constants;
import common.Commons;

public class DataNode implements IDataNode {
	
	public int myNodeId;
	public DataNode(int id){
		this.myNodeId = id;
	}
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

	//Send routing table
	public IRoutingTable getRoutingTable() {
		return this.routingTableObj;
	}
	
	//update to latest routing table
	public void UpdateRoutingTable(IRoutingTable ringNewTable) {
		this.routingTableObj = (RingRoutingTable) ringNewTable;
		System.out.println("New version: " +this.routingTableObj.version);
		//this.routingTableObj.printRoutingTable();
	}

	//write file request handling
	public boolean writeFile(String fileName, int replicaId) {
		//System.out.println("\nFileName: "+fileName);
		//When the receiving node is primary node for the given file
		int nId = routingTableObj.giveNodeId(fileName, replicaId);
		if(nId == myNodeId) {
			System.out.println("File written with Replication Id:"+replicaId);
			return true;
		}
		else {
			int hashVal = Math.abs(fileName.hashCode())%this.routingTableObj.MAX_HASH;
	        LinkedList<Integer> listOfNodesForGivenHash = routingTableObj.modifiedBinarySearch(hashVal);
	        if (listOfNodesForGivenHash!=null) {
	        	if(listOfNodesForGivenHash.size()>=this.routingTableObj.replicationFactor) {
	        		System.out.println(this.routingTableObj.routingMap.get(listOfNodesForGivenHash.get(replicaId-1)));
	        		nId = routingTableObj.routingMap.get(listOfNodesForGivenHash.get(replicaId-1));
	        		//When the receiving node is the given replica node for the file
	        		if(nId == myNodeId) {
	        			System.out.println("File written with Replication Id:"+replicaId);
	        			return true;
	        		}
	        		else {
	        			//sending write file request to the corresponding node
	        			Commons.messageSender.sendMessage(routingTableObj.physicalTable.get(nId), Constants.WRITE_FILE, Commons.GeneratePayload(fileName, replicaId, this.routingTableObj.version));
	        			return true;
	        		}
	        	}
	        }
		}
		return false;
	}
	
	public void addHashRange(String hashRange) {
		System.out.println("My Node Id: "+ this.myNodeId);
		System.out.println("Files corresponding to Hash range: "+ hashRange+" added\n");
	}
	
	public void deleteFile(String hashRange) {
		System.out.println("My Node Id: "+ this.myNodeId);
		System.out.println("Files corresponding to Hash range: "+ hashRange+" deleted\n");
	}
	
	public void MoveFiles(int clusterIdofNewNode,String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance) {
		
	}
	
}
