package ring;

import java.util.LinkedList;

import common.IDataNode;
import common.IRoutingTable;
import common.Constants;
import common.Commons;

public class DataNode implements IDataNode {
	
	public int myNodeId;
	public RingRoutingTable routingTableObj;
	
	public DataNode(int id){
		RingDHTScheme ring = new RingDHTScheme();
		this.routingTableObj = ring.routingTableObj;
		this.myNodeId = id;
	}
	public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTableObj;
    }
    public void addNode(int nodeId) {
    	this.routingTableObj.addNode(nodeId);
    }
	
	public void deleteNode(int nodeId) {
		this.routingTableObj.deleteNode(nodeId);
	}
	
	public void loadBalance(int nodeId, double loadFraction) {
		this.routingTableObj.loadBalance(nodeId, loadFraction);
	}

	//Send routing table
	public IRoutingTable getRoutingTable() {
		System.out.println("hey");
		return this.routingTableObj;
	}
	
	//update to latest routing table
	public void UpdateRoutingTable(IRoutingTable ringNewTable) {
		this.routingTableObj = (RingRoutingTable) ringNewTable;
		System.out.println("New versionNumber: " +this.routingTableObj.versionNumber);
		//this.routingTableObj.printRoutingTable();
	}

	//write file request handling
	public boolean writeFile(String fileName, int replicaId) {
		//System.out.println("\nFileName: "+fileName);
		//When the receiving node is primary node for the given file
		int nId = this.routingTableObj.giveNodeId(fileName, replicaId);
		if(nId == this.myNodeId) {
			System.out.println("File written with Replication Id:"+replicaId);
			return true;
		}
		else {
			int hashVal = Math.abs(fileName.hashCode())%this.routingTableObj.MAX_HASH;
	        LinkedList<Integer> listOfNodesForGivenHash = this.routingTableObj.modifiedBinarySearch(hashVal);
	        if (listOfNodesForGivenHash!=null) {
	        	if(listOfNodesForGivenHash.size()>=this.routingTableObj.replicationFactor) {
	        		System.out.println(this.routingTableObj.routingMap.get(listOfNodesForGivenHash.get(replicaId-1)));
	        		nId = this.routingTableObj.routingMap.get(listOfNodesForGivenHash.get(replicaId-1));
	        		System.out.println("nId:"+nId);
	        		System.out.println("my node Id:"+this.myNodeId);
	        		//When the receiving node is the given replica node for the file
	        		if(nId == this.myNodeId) {
	        			System.out.println("File written with Replication Id:"+replicaId);
	        			return true;
	        		}
	        		else {
	        			//sending write file request to the corresponding node
	        			Commons.messageSender.sendMessage(this.routingTableObj.physicalTable.get(nId), Constants.WRITE_FILE, Commons.GeneratePayload(fileName, replicaId, this.routingTableObj.versionNumber));
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

	@Override
	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {

	}

	public void deleteFile(String hashRange) {
		System.out.println("My Node Id: "+ this.myNodeId);
		System.out.println("Files corresponding to Hash range: "+ hashRange+" deleted\n");
	}
	
	public void MoveFiles(int clusterIdofNewNode,String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance) {
		
	}
	
}
