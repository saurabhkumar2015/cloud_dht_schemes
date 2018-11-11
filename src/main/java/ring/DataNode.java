package ring;

import java.util.*;

public class DataNode implements IDataNode {
	
	RingRoutingTable routingTableObj;
    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTable;
    }
    
    //nodeId = ip:port
    public void addNode(String nodeId) {
    	int newHash = routingTableObj.getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfHashesForNewHash = routingTableObj.modifiedBinarySearch(newHash);
    	System.out.println("\n");
    	System.out.println("Adding new node: "+nodeId);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " removed from Node :"+ routingTableObj.routingMap.get(listOfHashesForNewHash.get(0)));
    	System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " removed from Node :"+ routingTableObj.routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1)));
    	int newNodeId = ++this.routingTableObj.numNodeIds;
    	//update routing map
    	this.routingTableObj.routingMap.put(newHash, newNodeId);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " added to Node :"+ newNodeId);
    	//update physical table
    	this.routingTableObj.physicalTable.put(newNodeId, nodeId);
    	System.out.println("\n");
    	//Print updated Routing Table
    	System.out.println("New Routing Map after new node added");
    	routingTableObj.printRoutingTable();
    	System.out.println("\n");
    	System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	routingTableObj.printPhysicalTable();
    }
	
	public void deleteNode(String nodeId) {
		int deleteHash = routingTableObj.getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfAssociatedHashes = routingTableObj.modifiedBinarySearch(deleteHash);
    	LinkedList<Integer> predecessors = routingTableObj.modifiedBinarySearch(deleteHash-1);
    	System.out.println("\n");
    	System.out.println("Deleting node: "+nodeId);
    	System.out.println("Hash range "+ predecessors.get(0)+" - "+ (deleteHash-1) +"added to "+routingTableObj.routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
	}
	
	public void loadBalance(String nodeId, float loadFraction) {
		if(loadFraction>1.0) {
			System.out.println("Move node's start hash range to left side - increase the load");
		}
		else if(loadFraction<1.0) {
			System.out.println("Move node's start hash range to right side - decrease the load");
		}
		else {
			System.out.println("No change in the load");
		}
	}
	
}
