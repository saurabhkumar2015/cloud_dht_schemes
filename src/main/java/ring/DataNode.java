package ring;

import java.util.*;

import ring.IDataNode;
import ring.RingDHTScheme;

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
		
	}
	
	public void loadBalance(String nodeId, float loadFraction) {
		
	}
	
}
