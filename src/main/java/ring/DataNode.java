package ring;

import java.util.*;

import Ring.IDataNode;
import Ring.RingDHTScheme;

public class DataNode implements IDataNode {
	
	RingRoutingTable routingTableObj;
    
    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTable;
    }
    //nodeId = ip:port
    public void addNode(String nodeId) {
    	int newHash = routingTableObj.getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfHashesForNewHash = routingTableObj.modifiedBinarySearch(newHash);
    	
    	
    }
	
	public void deleteNode(String nodeId) {
		
	}
	
	public void loadBalance(String nodeId, float loadFraction) {
		
	}
	
}
