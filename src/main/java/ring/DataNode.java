package ring;

import java.util.*;

public class DataNode implements IDataNode {
	
	RingRoutingTable routingTableObj;
    
    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTable;
    }

	@Override
	public void writeFile(String fileName, String replicaId) {
		System.out.println("Got write request "+ fileName + "with replica ID" + replicaId );
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
