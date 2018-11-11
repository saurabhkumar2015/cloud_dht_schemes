package ring;

import java.util.*;

public class DataNode implements IDataNode {
	
	RingRoutingTable routingTableObj;
    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTableObj;
    }
    
    //nodeId = ip:port
    public void addNode(int nodeId) {
    	routingTableObj.addNode(nodeId);
    }
	
	public void deleteNode(int nodeId) {
		routingTableObj.deleteNode(nodeId);
	}
	
	public void loadBalance(int nodeId, double loadFraction) {
		routingTableObj.loadBalance(nodeId, loadFraction);
	}
	
}
