package ring;

import schemes.IDHTScheme;
import java.util.Map;
import java.util.*;

public class RingDHTScheme implements IDHTScheme {
	
	public RingRoutingTable routingTable;
	//public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public Map<Integer, String> physicalTable;
    
    //Constructor
    public RingDHTScheme(String dhtType, byte replicationFactor) {
    	this.routingTable = new RingRoutingTable(dhtType,replicationFactor);
    	this.physicalTable = routingTable.conf.nodesMap; // Node Id --> Ip:port for a datanode
    }
     
    public void printRoutingTable() {
    	System.out.println("HashVal\tNodeId");
    	for (Map.Entry<Integer, Integer> e : routingTable.routingMap.entrySet()) {
    		System.out.print(e.getKey());
    		System.out.print("\t");
    		System.out.println(e.getValue());
    	}
    }
    
    public void printPhysicalTable() {
    	System.out.println("NodeId\tNodeIp_Port");
    	for (Map.Entry<Integer, String> e : physicalTable.entrySet()) {
    		System.out.print(e.getKey());
    		System.out.print("\t");
    		System.out.println(e.getValue());
    	}
    }    
    
    //Bootstrap
    public static void main(String[] args) {
    	Scanner scan = new Scanner(System.in);
    	//System.out.println("Enter DHT Type: Centralized or Distributed");
    	//String dhtType = scan.nextLine();
    	String dhtType = "Centralized";
    	//System.out.println("Enter the replication factor");
    	//byte replicationFactor = scan.nextByte();
    	byte replicationFactor = 3;
    	//Initialize the ring with the existing node details
    	RingDHTScheme ring = new RingDHTScheme(dhtType, replicationFactor);  
    	
    	//printing Routing table which will be kept with each data node 
    	System.out.print("This is the initial routing table which will be available at every data node\n");
    	ring.printRoutingTable();
    	
    	DataNode dNode = new DataNode(ring);
    	
    	scan.close();
    }
}

