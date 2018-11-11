package ring;

import schemes.IDHTScheme;
import java.util.Map;
import java.util.*;

public class RingDHTScheme implements IDHTScheme {
	
	public RingRoutingTable routingTable;
	//public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public Map<Integer, String> physicalTable;
    public byte replicationFactor;
    
    //Constructor
    public RingDHTScheme(String dhtType, byte replicationFactor) {
    	this.routingTable = new RingRoutingTable(dhtType,replicationFactor);
    	this.physicalTable = routingTable.physicalTable; // Node Id --> Ip:port for a datanode
    	this.replicationFactor = replicationFactor;
    }
     
    //Bootstrap
    public static void main(String[] args) {
    	Scanner scan = new Scanner(System.in);
    	//System.out.println("Enter DHT Type: Centralized or Distributed");
    	//String dhtType = scan.nextLine();
    	String dhtType = "Centralized";
    	//System.out.println("Enter the replication factor");
    	//byte replicationFactor = scan.nextByte();
    	byte replicationFactor = 4;
    	//Initialize the ring with the existing node details
    	RingDHTScheme ring = new RingDHTScheme(dhtType, replicationFactor);  
    
    	DataNode dNode = new DataNode(ring);
    	dNode.addNode("192.168.0.101:2050");
    	scan.close();
    }
}

