package ring;

import schemes.IDHTScheme;

import java.util.Map;
import java.io.IOException;
import java.util.*;
import config.ConfigLoader;
import config.DHTConfig;

public class RingDHTScheme implements IDHTScheme {
	
	public RingRoutingTable routingTableObj;
	
    public RingDHTScheme() {
    	try {
			this.routingTableObj = new RingRoutingTable();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    //Bootstrap
    public static void main(String[] args) {
    	Scanner scan = new Scanner(System.in);
    	//System.out.println("Enter DHT Type: Centralized or Distributed");
    	//String dhtType = scan.nextLine();
    	
    	//System.out.println("Enter the replication factor");
    	//byte replicationFactor = scan.nextByte();
    
    	DHTConfig config = new DHTConfig();
        config.scheme = "RING";
        config.dhtType = "Centralized";
        config.replicationFactor = 4;
        
        //Populate physical node table from csv file
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1,"192.168.0.1:2000");
        map.put(2,"192.168.0.1:2010");
        map.put(3,"192.168.0.2:2020");
        map.put(4,"192.168.0.2:2030");
        map.put(5,"192.168.0.3:2040");
        map.put(6,"192.168.0.3:2000");
        map.put(7,"192.168.0.4:2010");
        map.put(8,"192.168.0.4:2020");
        map.put(9,"192.168.0.5:2030");
        map.put(10,"192.168.0.5:2040");
        map.put(11,"192.168.0.6:2000");
        map.put(12,"192.168.0.6:2010");
        map.put(13,"192.168.0.7:2020");
        map.put(14,"192.168.0.8:2030");
        map.put(15,"192.168.0.9:2040");
        map.put(16,"192.168.0.9:2000");
        map.put(17,"192.168.0.10:2010");
        map.put(18,"192.168.0.10:2020");
        map.put(19,"192.168.0.11:2030");
        map.put(20,"192.168.0.11:2040");
        map.put(21,"192.168.0.101:2050");
        map.put(22,"192.168.0.101:2050");
        config.nodesMap = map;
        
        ConfigLoader.config = config;
        
        RingDHTScheme ring = new RingDHTScheme();
        
    	DataNode dNode = new DataNode(ring);
    	dNode.addNode(21);
    	dNode.deleteNode(5);
    	scan.close();
    }
}
