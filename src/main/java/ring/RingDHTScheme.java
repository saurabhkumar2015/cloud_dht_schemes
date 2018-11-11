package ring;

import config.ConfigLoader;
import config.DHTConfig;
import schemes.IDHTScheme;

import java.util.Map;
import java.io.IOException;
import java.util.*;

public class RingDHTScheme implements IDHTScheme {

    public RingRoutingTable routingTable;
    //public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    //public Map<Integer, String> physicalTable;
    //public byte replicationFactor;
    public String configFile;

    //Constructor
    public RingDHTScheme() {

        //this.physicalTable = routingTable.physicalTable; // Node Id --> Ip:port for a datanode
        //this.replicationFactor = replicationFactor;
        try {
            this.routingTable = new RingRoutingTable();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    //Bootstrap
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        //System.out.println("Enter DHT Type: Centralized or Distributed");
        //String dhtType = scan.nextLine();
        DHTConfig config = new DHTConfig();
        config.scheme = "RING";
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "127.0.0.1:4000");
        map.put(2, "127.0.0.1:4001");
        config.nodesMap = map;
        ConfigLoader.config = config;
        //System.out.println("Enter the replication factor");
        //byte replicationFactor = scan.nextByte();
        byte replicationFactor = 4;
        //Initialize the ring with the existing node details
        String configFile = "";
        RingDHTScheme ring = new RingDHTScheme();

        DataNode dNode = new DataNode(ring);
        dNode.addNode(1);
        scan.close();
    }
}
