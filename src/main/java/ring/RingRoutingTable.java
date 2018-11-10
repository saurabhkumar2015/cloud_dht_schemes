package ring;
import java.io.*;
import java.util.*;
import config.DHTConfig;
public class RingRoutingTable {

    public long version;
    public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public DHTConfig conf;
    public Map<Integer, String> physicalTable;
    private static final int MAX_HASH = 2013265907;
    
    //Constructor
    public RingRoutingTable(String dhtType, byte replicationFactor){
    	this.conf = new DHTConfig();
    	this.conf.scheme = "RING";
    	this.conf.dhtType = dhtType;

    	this.version = conf.version;
    	this.routingMap = new TreeMap<Integer,Integer>();
    	this.physicalTable = this.conf.nodesMap;
    	this.populateTables(); 
    	
    }
    
    @Override
    public String toString() {
        return "RingRoutingTable{" +
                "version=" + version +
                ", routingMap=" + routingMap +
                '}';
    }
    
    //Hash generator for given string
    public int getHasValueFromIpPort(String ipPort) {
    	return Math.abs((ipPort.hashCode())%MAX_HASH);
    }
    
    //initiating physical table and routing map
    public void populateTables() {
    	int nodeId =0;
    	try {
    		BufferedReader reader = new BufferedReader(new FileReader(this.conf.nodeMapLocation));
    	    String line;
    	    while ((line = reader.readLine()) != null) {
    	    	nodeId+=1;
    	    	this.physicalTable.put(nodeId, line);
    	    	//Generate random hash for every IP:Port
    	    	int hashVal = this.getHasValueFromIpPort(line);
    	    	this.routingMap.put(hashVal, nodeId);
    	    }
    	    reader.close();
    	  }
    	  catch (Exception e) {
    	    System.err.format("Exception occurred trying to read '%s'.", this.conf.nodeMapLocation);
    	    e.printStackTrace();
    	  }
    	this.conf.numNodeIds = nodeId;
    	System.out.println(this.getNodeId("asddfr3rgerg",3));
    }
    
    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */  
    public LinkedList<Integer> modifiedBinarySearch(int findHashVal){
    	System.out.println(findHashVal);
    	
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	listOfHash.addAll(this.routingMap.keySet());
    	LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
    	LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
    	int start = 0;
    	int end = this.routingMap.size()-1;
    	
    	while(start<=end) {
    		
    		int mid = (start+end)/2;
    		//System.out.println("start"+start);
			//System.out.println("end"+end);
			//System.out.println("mid"+mid);
    		int midVal = listOfHash.get(mid);
    		if(midVal==findHashVal) {
    			System.out.println("found hash"+midVal);
    			listOfNodesForGivenHash.add(this.routingMap.get(midVal));
    			listOfHashesForGivenHash.add(midVal);
    			//add successors
    			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid+1)%this.conf.numNodeIds)));
    			listOfHashesForGivenHash.add(listOfHash.get((mid+1)%this.conf.numNodeIds));
    			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid+2)%this.conf.numNodeIds)));
    			listOfHashesForGivenHash.add(listOfHash.get((mid+2)%this.conf.numNodeIds));
    			break;
    		}
    		else if(midVal>findHashVal) {
    			//System.out.println("first half");
    			end = mid-1;
    			int nextVal = listOfHash.get(end);
    			if(nextVal<=findHashVal) {
        			System.out.println("found hash"+nextVal);
        			listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
        			listOfHashesForGivenHash.add(nextVal);
        			//add successors
        			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get(mid)));
        			listOfHashesForGivenHash.add(listOfHash.get(mid));
        			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid+1)%this.conf.numNodeIds)));
        			listOfHashesForGivenHash.add(listOfHash.get((mid+1)%this.conf.numNodeIds));
        			break;
        		}
    		}
    		else {
    			//System.out.println("second half");
    			start = mid;
    			int nextVal = listOfHash.get(start);
    			if(nextVal>=findHashVal) {
        			System.out.println("found hash"+nextVal);
        			listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
        			listOfHashesForGivenHash.add(listOfHash.get(nextVal));
        			//add successors
        			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get(mid)));
        			listOfHashesForGivenHash.add(listOfHash.get(mid));
        			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid+1)%this.conf.numNodeIds)));
        			listOfHashesForGivenHash.add(listOfHash.get((mid+1)%this.conf.numNodeIds));
        			break;
        		}
    		}
    	}
    	//List of nodes associated with given hash value
    	
    	for(int i: listOfNodesForGivenHash){
    		System.out.println("node: "+i);
    	}
    	
    	for(int hash: listOfHashesForGivenHash){
    		System.out.println("hash: "+hash);
    	}
    	
    	
    	//return listOfNodesForGivenHash;
    	return listOfHashesForGivenHash;
    }
    
    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicationId) {
    	int hashVal = this.getHasValueFromIpPort(fileName);
    	//hashVal = 90295461;
    	LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
    	return listOfNodesForGivenHash.get(replicationId-1);
    	
    }
}
