package ring;
import java.util.*;
import java.util.Map.Entry;

import common.Commons;
import common.Constants;
import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

public class RingRoutingTable implements IRoutingTable {

    public long version;
    public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public DHTConfig conf;
    public Map<Integer, String> physicalTable;
    public int MAX_HASH = 65536;
    public int numNodeIds;
    public byte replicationFactor;
    public static Random randomGen;
    
    public RingRoutingTable(){
    	
        this.conf = ConfigLoader.config;
    	this.numNodeIds = this.conf.nodeIdEnd-this.conf.nodeIdStart+1;
    	this.version = conf.version;
    	this.routingMap = new TreeMap<Integer,Integer>();
    	this.physicalTable = conf.nodesMap;
    	this.replicationFactor = conf.replicationFactor;
    	randomGen = new Random(this.conf.seed);
    	this.populateTables(); 
    	//printing Routing table which will be kept with each data node 
    	System.out.print("This is the initial routing table which will be available at every data node\n");
    	this.printRoutingTable();
    	System.out.print("\n");
    	//System.out.print("This is the initial NodeId-Physical Machine mapping table\n");
    	//this.printPhysicalTable();
    }

    @Override
    public String toString() {
        return "RingRoutingTable{" +
                "version=" + version +
                ", routingMap=" + routingMap +
                '}';
    }
    public int randomHashGenerator()
	{
		int low = 1;
		int high = MAX_HASH;
		int result = randomGen.nextInt(high-low);
		return result;
	}
    /*
    //Hash generator for given string
    public int getHasValueFromIpPort(String ipPort) {
        return Math.abs((ipPort.hashCode())%MAX_HASH);
    }*/

    //initiating physical table and routing map
    public void populateTables() {
    	int startNodeId = this.conf.nodeIdStart;
    	int endNodeId = this.conf.nodeIdEnd;
    	for(int s = startNodeId; s<=endNodeId; s++) {
    		int hashVal = randomHashGenerator();
    		this.routingMap.put(hashVal, s);
    	}
    	this.version++;
    }

    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public void printRoutingTable() {
    	System.out.println("Routing Table version: "+this.version);
        System.out.println("HashVal\tNodeId");
        for (Map.Entry<Integer, Integer> e : this.routingMap.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }

	@Override
	public long getVersionNumber() {
		return 0;
	}

	public void printPhysicalTable() {
        System.out.println("NodeId\tNodeIp_Port");
        for (Map.Entry<Integer, String> e : this.physicalTable.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }

    public int binarySearch(int findHashVal) {
    	//System.out.println("Inside BSearch");
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	listOfHash.addAll(this.routingMap.keySet());
    	int start = 0;
    	int end = this.routingMap.size()-1;

    	while(start<=end) {
    		//System.out.println("start:"+start);
    		//System.out.println("end:"+end);
    		int mid = (start+end)/2;
    		//System.out.println("mid:"+mid);
    		if(listOfHash.get(mid)==findHashVal) {
    			return mid;
    		}
    		else if(listOfHash.get(mid)>findHashVal) {
    			if(listOfHash.get(mid-1)<findHashVal)
    				return mid-1;
    			else {
    				end = mid;
    			}
    				
    		}
    		else {
    			if(listOfHash.get(mid+1)>findHashVal)
    				return mid;
    			else
    				start = mid;
    		}
    	}
    	return -1;
    }
    
    public LinkedList<Integer> getListOfNodes(int index, int replicationFactor){
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	listOfHash.addAll(this.routingMap.keySet());
    	LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
    	LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
    	
    	for (int j=0; j<replicationFactor; j++) {
    		listOfHashesForGivenHash.add(listOfHash.get((index+j)%this.numNodeIds));
    		listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((index+j)%this.numNodeIds)));
    	}
    	/*
    	System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes from binary search:");
    	for (int i=0; i<listOfNodesForGivenHash.size();i++) {
    		System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
    	}
    	*/
    	return listOfHashesForGivenHash;
    }
    
    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public LinkedList<Integer> modifiedBinarySearch(int findHashVal){
    	System.out.println("Searching Hash Val in routing table: "+findHashVal);    	
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	listOfHash.addAll(this.routingMap.keySet());
    	LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
    	LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
    	
    	int end = this.routingMap.size()-1;
    	// checking for - last node's hash range 
    	if(findHashVal >= listOfHash.get(end) || findHashVal < listOfHash.get(0)) {
    		//System.out.println("checking for last node's range");
    		int lastNode = this.routingMap.get(listOfHash.get(end));
    		listOfNodesForGivenHash.add(lastNode);
    		listOfHashesForGivenHash.add(listOfHash.get(end));
    		for (int j = 0 ; j< this.replicationFactor-1; j++) {
    			//System.out.println(j);
    			listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get(j)));
    			listOfHashesForGivenHash.add(listOfHash.get(j));
        	}
    		//System.out.println("\n");
        	//Print List of nodes associated with given hash value
        	System.out.println("List of nodes under consideration now:"+listOfNodesForGivenHash.size());
        	for (int i=0; i<listOfNodesForGivenHash.size();i++) {
        		//System.out.println(i);
        		System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
        	}
    		return listOfHashesForGivenHash;
    	}
    	else {
    		int index = binarySearch(findHashVal);
        	if(index!=-1){
        		//System.out.println("index: "+index);
        		listOfHashesForGivenHash= this.getListOfNodes(index, this.replicationFactor);
        		return listOfHashesForGivenHash;
        	}
        	System.out.println("Returning null");
        	return null;
    	}
    }

    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicationId) {
        int hashVal = fileName.hashCode()%this.MAX_HASH;
        LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
        if (listOfNodesForGivenHash!=null) {
        	if(listOfNodesForGivenHash.size()>=this.replicationFactor) {
        		//System.out.println(this.routingMap.get(listOfNodesForGivenHash.get(replicationId-1)));
        		return this.routingMap.get(listOfNodesForGivenHash.get(replicationId-1));
        	}
        	else
        		return -1;
        }
        return -1;
    }
    
    
    //nodeId = ip:port
    public IRoutingTable addNode(int nodeIdInt) {
    	System.out.println("\n");
    	//String nodeId = physicalTable.get(nodeIdInt);
    	System.out.println("Adding new node: "+nodeIdInt);
    	int newHash = randomHashGenerator();
    	System.out.println("New node's hash value: "+newHash);
    	//int newHash = getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfHashesForNewHash = modifiedBinarySearch(newHash);
    	/*
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfHashesForNewHash.size()-1;i++) {
    		System.out.println("NodeId: "+routingMap.get(listOfHashesForNewHash.get(i))+" hashStartValue: "+listOfHashesForNewHash.get(i));
    	}
    	*/
    	String nodeIp = this.physicalTable.get(routingMap.get(listOfHashesForNewHash.get(0)));
    	String payload = String.valueOf(newHash)+"-"+String.valueOf(listOfHashesForNewHash.get(1)-1);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " removed from Node :"+ routingMap.get(listOfHashesForNewHash.get(0)));
    	Commons.messageSender.sendMessage(nodeIp, Constants.REMOVE_HASH, payload);
    	
    	nodeIp = this.physicalTable.get(routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1)));
    	payload = String.valueOf(listOfHashesForNewHash.get(0))+"-"+String.valueOf((newHash-1));
    	System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " removed from Node :"+ routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1)));
    	Commons.messageSender.sendMessage(nodeIp, Constants.REMOVE_HASH, payload);
    	
    	int newNodeId = ++this.numNodeIds;
    	nodeIp = this.physicalTable.get(newNodeId);
    	payload = String.valueOf(newHash)+"-"+ (listOfHashesForNewHash.get(1)-1);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " added to Node :"+ newNodeId);
    	Commons.messageSender.sendMessage(nodeIp, Constants.ADD_HASH, payload);
    	
    	//update physical table
    	//this.routingTableObj.physicalTable.put(newNodeId, nodeId);
    	
    	//update routing map
    	this.routingMap.put(newHash, newNodeId);
    	this.version++;
    	
    	//Print updated Routing Table
    	System.out.println("\n");
    	//System.out.println("Routing Table version: "+this.version);
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	System.out.println("\n");

    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    	return this;
    }
	
    public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }
    
	public IRoutingTable deleteNode(int nodeIdInt) {
		String nodeId = physicalTable.get(nodeIdInt);
		System.out.println("nodeId to be deleted: "+nodeId);
		int deleteHash = getKeyByValue(this.routingMap, nodeIdInt);
		System.out.println("Hash value to be deleted: "+deleteHash);
    	LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(deleteHash);
    	System.out.println("To get predecessor of node getting deleted");
    	LinkedList<Integer> predecessors = modifiedBinarySearch(deleteHash-1);
    	/*
    	//System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<predecessors.size();i++) {
    		System.out.println("NodeId: "+routingMap.get(predecessors.get(i))+" hashStartValue: "+predecessors.get(i));
    	}*/
    	System.out.println("\n");
    	System.out.println("Deleting node: "+nodeIdInt);
    	int nid = routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1));
    	String nodeIp = this.physicalTable.get(nid);
    	String payload = String.valueOf(predecessors.get(0)) +"-"+String.valueOf((deleteHash-1));
    	System.out.println("Hash range "+ predecessors.get(0)+" - "+ (deleteHash-1) +" added to "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
    	Commons.messageSender.sendMessage(nodeIp, Constants.ADD_HASH, payload);
    	
    	//update routing map
    	this.routingMap.remove(deleteHash);
    	this.version++;
    	this.numNodeIds--;
    	System.out.println("\n");
    	//System.out.println("Routing Table version: "+this.version);
    	//Print updated Routing Table
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	System.out.println("\n");
    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    	return this;
	}
	
	public IRoutingTable loadBalance(int nodeIdInt, double loadFraction) {
		String nodeId = physicalTable.get(nodeIdInt);
		System.out.println("nodeId to be balanced: "+nodeId);
		int nodeHash = getKeyByValue(this.routingMap, nodeIdInt);
		LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(nodeHash-1);
		/*
		//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfAssociatedHashes.size();i++) {
    		System.out.println("NodeId: "+routingMap.get(listOfAssociatedHashes.get(i))+" hashStartValue: "+listOfAssociatedHashes.get(i));
    	}
    	*/
		if(loadFraction>1.0 && loadFraction<2.0) {
			System.out.println("\n");
			System.out.println("Moving node's start hash range to left side - increasing the load");
			int initialTotalHashRang = listOfAssociatedHashes.get(1)-listOfAssociatedHashes.get(0);
	    	System.out.println("Total number of hashes handled so far, by predecessor: "+initialTotalHashRang);
			int numOfHashesToBeAdded = (int) Math.ceil(initialTotalHashRang*(loadFraction-1.0));
			int newStartHash = listOfAssociatedHashes.get(1)-numOfHashesToBeAdded;
			System.out.println("newStartHash Value will be: "+newStartHash);
			//update routing table
			routingMap.remove(nodeHash);
			routingMap.put(newStartHash, nodeIdInt);
	    	//What all nodes will be updates
	    	System.out.println("Hash range "+ newStartHash +" - "+ (nodeHash-1) +" added to "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
	    	this.version++;
			System.out.println("\n");
			System.out.println("Routing Table version: "+this.version);
	    	//Print updated Routing Table
	    	System.out.println("New Routing Map after new node added");
	    	printRoutingTable();
	    	System.out.println("\n");
		}
		else if(loadFraction<1.0) {
			System.out.println("\n");
			System.out.println("Moving node's start hash range to right side - decreaseing the load");
			int initialTotalHashRang = listOfAssociatedHashes.get(2)-listOfAssociatedHashes.get(1);
	    	System.out.println("Total number of hashes handled so far, by predecessor: "+initialTotalHashRang);
			int numOfHashesToBeRemoved = (int) Math.ceil(initialTotalHashRang*(1.0-loadFraction));
			int newStartHash = listOfAssociatedHashes.get(1)+numOfHashesToBeRemoved;
			System.out.println("newStartHash Value will be: "+newStartHash);
			//update routing table
			routingMap.remove(nodeHash);
			routingMap.put(newStartHash, nodeIdInt);
			
	    	//What all nodes will be updates
	    	System.out.println("Hash range "+ nodeHash + " - "+ (newStartHash-1) +" added to NodeId "+routingMap.get(listOfAssociatedHashes.get(0)));
	    	System.out.println("Hash range "+ nodeHash + " - "+ (newStartHash-1) +" removed from NodeId "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
	    	this.version++;
			System.out.println("\n");
			System.out.println("Routing Table version: "+this.version);
	    	//Print updated Routing Table
	    	System.out.println("New Routing Map after new node added");
	    	printRoutingTable();
	    	System.out.println("\n");
		}
		
		else {
			System.out.println("\n");
			System.out.println("No change in the load");
		}
		return this;
	}

    @Override
    public List<Integer> getLiveNodes() {
        return null;
    }
}
