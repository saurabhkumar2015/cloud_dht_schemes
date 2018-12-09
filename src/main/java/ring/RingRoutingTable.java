package ring;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import common.Commons;
import common.Constants;
import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

import static common.Commons.randomGen;

public class RingRoutingTable implements IRoutingTable,Serializable {

    public long versionNumber;
    public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public Map<Integer, String> physicalTable;
    public int MAX_HASH;
    public int numNodeIds;
    public byte replicationFactor;
    
    public RingRoutingTable(){
    }

    public  void init() {
    	DHTConfig config = ConfigLoader.config;
		this.numNodeIds = config.nodeIdEnd-config.nodeIdStart+1;
		this.versionNumber = config.version;
		this.routingMap = new TreeMap<Integer,Integer>();
		this.physicalTable = config.nodesMap;
		this.replicationFactor = config.replicationFactor;
		this.MAX_HASH= config.bucketSize;
		randomGen = new Random(config.seed);
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
                "versionNumber=" + versionNumber +
                ", routingMap=" + routingMap +
                '}';
    }
    public int randomHashGenerator()
	{
		int low = 1;
		int high = this.MAX_HASH;
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
    	DHTConfig config = ConfigLoader.config;
    	int startNodeId = config.nodeIdStart;
    	int endNodeId = config.nodeIdEnd;
    	for(int s = startNodeId; s<=endNodeId; s++) {
    		int hashVal = randomHashGenerator();
    		//to prevent hash value being overwritten
    		while(this.routingMap.containsKey(hashVal)) {
    			hashVal = randomHashGenerator();
    		}
    		this.routingMap.put(hashVal, s);
    	}
    	this.versionNumber++;
    }

    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public void printRoutingTable() {
    	System.out.println("Routing Table versionNumber: "+this.versionNumber);
        System.out.println("HashRange \t NodeId");
        LinkedList<Integer> hashStart = new LinkedList<>();
        LinkedList<Integer> nodeIdSet = new LinkedList<>();
        for (Map.Entry<Integer, Integer> e : this.routingMap.entrySet()) {
        	hashStart.add(e.getKey());
            //System.out.print(e.getKey());
            //System.out.print("\t");
            //System.out.println(e.getValue());
            nodeIdSet.add(e.getValue());
        }
        int index = 0;
        for (index = 0; index < hashStart.size()-1; index++) {
        	System.out.print(hashStart.get(index)+"-"+(hashStart.get(index+1)-1));
        	System.out.print("\t\t");
        	System.out.println(nodeIdSet.get(index));
        }
        System.out.print(hashStart.get(index)+"-"+(hashStart.get(0)-1));
    	System.out.print("\t\t");
    	System.out.println(nodeIdSet.get(index));
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
    
    public LinkedList<Integer> giveListOfNodes(int index, int replicationFactor){
    	if(this.numNodeIds < replicationFactor) {
    		System.out.println("Warning: Number of nodes in the ring is less than the replication factor");
    		System.out.println("Replication Factor:"+replicationFactor);
    		System.out.println("Number of nodes in the ring: "+this.numNodeIds);
    	}
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
    	//System.out.println("Searching Hash Val in routing table: "+findHashVal);  
    	System.out.println("Searching Hash Val in routing table");  
    	//System.out.println("number of nodes in ring: "+this.numNodeIds);
    	//LinkedList<Integer> listOfAllHash =  new LinkedList<Integer>();
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	/*for(int i=0;i<this.numNodeIds; i++) {
    		listOfHash.add(listOfAllHash.get(i));
    	}*/
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
    		/*
        	System.out.println("List of nodes under consideration now:"+listOfNodesForGivenHash.size());
        	for (int i=0; i<listOfNodesForGivenHash.size();i++) {
        		//System.out.println(i);
        		System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
        	}*/
    		return listOfHashesForGivenHash;
    	}
    	else {
    		int index = binarySearch(findHashVal);
        	if(index!=-1){
        		//System.out.println("index: "+index);
        		listOfHashesForGivenHash= this.giveListOfNodes(index, this.replicationFactor);
        		return listOfHashesForGivenHash;
        	}
        	System.out.println("Returning null");
        	return null;
    	}
    }

    //Find Node corresponding to given filename
    public int giveNodeId(String fileName, int replicationId) {
        int hashVal = Math.abs(fileName.hashCode())%this.MAX_HASH;
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
    	++this.numNodeIds;
    	
    	//Update the predecessor - remove this hash range from predecessor
    	String nodeIp = this.physicalTable.get(routingMap.get(listOfHashesForNewHash.get(0)));
    	//payload consists of new node id - which will trigger node to node communication
    	String payload = String.valueOf(newHash)+"-"+String.valueOf(listOfHashesForNewHash.get(1)-1)+":"+nodeIdInt;
    	
    	int removeNodeId = routingMap.get(listOfHashesForNewHash.get(0));
    	if(removeNodeId == Commons.nodeId)
    		System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " removed from this Node :"+ removeNodeId);
    	else {
			System.out.println("Hash range " + newHash + " - " + (listOfHashesForNewHash.get(1) - 1) + " removed from Node :" + removeNodeId);
		}
    	//hash range will be removed from predecessor and will be added to new node
    	Commons.messageSender.sendMessage(nodeIp, Constants.REMOVE_HASH, payload);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " added to newly added Node :"+ nodeIdInt);
    	
    	// predecessor hash range will be removed from last replica
    	nodeIp = this.physicalTable.get(routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1)));
    	payload = String.valueOf(listOfHashesForNewHash.get(0))+"-"+String.valueOf((newHash-1))+":"+nodeIdInt;
    	int rNodeId = routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1));
    	if(rNodeId == Commons.nodeId)
			System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " removed from same Node :"+ rNodeId);
		else {
			System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " removed from Node :"+ rNodeId);
		}
    	//predecessor hash range will be added to new node (as replica)
    	Commons.messageSender.sendMessage(nodeIp, Constants.REMOVE_HASH, payload);
    	
    	System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " added to newly added Node :"+ nodeIdInt);
    	    	
    	//update physical table
    	//this.routingTableObj.physicalTable.put(newNodeId, nodeId);
    	
    	//update routing map
    	this.routingMap.put(newHash, nodeIdInt);
    	this.versionNumber++;
    	
    	//Print updated Routing Table
    	System.out.println("\n");
    	//System.out.println("Routing Table versionNumber: "+this.versionNumber);
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	System.out.println("\n");

    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    	return this;
    }
	
    public static <T, E> T giveKeyByValue(Map<T, E> map, E value) {
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
		int deleteHash = giveKeyByValue(this.routingMap, nodeIdInt);
		System.out.println("Hash value to be deleted: "+deleteHash);
    	LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(deleteHash);
    	System.out.println("To get predecessor of node getting deleted");
    	LinkedList<Integer> predecessors = modifiedBinarySearch(deleteHash-1);
    	LinkedList<Integer> successors = modifiedBinarySearch(listOfAssociatedHashes.get(1));
    	/*
    	//System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<predecessors.size();i++) {
    		System.out.println("NodeId: "+routingMap.get(predecessors.get(i))+" hashStartValue: "+predecessors.get(i));
    	}*/
    	System.out.println("\n");
    	System.out.println("Deleting node: "+nodeIdInt);
    	
    	//predecessor's hash range to be added in the last replica node
    	int nid = this.routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1));
    	String nodeIp = this.physicalTable.get(nid);
    	String payload = String.valueOf(predecessors.get(0)) +"-"+String.valueOf((deleteHash-1));
    	System.out.println("Predecessor's Hash range "+ payload +" added to deleted Nodes's last replica: "+ nid);
    	Commons.messageSender.sendMessage(nodeIp, Constants.ADD_HASH, payload);
    	
    	//successor to take care of deleted node's hash range - so update last replica of successor
    	int succLastRepNodeId = routingMap.get(successors.get(successors.size()-1));
    	String succLastRepNodeIp = this.physicalTable.get(succLastRepNodeId);
    	payload = String.valueOf((deleteHash))+"-" + String.valueOf((listOfAssociatedHashes.get(1))-1);
    	System.out.println("Deleted Node's Hash range "+ payload +" added to successor's last replica : "+ succLastRepNodeId);
    	Commons.messageSender.sendMessage(succLastRepNodeIp, Constants.ADD_HASH, payload);
    	
    	//update routing map
    	this.routingMap.remove(deleteHash);
    	this.versionNumber++;
    	this.numNodeIds--;
    	System.out.println("\n");
    	//Starting range of deleted node's successor will be changed
    	int succHash = listOfAssociatedHashes.get(1);
    	int succNid = this.routingMap.get(succHash);
    	this.routingMap.remove(succHash);
    	this.routingMap.put(deleteHash, succNid);
    	
    	//System.out.println("Routing Table versionNumber: "+this.versionNumber);
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
		//System.out.println("nodeId to be balanced: "+nodeId);
		int nodeHash = giveKeyByValue(this.routingMap, nodeIdInt);
		LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(nodeHash);
		System.out.println("\n");
		
		int initialTotalHashRang = 0;
		int succHashVal = listOfAssociatedHashes.get(1);
		int myHashVal = listOfAssociatedHashes.get(0);
		LinkedList<Integer> listOfAssociatedHashesForSucc = modifiedBinarySearch(succHashVal);
		
		if (myHashVal > succHashVal){
			initialTotalHashRang = (this.MAX_HASH - myHashVal)+ succHashVal;
		}
		else {
			initialTotalHashRang = listOfAssociatedHashes.get(1)-listOfAssociatedHashes.get(0);
		}
    	System.out.println("Total number of hashes handled so far, by this node is: "+initialTotalHashRang);
		int numOfHashesToBeRemoved = (int) Math.ceil(initialTotalHashRang*(1.0-loadFraction));
		int newStartHashForSucc = listOfAssociatedHashes.get(1)-numOfHashesToBeRemoved; //check for boundary
		if (newStartHashForSucc<0) {
			newStartHashForSucc = this.MAX_HASH+ newStartHashForSucc;
		}
		System.out.println("newStartHash Value for successor will be: "+newStartHashForSucc);
		int oldStartHashForSucc = listOfAssociatedHashes.get(1);
		int succNodeId = this.routingMap.get(oldStartHashForSucc);
		int newEndRangeForThisNode = listOfAssociatedHashes.get(1)-numOfHashesToBeRemoved;
		if(newEndRangeForThisNode<0) {
			newEndRangeForThisNode = this.MAX_HASH+ newEndRangeForThisNode;
		}
		String hashRangeToBeRemoved = String.valueOf(newEndRangeForThisNode+1)+"-"+String.valueOf(listOfAssociatedHashes.get(1)-1);
		String hashRangeToBeRemovedPayload = String.valueOf(newEndRangeForThisNode+1)+"-"+String.valueOf(listOfAssociatedHashes.get(1)-1)+":-1";
		String hashRangeToBeAddedToLastRepPayload = String.valueOf(newEndRangeForThisNode+1)+"-"+String.valueOf(listOfAssociatedHashes.get(1)-1);
		//update routing table for successors
		routingMap.remove(oldStartHashForSucc);
		routingMap.put(newStartHashForSucc, succNodeId);
		
		//message exchange
		System.out.println("Hash range "+ hashRangeToBeRemoved +" removed from node "+nodeIdInt);
    	Commons.messageSender.sendMessage(nodeId, Constants.REMOVE_HASH, hashRangeToBeRemovedPayload);
    	
    	//update last replica of the succ
    	int lastRepHash = listOfAssociatedHashesForSucc.get(listOfAssociatedHashesForSucc.size()-1);
    	int lastRepNodeId = this.routingMap.get(lastRepHash);
    	String lastRepNodeIp = this.physicalTable.get(lastRepNodeId);
    	Commons.messageSender.sendMessage(lastRepNodeIp, Constants.ADD_HASH, hashRangeToBeAddedToLastRepPayload);
    	
    	//increament routing table version
		this.versionNumber++;
		System.out.println("\n");
		System.out.println("Routing Table versionNumber: "+this.versionNumber);
    	//Print updated Routing Table
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	System.out.println("\n");
		return this;
	}

    public List<Integer> giveLiveNodes() {
    	List<Integer> arList = new ArrayList<Integer>();
    	for(Map.Entry<Integer,Integer> map : this.routingMap.entrySet()){
    	     arList.add(map.getValue());
    	}
        return arList;
    }

	public long getVersionNumber() {
		return this.versionNumber;
	}
    
}
