package ring;
import java.util.*;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

public class RingRoutingTable implements IRoutingTable {

    public long version;
    public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public DHTConfig conf;
    public Map<Integer, String> physicalTable;
    private static final int MAX_HASH = 2013265907;
    public int numNodeIds;
    public byte replicationFactor;
    
    public RingRoutingTable() {
        this.conf = ConfigLoader.config;
    	this.numNodeIds = this.conf.nodeIdEnd-this.conf.nodeIdStart+1;
    	this.version = conf.version;
    	this.routingMap = new TreeMap<Integer,Integer>();
    	this.physicalTable = conf.nodesMap;
    	this.replicationFactor = conf.replicationFactor;
    	this.populateTables(); 
    	//printing Routing table which will be kept with each data node 
    	System.out.print("This is the initial routing table which will be available at every data node\n");
    	this.printRoutingTable();
    	System.out.print("\n");
    	System.out.print("This is the initial NodeId-Physical Machine mapping table\n");
    	this.printPhysicalTable();
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
    	int startNodeId = this.conf.nodeIdStart;
    	int endNodeId = this.conf.nodeIdEnd;
    	int totalNodesInRT = 0;
    	for(int s = startNodeId; s<=endNodeId; s++) {
    		String ipPort = this.conf.nodesMap.get(s);
    		int hashVal = this.getHasValueFromIpPort(ipPort);
    		this.routingMap.put(hashVal, s);
    		totalNodesInRT++;
    	}
    }

    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public void printRoutingTable() {
        System.out.println("HashVal\tNodeId");
        for (Map.Entry<Integer, Integer> e : this.routingMap.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }

    public void printPhysicalTable() {
        System.out.println("NodeId\tNodeIp_Port");
        for (Map.Entry<Integer, String> e : this.physicalTable.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }


    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public LinkedList<Integer> modifiedBinarySearch(int findHashVal){
    	System.out.println("Searching Hash Val: "+findHashVal);
    	
    	LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
    	listOfHash.addAll(this.routingMap.keySet());
    	LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
    	LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
    	int start = 0;
    	int end = this.routingMap.size()-1;
	int originalSize = end;
    	int returnHashValIndex = 0;
    	boolean nodeFound = false;
    	while(start<=end) {
    		int mid = (start+end)/2;
    		int midVal = listOfHash.get(mid);
    		if(midVal==findHashVal) {
    			System.out.println("found hash: "+midVal);
    			listOfNodesForGivenHash.add(this.routingMap.get(midVal));
    			listOfHashesForGivenHash.add(midVal);
    			returnHashValIndex = mid;
    			nodeFound = true;
    			break;
    		}
    		else if(midVal>findHashVal) {
    			//System.out.println("first half");
    			end = mid-1;
    			int nextVal = listOfHash.get(end);
    			if(nextVal<findHashVal) {
        			System.out.println("found hash: "+nextVal);
        			listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
        			listOfHashesForGivenHash.add(nextVal);
        			returnHashValIndex = end;
        			break;
        		}
    		}
    		else {
    			//System.out.println("second half");
    			start = mid+1;
			if(start < originalSize){
    			int nextVal = listOfHash.get(start);
    			if(nextVal>findHashVal) {
        			System.out.println("found hash: "+midVal);
        			listOfNodesForGivenHash.add(this.routingMap.get(midVal));
        			listOfHashesForGivenHash.add(midVal);
        			returnHashValIndex = start;
        			break;
        		}
			}
    		}
    	}
    	if(start>end){
		return null;
	}
    	//add successors
    	//System.out.println("returnHashValIndex: "+returnHashValIndex);
    	int replicationStartIndex = 0;
    	if(nodeFound)
    		replicationStartIndex=1;
    	else
    		replicationStartIndex = 0;
    	
    	for (int j = replicationStartIndex ; j< this.replicationFactor; j++) {
    		listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((returnHashValIndex+j)%this.numNodeIds)));
			listOfHashesForGivenHash.add(listOfHash.get((returnHashValIndex+j)%this.numNodeIds));
    	}
    	/*
    	System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfNodesForGivenHash.size();i++) {
    		System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
    	}
    	*/
    	//return listOfNodesForGivenHash;
    	return listOfHashesForGivenHash;

    }

    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicationId) {
        int hashVal = this.getHasValueFromIpPort(fileName);
        LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
	if(listOfNodesForGivenHash!=null){
	    if(listOfNodesForGivenHash.size()==this.replicationFactor)
		    return listOfNodesForGivenHash.get(replicationId-1);
	    else
		    return -1;
	}
        return -1;
    }
    
    //nodeId = ip:port
    public IRoutingTable addNode(int nodeIdInt) {
    	String nodeId = physicalTable.get(nodeIdInt); 
    	int newHash = getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfHashesForNewHash = modifiedBinarySearch(newHash);
    	System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfHashesForNewHash.size()-1;i++) {
    		System.out.println("NodeId: "+routingMap.get(listOfHashesForNewHash.get(i))+" hashStartValue: "+listOfHashesForNewHash.get(i));
    	}
    	
    	System.out.println("\n");
    	System.out.println("Adding new node: "+nodeId);
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " removed from Node :"+ routingMap.get(listOfHashesForNewHash.get(0)));
    	
    	System.out.println("Hash range "+ listOfHashesForNewHash.get(0)+" - "+(newHash-1)+ " removed from Node :"+ routingMap.get(listOfHashesForNewHash.get(listOfHashesForNewHash.size()-1)));
    	int newNodeId = ++this.numNodeIds;
    	System.out.println("Hash range "+ newHash +" - "+(listOfHashesForNewHash.get(1)-1)+ " added to Node :"+ newNodeId);
    	
    	//update physical table
    	//this.routingTableObj.physicalTable.put(newNodeId, nodeId);
    	
    	//update routing map
    	this.routingMap.put(newHash, newNodeId);
    	this.version++;
    	
    	//Print updated Routing Table
    	System.out.println("\n");
    	System.out.println("Routing Table version: "+this.version);
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	
    	//System.out.println("\n");
    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    	return this;
    }
	
	public IRoutingTable deleteNode(int nodeIdInt) {
		String nodeId = physicalTable.get(nodeIdInt);
		int deleteHash = getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(deleteHash);
    	System.out.println("To get predecessor of node getting deleted");
    	LinkedList<Integer> predecessors = modifiedBinarySearch(deleteHash-1);
    	System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<predecessors.size();i++) {
    		System.out.println("NodeId: "+routingMap.get(predecessors.get(i))+" hashStartValue: "+predecessors.get(i));
    	}
    	System.out.println("\n");
    	System.out.println("Deleting node: "+nodeId);
    	System.out.println("Hash range "+ predecessors.get(0)+" - "+ (deleteHash-1) +"added to "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
    	
    	//update routing map
    	this.routingMap.remove(deleteHash);
    	this.version++;
    	
    	System.out.println("\n");
    	System.out.println("Routing Table version: "+this.version);
    	//Print updated Routing Table
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	
    	//System.out.println("\n");
    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    	return this;
	}
	
	public IRoutingTable loadBalance(int nodeIdInt, double loadFraction) {
		String nodeId = physicalTable.get(nodeIdInt);
		int nodeHash = getHasValueFromIpPort(nodeId);
		LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(nodeHash-1);
		//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfAssociatedHashes.size();i++) {
    		System.out.println("NodeId: "+routingMap.get(listOfAssociatedHashes.get(i))+" hashStartValue: "+listOfAssociatedHashes.get(i));
    	}
    	
		if(loadFraction>1.0) {
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
			this.version++;
			System.out.println("\n");
			System.out.println("Routing Table version: "+this.version);
	    	//Print updated Routing Table
	    	System.out.println("New Routing Map after new node added");
	    	printRoutingTable();
	    	//What all nodes will be updates
	    	System.out.println("Hash range "+ newStartHash +" - "+ (nodeHash-1) +" added to "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
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
			this.version++;
			System.out.println("\n");
			System.out.println("Routing Table version: "+this.version);
	    	//Print updated Routing Table
	    	System.out.println("New Routing Map after new node added");
	    	printRoutingTable();
	    	//What all nodes will be updates
	    	System.out.println("Hash range "+ nodeHash + " - "+ (newStartHash-1) +" added to NodeId "+routingMap.get(listOfAssociatedHashes.get(0)));
	    	System.out.println("Hash range "+ nodeHash + " - "+ (newStartHash-1) +" removed from NodeId "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
		}
		
		else {
			System.out.println("\n");
			System.out.println("No change in the load");
		}
		return this;
	}
}
