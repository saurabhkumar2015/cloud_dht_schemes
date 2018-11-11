package ring;
import java.io.*;
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
    
    public RingRoutingTable() throws IOException {
        //ConfigLoader.init(configFile);
        this.conf = ConfigLoader.config;
    	//this.conf.scheme = "RING";
    	//this.conf.dhtType = dhtType;
    	this.numNodeIds = this.conf.nodeIdEnd-this.conf.nodeIdStart+1;
    	this.version = conf.version;
    	this.routingMap = new TreeMap<Integer,Integer>();
    	//this.physicalTable = new HashMap<Integer,String>();
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
    	/*
        try {
        	
            for ( Map.Entry<Integer, String> e : this.conf.nodesMap.entrySet())
            {
                //this.physicalTable.put(e.getKey(), e.getValue());
                //Generate random hash for every IP:Port
                int hashVal = this.getHasValueFromIpPort(e.getValue());
                this.routingMap.put(hashVal, e.getKey());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //this.numNodeIds = nodeId;
        //System.out.println(this.getNodeId("asddfr3rgerg",3));
         * 
         */
    }
    
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
    	
    	//add successors
    	//System.out.println("returnHashValIndex: "+returnHashValIndex);
    	int replicationStartIndex = 0;
    	if(nodeFound)
    		replicationStartIndex=1;
    	else
    		replicationStartIndex = 0;
    	
    	for (int j = replicationStartIndex ; j<= this.replicationFactor-1; j++) {
    		listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((returnHashValIndex+j)%this.numNodeIds)));
			listOfHashesForGivenHash.add(listOfHash.get((returnHashValIndex+j)%this.numNodeIds));
    	}
    	System.out.println("\n");
    	//Print List of nodes associated with given hash value
    	System.out.println("List of nodes under consideration now:");
    	for (int i=0; i<listOfNodesForGivenHash.size();i++) {
    		System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
    	}
    	
    	//return listOfNodesForGivenHash;
    	return listOfHashesForGivenHash;
    }

    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicaId) {
    	int hashVal = this.getHasValueFromIpPort(fileName);
    	LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
    	return listOfNodesForGivenHash.get(replicaId-1);
    }
    
    //nodeId = ip:port
    public void addNode(int nodeIdInt) {
    	String nodeId = physicalTable.get(nodeIdInt); 
    	int newHash = getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfHashesForNewHash = modifiedBinarySearch(newHash);
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
    	
    	//Print updated Routing Table
    	System.out.println("\n");
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	
    	//System.out.println("\n");
    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
    }
	
	public void deleteNode(int nodeIdInt) {
		String nodeId = physicalTable.get(nodeIdInt);
		int deleteHash = getHasValueFromIpPort(nodeId);
    	LinkedList<Integer> listOfAssociatedHashes = modifiedBinarySearch(deleteHash);
    	System.out.println("To get predecessor of node getting deleted");
    	LinkedList<Integer> predecessors = modifiedBinarySearch(deleteHash-1);
    	System.out.println("\n");
    	System.out.println("Deleting node: "+nodeId);
    	System.out.println("Hash range "+ predecessors.get(0)+" - "+ (deleteHash-1) +"added to "+routingMap.get(listOfAssociatedHashes.get(listOfAssociatedHashes.size()-1)));
    	
    	//update routing map
    	this.routingMap.remove(deleteHash);
    	
    	System.out.println("\n");
    	//Print updated Routing Table
    	System.out.println("New Routing Map after new node added");
    	printRoutingTable();
    	
    	//System.out.println("\n");
    	//System.out.println("New NodeId - PhysicalNode mapping after new node added");
    	//routingTableObj.printPhysicalTable();
	}
	
	public void loadBalance(int nodeIdInt, double loadFraction) {
		String nodeId = physicalTable.get(nodeIdInt);
		if(loadFraction>1.0) {
			System.out.println("Move node's start hash range to left side - increase the load");
		}
		else if(loadFraction<1.0) {
			System.out.println("Move node's start hash range to right side - decrease the load");
		}
		else {
			System.out.println("No change in the load");
		}
	}

}