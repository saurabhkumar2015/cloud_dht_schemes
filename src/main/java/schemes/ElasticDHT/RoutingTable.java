package schemes.ElasticDHT;

import common.IRoutingTable;

import config.ConfigLoader;
import config.DHTConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

public class RoutingTable implements IRoutingTable{

	public static   ElasticRoutingTableInstance[] elasticTable  = new ElasticRoutingTableInstance[100];
	
	public static RoutingTable single_instance = null;

	DHTConfig config = ConfigLoader.config;
	int size = config.bucketSize;
	Set<Integer> liveNodes = new HashSet<Integer>();
	Map<Integer,Integer> hashReplicaNodeId = new HashMap<Integer,Integer>();
	
	public RoutingTable()
	{
		
	}
	
	// Make singleton Instance of routing table 
	public static RoutingTable GetInstance()
	{
		if (single_instance == null) 
            single_instance = new RoutingTable(); 
  
        return single_instance; 
	}
    
	public ElasticRoutingTableInstance[] getRoutingTable()
	{
		ElasticRoutingTable elasticTable1 = new ElasticRoutingTable();
		 elasticTable = elasticTable1.populateRoutingTable();
		 
		 return elasticTable;
	}
	
	//@SuppressWarnings("unchecked")
/*	public IRoutingTable addNode(int clusterId, int nodeId)
	{
		Random rno =  new Random(config.seed);
		int noOfHashIndices = rno.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		int mainIndex = 0;//The number of hash values for which we change the node Id.
		for(int i = 0;i<noOfHashIndices;i++) {
			Random rno1 = new Random();
			 mainIndex = rno1.nextInt(noOfHashIndices); // For which hashIndex, we want
			int subIndex = rno1.nextInt(config.replicationFactor)+1;
			elasticTable[mainIndex].nodeId.set(subIndex, nodeId);
			
			
		}
		return null;
	}
		//System.out.print(elasticTable[mainIndex].hashIndex);
		//System.out.print(elasticTable[mainIndex].nodeId1);
		//System.out.print(elasticTable[mainIndex].nodeId2);
		//System.out.print( elasticTable[mainIndex].nodeId3);
		
		// Implement this



*/
	
	public IRoutingTable deleteNode(int nodeId)
	{
		System.out.println("Entering delete functions");
		int replaceNodeId = 0;
		Random rn = new Random();
		replaceNodeId = rn.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		while(replaceNodeId==nodeId) {
			replaceNodeId = rn.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		}
		System.out.println(replaceNodeId);
		for(int i = 0;i<size;i++) {
			
			int  k  = check(i,nodeId);
			if(k!=-1) {
				elasticTable[i].nodeId.set(k, replaceNodeId);
				System.out.println("Deleting and replacing with : "+replaceNodeId);
				System.out.println( "New table : "+elasticTable[i].hashIndex+" "+elasticTable[i].nodeId.get(k));
			}
			

			
			// check if nodeId is in hash and get that index
		}
		
	}
	

	

	public boolean Resize()
	{
		//Create new elasticTbaletemp and reassign to it.
		//Ratio based resize factor hashbuckets/live nodes> read the resize factor config.
		// Implement this
		return false;
	}
	public int getNodeId(String filename, int replicaId) {
		 int code = filename.hashCode()%1024;
		 System.out.println(code);
		 // May not be the temporary hash code I have
		 int nodeId = 0;
		 
		 for(int k = 0;k<elasticTable.length;k++) {
			 if(elasticTable[k].hashIndex==code) {
				nodeId = elasticTable[k].nodeId.get(replicaId-1);
			 System.out.println("In function");
			 }
		 }
		 return nodeId;
	}

	//public void addNode(int nodeId) throws IOException {
	//	return null;
	//}
// Get live nodes and choose randomly
	//Map for hash and replica 
	//Load balance
	// Drop inverted index, <hash,replica> for nodeId in consideration  map for a node Id, Iterate over it, 
	@SuppressWarnings("unchecked")
	public IRoutingTable loadBalance(int nodeId, double factor) {
		// Get strength of current nodeId
		//Randomly choose replaceNodeId
		//
		liveNodes = getLiveNodes(elasticTable);
		int maxLiveNodes = liveNodes.size();
		System.out.println(maxLiveNodes);
		hashReplicaNodeId = createMapforNodeId(nodeId,elasticTable);
		Set<Integer> keys = hashReplicaNodeId.keySet();
		Iterator<Integer> keyIterator = keys.iterator();
		int currentstrength = hashReplicaNodeId.size();
		int newStrength = (int) (currentstrength*factor);
		System.out.println("Current Strength = "+currentstrength+" New Strength = "+newStrength);
		Random rn = new Random();
		int deleteNodes = currentstrength -newStrength;
		Object[] liveArray = liveNodes.toArray();
		
		for(Entry<Integer, Integer> e : hashReplicaNodeId.entrySet()) {
			if(deleteNodes <= 0) break;
			
			int nodeIndex = rn.nextInt(maxLiveNodes-1);
			while(nodeId == (Integer)liveArray[nodeIndex]) {
				 nodeIndex = rn.nextInt(maxLiveNodes-1);

			}
			elasticTable[e.getKey()].nodeId.set(e.getValue(), (Integer)liveArray[nodeIndex]);
			System.out.println(deleteNodes+":File moved from "+nodeId +"to "+liveArray[nodeIndex]);
			//System.out.println(elasticTable[e.getKey()]+ "  :"+ elasticTable[e.getKey()].nodeId.get(e.getValue()));
			deleteNodes--;
	
		}
		System.out.println("Load balanced");
		
	}
	public Set<Integer> getLiveNodes(ElasticRoutingTableInstance rt []){
		int tempNode;
		for(int i = 0;i<config.bucketSize;i++) {
			for(int j = 0;j<3;j++) {
				
				tempNode = elasticTable[i].nodeId.get(j);				
					liveNodes.add(tempNode);
				
				
			}
		}
		return liveNodes;
	
	}
	public Map<Integer,Integer> createMapforNodeId(int nodeId,ElasticRoutingTableInstance rt[]){
		for(int i = 0;i<config.bucketSize;i++) {
			for(int j = 0;j<3;j++) {
				if(nodeId==rt[i].nodeId.get(j)) {
					hashReplicaNodeId.put(rt[i].hashIndex, j);
				}
			}
			
		}
	
		return hashReplicaNodeId;
	}
	
	

	
	private boolean unique(int tempNode, int i,int j) {
		boolean b =  true;
		if(i==0) {
			b = true;
		}
		else {
			for(int k = 0;k<i;k++) {
				if(elasticTable[k].nodeId.get(j)==tempNode) {
					b = false;
					
				}
			}
		}
		// TODO Auto-generated method stub
		return false;
	}

	int check(int index, int nodeId) {
		int i = 0;
		boolean b =false;
		if(index == 401) {
			System.out.println("" );
		}
		
		for( i = 0;i<3;i++) {
			if(elasticTable[index].nodeId.get(i)==nodeId) {
				b= true;
				break;
			}
		}
		if(b==true) {
			return i;
		}
		else {
			return -1;
		}
		
	}
	public static void main(String arg[]) {
		RoutingTable r =  new RoutingTable();
		elasticTable = r.getRoutingTable();
//		r.addNode(5);
	}

	@SuppressWarnings("unchecked")
	public IRoutingTable addNode(int nodeId) {
		Random rno =  new Random();
		int noOfHashIndices = rno.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		int  interval = config.bucketSize % noOfHashIndices;
		int count =0;
		int mainIndex = 0;//The number of hash values for which we change the node Id.
		for(int i = interval;i<config.bucketSize;i+=interval) {
			count++;
			
			Random rno1 = new Random();
			 mainIndex = i;
			 // For which hashIndex, we want
			int subIndex = rno1.nextInt(3)+1;
			if(nodeId!=elasticTable[mainIndex].nodeId.get(subIndex-1)) {
				 System.out.println("MainIndex : "+ mainIndex);

			System.out.println("SubIndex: "+subIndex);
			System.out.println("Before : "+elasticTable[mainIndex].hashIndex +"    " + elasticTable[mainIndex].nodeId.get(subIndex-1));
			
			elasticTable[mainIndex].nodeId.set(subIndex-1, nodeId);
			System.out.println("After : " +elasticTable[mainIndex].hashIndex +"    " + elasticTable[mainIndex].nodeId.get(subIndex-1));
			}
			if(count>10) {
				break;
			}
		}
		
		
		// TODO Auto-generated method stub
		
	}
}

