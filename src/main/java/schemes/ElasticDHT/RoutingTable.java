package schemes.ElasticDHT;

import common.IRoutingTable;

import config.ConfigLoader;
import config.DHTConfig;

import java.util.BitSet;
import java.util.Random;

public class RoutingTable implements IRoutingTable{

	public static   ElasticRoutingTableInstance[] elasticTable  = new ElasticRoutingTableInstance[100];
	
	public static RoutingTable single_instance = null;

	private int changes[]; 
	DHTConfig config = ConfigLoader.config;
	int size = config.bucketSize;
	
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
	
	@SuppressWarnings("unchecked")
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
			System.out.println("k = "+k+"i = "+i);
			if(k!=-1) {
				elasticTable[i].nodeId.set(k, replaceNodeId);
				System.out.println("Deleting and replacing with : "+replaceNodeId);
				System.out.println( "New table : "+elasticTable[i].hashIndex+" "+elasticTable[i].nodeId.get(k));
			}
			

			
			// check if nodeId is in hash and get that index
		}
		return this;
	}

	

	public boolean Resize()
	{
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

	@SuppressWarnings("unchecked")
	public IRoutingTable loadBalance(int nodeId, double factor) {
		int replaceNodeId = 0;
		Random rn = new Random(config.seed);
		replaceNodeId = rn.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		Random rn1 = new Random(config.seed);
		replaceNodeId = rn1.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
		while(replaceNodeId==nodeId) {
			replaceNodeId = rn.nextInt(7)+0;
		}
		InvertedIndexTable i = InvertedIndexTable.GetInstance();
		i.CreateInvertedIndexTable();
		int currentStrength = 0;
		BitSet b = null;
		// Get nodeId and the bitset value.
		for(int k = 0; k<7;k++) {
			if(i.indexInstance.get(k).nodeId==nodeId) {
				b = i.indexInstance.get(k).usedHashedIndex;
				break;
			}
		}
		changes = null;
		int count = 0;
		// find strength
		for(int j = 0;j <b.length();j++) {
			if(b.get(j)) {
				changes[count] = j;
				currentStrength++;
			}
			count++;
		}
		int newStrength = (int) (currentStrength*factor);
		for(int k = 0;k<currentStrength-newStrength;k++) {
			int index = check(elasticTable[changes[k]].hashIndex,nodeId);
			elasticTable[changes[k]].nodeId.set(index, replaceNodeId);
			System.out.println("Files were moved from "+nodeId +"to "+replaceNodeId);
		}
		
		
		return this;
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
		Random rno =  new Random(config.seed);
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
		
		return this;
	}
}

