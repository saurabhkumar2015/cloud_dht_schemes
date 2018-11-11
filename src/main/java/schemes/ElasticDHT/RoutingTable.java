package schemes.ElasticDHT;

import common.IRoutingTable;

import java.util.BitSet;
import java.util.Random;

public class RoutingTable {

	public static   ElasticRoutingTableInstance[] elasticTable  = new ElasticRoutingTableInstance[100];
	
	public static RoutingTable single_instance = null;

	private int changes[]; 
	
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
	
	public IRoutingTable addNode(int clusterId, int nodeId)
	{
		Random rno =  new Random();
		int noOfHashIndices = rno.nextInt(7)+0;
		int mainIndex = 0;//The number of hash values for which we change the node Id.
		for(int i = 0;i<noOfHashIndices;i++) {
			Random rno1 = new Random();
			 mainIndex = rno1.nextInt(noOfHashIndices); // For which hashIndex, we want
			int subIndex = rno1.nextInt(3)+1;
			switch(subIndex) {
			case 1:
			{
				elasticTable[mainIndex].nodeId1 = nodeId;
				
			}
			case 2:{
				elasticTable[mainIndex].nodeId2 = nodeId;
			}
			default : {
				elasticTable[mainIndex].nodeId3 = nodeId;
			}
		}
	}
		System.out.print(elasticTable[mainIndex].hashIndex);
		System.out.print(elasticTable[mainIndex].nodeId1);
		System.out.print(elasticTable[mainIndex].nodeId2);
		System.out.print( elasticTable[mainIndex].nodeId3);
		
		// Implement this
		return null;
}

	
	public void deleteNode(int nodeId)
	{
		int replaceNodeId = 0;
		Random rn = new Random();
		replaceNodeId = rn.nextInt(7)+0;
		while(replaceNodeId==nodeId) {
			replaceNodeId = rn.nextInt(7)+0;
		}
		for(int i = 0;i<100;i++) {
			if(elasticTable[i].nodeId1==nodeId) {
				elasticTable[i].nodeId1 = replaceNodeId;
				
			}
			if(elasticTable[i].nodeId2==nodeId) {
				elasticTable[i].nodeId2 = replaceNodeId;
			}
			if(elasticTable[i].nodeId3==nodeId) {
				elasticTable[i].nodeId3 = replaceNodeId;
			}
		}
//		return false;
	}
	
	public boolean Resize()
	{
		// Implement this
		return false;
	}
	public int getNodeId(String filename, int replicaId) {
		 int code = filename.hashCode(); // May not be the temporary hash code I have
		 int nodeId = 0;
		 for(int k = 0;k<elasticTable.length;k++) {
			 if(elasticTable[k].hashIndex==code) {
				 if(replicaId==1) {
					nodeId = elasticTable[k].nodeId1; 
				 }
				 else if(replicaId==2) {
					 nodeId = elasticTable[k].nodeId2;
				 }
				 else {
					 nodeId = elasticTable[k].nodeId3;
				 }
			 }
		 }
		 return nodeId;
	}
	public boolean loadBalance(int nodeId, int factor) {
		int replaceNodeId = 0;
		Random rn = new Random();
		replaceNodeId = rn.nextInt(7)+0;
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
		int newStrength = currentStrength*factor;
		for(int k = 0;k<currentStrength-newStrength;k++) {
			if(elasticTable[changes[k]].nodeId1==nodeId) {
				elasticTable[changes[k]].nodeId1 = replaceNodeId;
				
			}
			if(elasticTable[changes[k]].nodeId2==nodeId) {
				elasticTable[changes[k]].nodeId2 = replaceNodeId;
			}
			if(elasticTable[changes[k]].nodeId3==nodeId) {
				elasticTable[changes[k]].nodeId3 = replaceNodeId;
			}
			
		}
		// Call the new Delete to change value;
		
		
		// Change routing table appropriately across all primary and replicas 
		return false;
		
	}
	public static void main(String arg[]) {
		RoutingTable r =  new RoutingTable();
		elasticTable = r.getRoutingTable();
//		r.addNode(5);
		
		
	}
}

