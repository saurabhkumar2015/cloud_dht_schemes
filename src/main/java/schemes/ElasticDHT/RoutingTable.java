package schemes.ElasticDHT;

import java.util.Random;

public class RoutingTable {

	public static   ElasticRoutingTableInstance[] elasticTable  = new ElasticRoutingTableInstance[100];
	
	public static RoutingTable single_instance = null; 
	
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
	
	public   ElasticRoutingTableInstance[] AddNode(int nodeId)
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
		return elasticTable;
}

	
	public boolean DeleteNode(int nodeId)
	{
		for(int i = 0;i<100;i++) {
			if(elasticTable[i].nodeId1==nodeId) {
				elasticTable[i].nodeId1 = nodeId-1;
				
			}
			if(elasticTable[i].nodeId2==nodeId) {
				elasticTable[i].nodeId2 = nodeId-1;
			}
			if(elasticTable[i].nodeId3==nodeId) {
				elasticTable[i].nodeId3 = nodeId-1;
			}
		}
		return false;
	}
	
	public boolean Resize()
	{
		// Implement this
		return false;
	}
	public boolean LoadBalance(int nodeId) {
		// Change routing table appropriately across all primary and replicas 
		return false;
		
	}
	public static void main(String arg[]) {
		RoutingTable r =  new RoutingTable();
		elasticTable = r.getRoutingTable();
		elasticTable = r.AddNode(5);
		
		
	}
}

