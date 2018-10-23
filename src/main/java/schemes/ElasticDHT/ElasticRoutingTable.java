package schemes.ElasticDHT;

public class ElasticRoutingTable {
	public ElasticRoutingTableInstance[] populateRoutingTable() {
		ElasticRoutingTableInstance[] initialTable  = new ElasticRoutingTableInstance[7];
		
		initialTable[0]= new ElasticRoutingTableInstance(1,1,2,3) ;
		initialTable[1]= new ElasticRoutingTableInstance(2,2,1,4) ;
		initialTable[2]= new ElasticRoutingTableInstance(3,4,5,3) ;
		initialTable[3]= new ElasticRoutingTableInstance(4,5,4,1) ;
		initialTable[4]= new ElasticRoutingTableInstance(5,2,4,1) ;	
		
		initialTable[5]= new ElasticRoutingTableInstance(6,1,2,2) ;	
	
		initialTable[6]= new ElasticRoutingTableInstance(7,3,4,5) ;	
	
	
		

		return initialTable;
		
		
	}

}
