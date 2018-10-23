package schemes.ElasticDHT;

public class ElasticRoutingTableInstance {

	int hashIndex;
	int nodeId1, nodeId2,nodeId3;
	ElasticRoutingTableInstance(int h,int n1, int n2, int n3){
		hashIndex = h;
		nodeId1 = n1;
		nodeId2 = n2;
		nodeId3 = n3;
	}
	public ElasticRoutingTableInstance() {
		// TODO Auto-generated constructor stub
	}
	
}

// Create Object ElasticRoutingTable ert[] , ert[0].hashIndex : Access, similarly for all fields.
