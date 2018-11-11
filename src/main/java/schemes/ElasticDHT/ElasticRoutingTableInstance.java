package schemes.ElasticDHT;

import java.util.List;

public class ElasticRoutingTableInstance {

	public int hashIndex;
	@SuppressWarnings("rawtypes")
	public List nodeId;
	
	
	@SuppressWarnings("rawtypes")
	public ElasticRoutingTableInstance(int k, List l) {
		hashIndex = k;
		nodeId = l;
		// TODO Auto-generated constructor stub
	}
	
}

// Create Object ElasticRoutingTable ert[] , ert[0].hashIndex : Access, similarly for all fields.
