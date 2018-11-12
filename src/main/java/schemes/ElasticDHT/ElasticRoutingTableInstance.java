package schemes.ElasticDHT;

import java.util.ArrayList;

public class ElasticRoutingTableInstance {

	public int hashIndex;
	@SuppressWarnings("rawtypes")
	public ArrayList nodeId;


	@SuppressWarnings("rawtypes")
	public ElasticRoutingTableInstance(int k, ArrayList l) {
		hashIndex = k;
		nodeId = l;
		// TODO Auto-generated constructor stub
	}

}

// Create Object ElasticRoutingTable ert[] , ert[0].hashIndex : Access, similarly for all fields.