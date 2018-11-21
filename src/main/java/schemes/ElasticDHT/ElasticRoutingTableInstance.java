package schemes.ElasticDHT;

import java.io.Serializable;
import java.util.ArrayList;

public class ElasticRoutingTableInstance implements Serializable {

	public int hashIndex;
	@SuppressWarnings("rawtypes")
	public ArrayList<Integer> nodeId;
	
	
	@SuppressWarnings("rawtypes")
	public ElasticRoutingTableInstance(int k, ArrayList<Integer> l) {
		hashIndex = k;
		nodeId = l;
		// TODO Auto-generated constructor stub
	}

	public ElasticRoutingTableInstance(){}
}

// Create Object ElasticRoutingTable ert[] , ert[0].hashIndex : Access, similarly for all fields.
