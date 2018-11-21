package schemes.ElasticDHT;

import common.Commons;
import common.IDataNode;
import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

import static common.Commons.elasticTable1;

public class DataNodeElastic implements IDataNode {

	private int nodeId;
	public IRoutingTable elasticTable;
	private static DataNodeElastic single_instance = null;
    private DHTConfig config;
    public DataNodeElastic(int nodeId) {
    	this.config = ConfigLoader.config;
    	this.nodeId = nodeId;
    	elasticTable  = new ERoutingTable();
		Commons.elasticTable = elasticTable1.populateRoutingTable();
    	
    }
    public static DataNodeElastic getInstance(int nodeId) {
    	if(single_instance==null) {
    		single_instance = new DataNodeElastic(nodeId);
    	}
    	return single_instance;
    }
	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight) {
 	}
 	public void UpdateRoutingTable(IRoutingTable elasticTable) {
 		this.elasticTable = elasticTable;
 		ERoutingTable rt = (ERoutingTable)elasticTable;
 		System.out.println("New elastic table with versionNumber number : "+rt.versionNumber);
 	
 	}

	
	public boolean writeFile(String fileName, int replicaId) {
		int hashcode = fileName.hashCode()%1024;
		nodeId = 0;
		for(int i = 0; i< Commons.elasticTable.length;i++) {
			if(Commons.elasticTable[i].hashIndex==hashcode) {
				 nodeId = (Integer) Commons.elasticTable[i].nodeId.get(replicaId-1);
				break;
			}
		}
		int writeNodeId = this.elasticTable.giveNodeId(fileName, replicaId-1);
		if(writeNodeId!=nodeId) {
			return false;
		}
		System.out.println("File written to "+nodeId);
		return true;
		
		
	}

	public void deleteFile(String fileName) {
		int hashcode =  fileName.hashCode();
		for(int i = 0; i<Commons.elasticTable.length; i++) {
			if(Commons.elasticTable[i].hashIndex==hashcode) {
				System.out.println("File deleted from all the replicas");
			}
		}
		// TODO Auto-generated method stub
		
	}


	public void addNode(int nodeId) {
		ERoutingTable.giveInstance().addNode(nodeId);
		// TODO Auto-generated method stub
		
	}

	public void deleteNode(int nodeId) {
		ERoutingTable.giveInstance().deleteNode(nodeId);
		// TODO Auto-generated method stub
		
	}

	public void loadBalance(int nodeId, double loadFraction) {
		ERoutingTable.giveInstance().loadBalance(nodeId, loadFraction);

		// TODO Auto-generated method stub
		
	}

	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance) {

	}

	public IRoutingTable getRoutingTable() {
		return this.elasticTable;
	}
	
	@Override
	public String toString() {
		return "DataNodeElastic [nodeId=" + nodeId + ", elasticTable=" + elasticTable + ", config=" + config
				+ ", giveRoutingTable()=" + getRoutingTable() + "]";
	}
	public void addHashRange(String hashRange) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {

	}
}
