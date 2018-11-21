package schemes.ElasticDHT;

import common.Commons;
import common.IDataNode;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;
import config.DHTConfig;

import java.util.List;

import static common.Commons.elasticTable;
import static common.Commons.elasticTable1;

public class DataNodeElastic implements IDataNode {

	private int nodeId;
	private static DataNodeElastic single_instance = null;
	private DHTConfig config;
	public DataNodeElastic(int nodeId) {
		this.config = ConfigLoader.config;
		this.nodeId = nodeId;
		Commons.elasticERoutingTable  = new ERoutingTable();
		elasticTable = elasticTable1.populateRoutingTable();

	}
	public static DataNodeElastic getInstance(int nodeId) {
		if(single_instance==null) {
			single_instance = new DataNodeElastic(nodeId);
		}
		return single_instance;
	}
	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight) {
	}
	public void UpdateRoutingTable(IRoutingTable elasticTable, String type) {
		Commons.elasticERoutingTable = (ERoutingTable) elasticTable;
		System.out.println("New elastic table with versionNumber number : "+Commons.elasticERoutingTable.versionNumber);
	}


	public boolean writeFile(String fileName, int replicaId) {
		int hashcode = fileName.hashCode()%1024;
		nodeId = 0;
		for(int i = 0; i< elasticTable.length;i++) {
			if(elasticTable[i].hashIndex==hashcode) {
				nodeId = (Integer) elasticTable[i].nodeId.get(replicaId-1);
				break;
			}
		}
		int writeNodeId = Commons.elasticERoutingTable.giveNodeId(fileName, replicaId-1);
		if(writeNodeId!=nodeId) {
			return false;
		}
		System.out.println("File written to "+nodeId);
		return true;


	}


	public void deleteFile(String fileName) {
		int hashcode =  fileName.hashCode();
		for(int i = 0; i<elasticTable.length; i++) {
			if(elasticTable[i].hashIndex==hashcode) {
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
		return Commons.elasticERoutingTable;
	}

	@Override
	public String toString() {
		return "DataNodeElastic [nodeId=" + nodeId + ", elasticTable=" + Commons.elasticERoutingTable + ", config=" + config
				+ ", giveRoutingTable()=" + getRoutingTable() + "]";
	}
	public void addHashRange(String hashRange) {
		// TODO Auto-generated method stub

	}

	@Override
	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {

	}
	
	@Override
	public boolean writeAllFiles(List<Payload> payloads) {
		// TODO Auto-generated method stub
		return false;
	}
}
