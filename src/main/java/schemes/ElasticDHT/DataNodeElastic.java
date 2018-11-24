package schemes.ElasticDHT;

import static common.Commons.elasticTable;
import static common.Commons.elasticTable1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	public static Payload [] payload;
	public List<Payload> listofPayloads = new ArrayList<Payload>();
	
	public DataNodeElastic(int nodeId) {
		this.config = ConfigLoader.config;
		this.nodeId = nodeId;
		Commons.elasticERoutingTable  = new ERoutingTable();
		elasticTable = elasticTable1.populateRoutingTable();
		Commons.elasticERoutingTable.versionNumber = Commons.elasticERoutingTable.versionNumber + 1;

	}
	public static DataNodeElastic getInstance(int nodeId) {
		if(single_instance==null) {
			single_instance = new DataNodeElastic(nodeId);
		}
		return single_instance;
	}
	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight) {
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
	//List of payload is specific to nodeId, get it from map and then populate it.

	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {
		ElasticRoutingTableInstance [] newTable = ((ERoutingTable)rt).giveRoutingTable();
		ElasticRoutingTableInstance [] oldTable = Commons.elasticERoutingTable.giveRoutingTable();
		List<Integer> ListofnodeIds = new ArrayList<Integer>();
		Map<Integer, List<Payload>> nodeMap = new HashMap<Integer,List<Payload>>();
		if(type.equals("ADD_FILES")) {
			ListofnodeIds.clear();
			for(int  i = 0;i<oldTable.length;i++) {
				for(int j = 0;j<Commons.elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)== this.nodeId && oldTable[i].nodeId.get(j)!=newTable[i].nodeId.get(j)) {
						int updatedNodeId = newTable[i].nodeId.get(j);
						Payload p = new Payload("", j, this.getRoutingTable().getVersionNumber(),this.nodeId ,i);
						List<Payload> list = nodeMap.get(updatedNodeId);
						if(list == null) list = new ArrayList<Payload>();
						list.add(p);
						nodeMap.put(updatedNodeId, list);
					}
				}
				
			}
			
			for(Entry<Integer, List<Payload>> e : nodeMap.entrySet()) {
				int key = e.getKey();
				common.Commons.messageSender.sendMessage(config.nodesMap.get(key), common.Constants.ADD_FILES, e.getValue());

			}
		}
		if(type.equals("DELETE_FILE")) {
			ListofnodeIds.clear();
			for(int  i = 0;i<oldTable.length;i++) {
				for(int j = 0;j<Commons.elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)==this.nodeId&&oldTable[i].nodeId.get(j)!=newTable[i].nodeId.get(j)) {
						int updatedNodeId = newTable[i].nodeId.get(j);
						Payload p = new Payload("", j, this.getRoutingTable().getVersionNumber(),this.nodeId ,i);
						List<Payload> list = nodeMap.get(updatedNodeId);
						if(list == null) list = new ArrayList<Payload>();
						list.add(p);
						nodeMap.put(updatedNodeId, list);
					}
				}
			}
			for(Entry<Integer, List<Payload>> e : nodeMap.entrySet()) {
				int key = e.getKey();
				common.Commons.messageSender.sendMessage(config.nodesMap.get(key), common.Constants.DELETE_FILE, e.getValue());

			}
		}
		if(type.equals("MOVE_FILE")) {
			ListofnodeIds.clear();
			for(int i = 0;i<oldTable.length;i++) {
				for(int j = 0; j<Commons.elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)==this.nodeId&&oldTable[i].nodeId.get(j)!=newTable[i].nodeId.get(j)) {
						int oldNodeId;
						int temp;
						if(j==0) {
							temp = j+1;
						}
						else if(j==Commons.elasticERoutingTable.rFactor-1) {
							temp = j-1;
						}
						else {
							temp = j-1;
						}
						oldNodeId = oldTable[i].nodeId.get(temp);
						Payload p = new Payload("", j, this.getRoutingTable().getVersionNumber(),newTable[i].nodeId.get(j) ,i);
						List<Payload> list = nodeMap.get(oldTable[i].nodeId.get(temp));
						if(list == null) list = new ArrayList<Payload>();
						list.add(p);
						nodeMap.put(oldNodeId, list);
					}
				}
			}
			for(Entry<Integer, List<Payload>> e : nodeMap.entrySet()) {
				int key = e.getKey();
				common.Commons.messageSender.sendMessage(config.nodesMap.get(key), common.Constants.MOVE_FILE, e.getValue());

			}
		}

	}
	public void UpdateRoutingTable(IRoutingTable cephrtTable, String updateType) {
		// TODO Auto-generated method stub
		
	
	
}
	public boolean writeAllFiles(List<Payload> payloads) {
		// TODO Auto-generated method stub
		return false;
	}
}
