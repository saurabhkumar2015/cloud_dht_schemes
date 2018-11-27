package schemes.ElasticDHT;

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


import static common.Commons.*;

public class DataNodeElastic implements IDataNode {

	private int nodeId;
	private static DataNodeElastic single_instance = null;
	private DHTConfig config;
	public static Payload [] payload;
	public List<Payload> listofPayloads = new ArrayList<Payload>();
    public  ElasticRoutingTableInstance[] elasticTable;

	public boolean fileLock = true;

	public DataNodeElastic(int nodeId) {
		this.config = ConfigLoader.config;
		this.nodeId = nodeId;
		elasticERoutingTable  = new ERoutingTable();
		elasticERoutingTable.elasticTable = elasticTable1.populateRoutingTable();
		elasticERoutingTable.versionNumber = elasticERoutingTable.versionNumber + 1;
		elasticOldERoutingTable = elasticERoutingTable;
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
		for(int i = 0; i< elasticERoutingTable.elasticTable.length;i++) {
			if(elasticERoutingTable.elasticTable[i].hashIndex==hashcode) {
				nodeId = (Integer) elasticERoutingTable.elasticTable[i].nodeId.get(replicaId-1);
				break;
			}
		}
		int writeNodeId = elasticERoutingTable.giveNodeId(fileName, replicaId);
		if(writeNodeId!=nodeId) {
			return false;
		}
		return true;
	}


	public void deleteFile(String fileName) {
		int hashcode =  fileName.hashCode();
		for(int i = 0; i<elasticERoutingTable.elasticTable.length; i++) {
			if(elasticERoutingTable.elasticTable[i].hashIndex==hashcode) {
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
		return elasticERoutingTable;
	}

	@Override
	public String toString() {
		return "DataNodeElastic [nodeId=" + nodeId + ", elasticTable=" + elasticERoutingTable + ", config=" + config
				+ ", giveRoutingTable()=" + getRoutingTable() + "]";
	}
	public void addHashRange(String hashRange) {
		// TODO Auto-generated method stub

	}
	//List of payload is specific to nodeId, get it from map and then populate it.

	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {
		fileLock = false;
		ElasticRoutingTableInstance [] newTable = ((ERoutingTable)rt).giveRoutingTable();
		ElasticRoutingTableInstance [] oldTable = elasticERoutingTable.giveRoutingTable();
		Commons.elasticOldERoutingTable = elasticERoutingTable;
		List<Integer> ListofnodeIds = new ArrayList<Integer>();
		Map<Integer, List<Payload>> nodeMap = new HashMap<Integer,List<Payload>>();
		if(type.equals("ADD_FILES")) {
			ListofnodeIds.clear();
			for(int  i = 0;i<oldTable.length;i++) {
				for(int j = 0;j<elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)== this.nodeId && !oldTable[i].nodeId.get(j).equals(newTable[i].nodeId.get(j))) {
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
				for(int j = 0;j<elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)==this.nodeId&& !oldTable[i].nodeId.get(j).equals(newTable[i].nodeId.get(j))) {
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
				for(int j = 0; j<elasticERoutingTable.rFactor;j++) {
					if(oldTable[i].nodeId.get(j)==this.nodeId&& !oldTable[i].nodeId.get(j).equals(newTable[i].nodeId.get(j))) {
						int oldNodeId;
						int temp;
						if(j==0) {
							temp = j+1;
						}
						else if(j==elasticERoutingTable.rFactor-1) {
							temp = j-1;
						}
						else {
							temp = j-1;
						}
						oldNodeId = oldTable[i].nodeId.get(temp);
						Payload p = new Payload("", j, this.getRoutingTable().getVersionNumber(),oldTable[i].nodeId.get(temp) ,i);
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
		((ERoutingTable)this.getRoutingTable()).elasticTable = newTable;
		fileLock = true;
	}
	public void UpdateRoutingTable(IRoutingTable cephrtTable, String updateType) {
		// TODO Auto-generated method stub
		
	
	
}
	public boolean writeAllFiles(List<Payload> payloads) {
		int lenght = payloads.size();
		List<Integer> hashBUckets = new ArrayList<Integer>();
		for(int i = 0;i<lenght;i++) {
			Payload p = payloads.get(i);
			hashBUckets.add(p.hashBucket);
			
		}
		System.out.println(hashBUckets);
				// TODO Auto-generated method stub
		return false;
	}
	
	
	@Override
	public IRoutingTable getOldRoutingTable() {

		return Commons.elasticOldERoutingTable;
	}
	@Override
	public void setOldRoutingTable() {
		
	}
	@Override
	public boolean getUseUpdatedRtTable() {
		// TODO Auto-generated method stub
		return fileLock;
	}
	@Override
	public void setUseUpdatedRtTable(boolean value) {
		
	}
	@Override
	public int getNodeId() {
		// TODO Auto-generated method stub
		return this.nodeId;
	}
}
