package schemes.ElasticDHT;

import common.Commons;
import common.IRoutingTable;


import config.ConfigLoader;
import socket.MockMessageSender;

import java.util.*;
import java.util.Map.Entry;
import static common.Constants.*;


public class RoutingTable implements IRoutingTable {

	int bucketSize = ConfigLoader.config.bucketSize;
	int rFactor = ConfigLoader.config.replicationFactor;
	long versionNumber;
	

	@Override
	public String toString() {
		return "RoutingTable [bucketSize=" + bucketSize + ", rFactor=" + rFactor + ", versionNumber=" + versionNumber
				+ ", hashReplicaNodeId=" + hashReplicaNodeId + ", getRoutingTable()="
				+ Arrays.toString(getRoutingTable()) + ", getVersionNumber()=" + getVersionNumber() + "]";
	}



	public static ElasticRoutingTableInstance[] elasticTable;
	public static ElasticRoutingTable elasticTable1 = new ElasticRoutingTable();

	public static RoutingTable single_instance = null;

	Map<Integer, Integer> hashReplicaNodeId = new HashMap<Integer, Integer>();
	Map<Integer, Map<Integer,Integer>> hashNodeIdReplicaAdd = new HashMap<Integer,Map<Integer,Integer>>();//Hash,NodeId,Replica
	Map<Integer,Integer> hashNodeIdDelete = new HashMap<Integer,Integer>();
	

	public RoutingTable() {
		elasticTable = elasticTable1.populateRoutingTable();
	}

	// Make singleton Instance of routing table
	public static RoutingTable GetInstance() {
		if (single_instance == null)
			single_instance = new RoutingTable();

		return single_instance;
	}

	public ElasticRoutingTableInstance[] getRoutingTable() {
		return elasticTable;
	}

	public IRoutingTable deleteNode(int nodeId) {

		System.out.println("Entering delete functions");
		Commons.messageSender = new MockMessageSender();

		Random rn = new Random();
		List<Integer> liveNodes = getLiveNodes();
		if(!liveNodes.remove((Integer)nodeId)) {
			System.out.print("Node id " + nodeId + "is not present in routing table.");
			return this;
		}
		System.out.println("Delete Node Request" + nodeId+":"+liveNodes);
		System.out.println("Delet Node Request" + nodeId+":"+liveNodes);
		int l = liveNodes.size();

		for (int i = 0; i < bucketSize; i++) {
			int k = check(i, nodeId);
			if (k != -1) {
				int replaceNodeId = liveNodes.get(rn.nextInt(l));
				while (replaceNodeId == nodeId || elasticTable[i].nodeId.contains(replaceNodeId)) {
					replaceNodeId = liveNodes.get(rn.nextInt(l));
				}
				hashNodeIdDelete = createSendMapforDeleteNode( nodeId);
				int temp = hashNodeIdDelete.get(i);
				Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(temp), MOVE_FILE, hashNodeIdDelete);
				System.out.println("Moving files from "+temp +"to "+replaceNodeId);
				elasticTable[i].nodeId.set(k, replaceNodeId);
				hashNodeIdReplicaAdd = createSendMapforAddNode(replaceNodeId);

				Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(replaceNodeId), ADD_FILES, hashNodeIdReplicaAdd);
				System.out.println("Files of the hash bucket "+elasticTable[i].hashIndex + "were added to "+replaceNodeId);
				System.out.println("The node with nodeId "+nodeId+" was deleted");
				
				
				System.out.println("Deleting and replacing " + elasticTable[i].hashIndex + ":" + nodeId + " with : "
						+ replaceNodeId + " " + elasticTable[i].nodeId);
				elasticTable[i].nodeId.set(k, replaceNodeId);
			}

			// check if nodeId is in hash and get that index
		}

		return this;
	}

	public ElasticRoutingTableInstance [] Resize() {
		ElasticRoutingTableInstance[] resizeElasticTable = new ElasticRoutingTableInstance[elasticTable.length*2];

		for(int i = 0;i<elasticTable.length;i++) {
			resizeElasticTable[i] = new ElasticRoutingTableInstance(elasticTable[i].hashIndex,elasticTable[i].nodeId);
		}
		//Exapnded
		int newLength = elasticTable.length*2;
		for(int j = elasticTable.length;j<newLength;j++) {
			resizeElasticTable[j] = new ElasticRoutingTableInstance(j,elasticTable[j%elasticTable.length].nodeId);
		}
		System.out.println("Hash table resized");
		return resizeElasticTable;
	}

	public int getNodeId(String filename, int replicaId) {
		int code = filename.hashCode() % 1024;
		System.out.println(code);
		// May not be the temporary hash code I have
		int nodeId = 0;

		for (int k = 0; k < elasticTable.length; k++) {
			if (elasticTable[k].hashIndex == code) {
				nodeId = elasticTable[k].nodeId.get(replicaId - 1);
				System.out.println("In function");
			}
		}
		return nodeId;
	}

	// public void addNode(int nodeId) throws IOException {
	// return null;
	// }
// Get live nodes and choose randomly
	// Map for hash and replica
	// Load balance
	// Drop inverted index, <hash,replica> for nodeId in consideration map for a
	// node Id, Iterate over it,
	public IRoutingTable loadBalance(int nodeId, double factor) {
		// Get strength of current nodeId
		// Randomly choose replaceNodeId
		//
		Commons.messageSender = new MockMessageSender();

		List<Integer> liveNodes = getLiveNodes();
		int maxLiveNodes = liveNodes.size();
		System.out.println(maxLiveNodes);

		hashReplicaNodeId = createMapforNodeId(nodeId, elasticTable);
		Set<Integer> keys = hashReplicaNodeId.keySet();
		int currentstrength = hashReplicaNodeId.size();
		int newStrength = (int) (currentstrength * factor);
		System.out.println("Current Strength = " + currentstrength + " New Strength = " + newStrength);
		Random rn = new Random();
		int deleteNodes = currentstrength - newStrength;
		


		for (Entry<Integer, Integer> e : hashReplicaNodeId.entrySet()) {
			if (deleteNodes <= 0)
				break;

			int key = e.getKey();
			Map<Integer,Integer> hashNodeIdTrueMapPayload = new HashMap<Integer,Integer>();
			hashNodeIdTrueMapPayload.put(key, nodeId);
			int nodeIndex = rn.nextInt(maxLiveNodes);
			while (nodeId == liveNodes.get(nodeIndex) || elasticTable[key].nodeId.contains((Integer)liveNodes.get(nodeIndex) )) {
				nodeIndex = rn.nextInt(maxLiveNodes);

			}
			Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(nodeId), MOVE_FILE, hashNodeIdTrueMapPayload);
			System.out.println("Moving files with hashbucket value = "+key);
			
			elasticTable[key].nodeId.set(e.getValue(), liveNodes.get(nodeIndex));
			Map<Integer,Integer> hashNodeIdNewNodePayload = new HashMap<Integer,Integer>();
			hashNodeIdNewNodePayload.put(key, liveNodes.get(nodeIndex));

			Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(liveNodes.get(nodeIndex)), ADD_FILES,hashNodeIdNewNodePayload );
			elasticTable[key].nodeId.set(e.getValue(), liveNodes.get(nodeIndex));
			System.out.println(key + ":File moved from " + nodeId + "to " + liveNodes.get(nodeIndex));
			// System.out.println(elasticTable[e.getKey().intValue()]+ " :"+
			// elasticTable[e.getKey().intValue()].nodeId.get(e.getValue()));
			deleteNodes--;

		}
		System.out.println("Load balanced");
		return this;
	}

	public List<Integer> getLiveNodes() {

		return new ArrayList<Integer>(getLiveNodesSet());
	}

	public Set<Integer> getLiveNodesSet() {
		int tempNode;

		Set<Integer> liveNodes = new HashSet<Integer>();
		for (int i = 0; i < ConfigLoader.config.bucketSize; i++) {
			for (int j = 0; j < 3; j++) {
				tempNode = elasticTable[i].nodeId.get(j);
				if (tempNode == 3) {
					System.out.print("");
				}
				liveNodes.add(tempNode);
			}
		}
		return liveNodes;

	}

	public Map<Integer, Integer> createMapforNodeId(int nodeId, ElasticRoutingTableInstance rt[]) {
		for (int i = 0; i < ConfigLoader.config.bucketSize; i++) {
			for (int j = 0; j < 3; j++) {
				if (nodeId == rt[i].nodeId.get(j)) {
					hashReplicaNodeId.put(rt[i].hashIndex, j);
				}
			}

		}

		return hashReplicaNodeId;
	}

	int check(int index, int nodeId) {
		int i = 0;
		boolean b = false;
		if (index == 401) {
			System.out.println("");
		}

		for (i = 0; i < 3; i++) {
			if (elasticTable[index].nodeId.get(i) == nodeId) {
				b = true;
				break;
			}
		}
		if (b) {
			return i;
		} else {
			return -1;
		}

	}

	public IRoutingTable addNode(int nodeId) {
		Commons.messageSender = new MockMessageSender();

		
		List<Integer> liveNodes = getLiveNodes();
		if(liveNodes.contains((Integer) nodeId)) {
			System.out.println("Node Id " + nodeId + "already exists in Routing Table.");
			return this;
		}
		int range = liveNodes.size();
		int interval = bucketSize / range;

		Random rno1 = new Random();
		Set<Integer> usedBuckets = new HashSet<Integer>();

		for (int i = 0; i < interval; i++) {
			int index = rno1.nextInt(bucketSize);
			while (usedBuckets.contains(index)) {
				index = rno1.nextInt(bucketSize);
			}
			usedBuckets.add(index);
			// For which hashIndex, we want
			int subIndex = rno1.nextInt(rFactor);
			if (nodeId != elasticTable[index].nodeId.get(subIndex)) {
				int previous = elasticTable[index].nodeId.get(subIndex);
				System.out.println("File with hash bucket : "+elasticTable[index].hashIndex + "with replica id: "+subIndex + "with node id : "+previous);
				elasticTable[index].nodeId.set(subIndex, nodeId);
				hashNodeIdReplicaAdd = createSendMapforAddNode(nodeId);
				Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(previous), DELETE_FILE, hashNodeIdReplicaAdd);
				System.out.println("File with hash bucket : " + elasticTable[index].hashIndex + " with replicaId" + subIndex + " is now in nodeId"
						+ nodeId);
				Commons.messageSender.sendMessage(ConfigLoader.config.nodesMap.get(nodeId), ADD_FILES, hashNodeIdReplicaAdd);
				elasticTable[index].nodeId.set(subIndex, nodeId);
				System.out.println("After Add node : " + elasticTable[index].hashIndex + ":" + previous + ","
						+ elasticTable[index].nodeId.get(subIndex));
			}
		}
		liveNodes = getLiveNodes();
		range = liveNodes.size();
		int ratio = elasticTable.length/range;
		if(ratio<config.ConfigLoader.config.resizeFactor) {
			elasticTable = Resize();
		}
		

		// TODO Auto-generated method stub
		return this;
	}

	public void printRoutingTable() {

		int size = elasticTable.length;
		for (int i = 0; i < size; i++) {
			ElasticRoutingTableInstance bucket = elasticTable[i];
			System.out.println(bucket.hashIndex + ":" + bucket.nodeId);
		}

	}
	public  Map<Integer,Map<Integer,Integer>> createSendMapforAddNode(int nodeId){
		Map<Integer,Map<Integer,Integer>> tempMap = new HashMap<Integer,Map<Integer,Integer>>();
		Map<Integer,Integer> nestMap = new HashMap<Integer,Integer>();
		for(int i = 0;i<elasticTable.length;i++) {
			for(int j = 0;j<rFactor;j++) 
				if(elasticTable[i].nodeId.get(j)==nodeId) {
					int tempNodeId = elasticTable[i].nodeId.get(j);
					nestMap.put(tempNodeId, j);
					tempMap.put(elasticTable[i].hashIndex, nestMap);
			}
		}
		return tempMap;
	}
	public Map<Integer,Integer> createSendMapforDeleteNode(int nodeId){
		Map<Integer,Integer> tempMap = new HashMap<Integer,Integer>();
		int k;
		for(int i =0;i<elasticTable.length;i++) {
			for(int j = 0;j<rFactor;j++) {
				if(elasticTable[i].nodeId.get(j)==nodeId) {
					if(j==0) {
						k = j+1;
					}
					else if(j==rFactor-1) {
						k = j-1;
					}
					else {
						k = j +1;
					}
					tempMap.put(elasticTable[i].hashIndex, k);
				}
			}
		}
		return tempMap;
	}
	
	

	public long getVersionNumber() {
		return this.versionNumber;
	}


}
