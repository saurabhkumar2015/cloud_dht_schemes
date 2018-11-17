package schemes.ElasticDHT;

import common.IRoutingTable;

import config.ConfigLoader;
import config.DHTConfig;

import java.util.*;
import java.util.Map.Entry;

public class RoutingTable implements IRoutingTable {

	int bucketSize = ConfigLoader.config.bucketSize;
	int rFactor = ConfigLoader.config.replicationFactor;

	public static ElasticRoutingTableInstance[] elasticTable;
	public static ElasticRoutingTable elasticTable1 = new ElasticRoutingTable();

	public static RoutingTable single_instance = null;

	Map<Integer, Integer> hashReplicaNodeId = new HashMap<Integer, Integer>();

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

		Random rn = new Random();
		List<Integer> liveNodes = getLiveNodes();
		if(!liveNodes.remove((Integer)nodeId)) {
			System.out.print("Node id " + nodeId + "is not present in routing table.");
			return this;
		}
		System.out.println("Delet Node Request" + nodeId+":"+liveNodes);
		int l = liveNodes.size();

		for (int i = 0; i < bucketSize; i++) {
			int k = check(i, nodeId);
			if (k != -1) {
				int replaceNodeId = liveNodes.get(rn.nextInt(l));
				while (replaceNodeId == nodeId || elasticTable[i].nodeId.contains(replaceNodeId)) {
					replaceNodeId = liveNodes.get(rn.nextInt(l));
				}
				System.out.println("Deleting and replacing " + elasticTable[i].hashIndex + ":" + nodeId + " with : "
						+ replaceNodeId + " " + elasticTable[i].nodeId);
				elasticTable[i].nodeId.set(k, replaceNodeId);
			}

			// check if nodeId is in hash and get that index
		}

		return this;
	}

	public boolean Resize() {
		// Create new elasticTbaletemp and reassign to it.
		// Ratio based resize factor hashbuckets/live nodes> read the resize factor
		// config.
		// Implement this
		return false;
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
			int nodeIndex = rn.nextInt(maxLiveNodes);
			while (nodeId == liveNodes.get(nodeIndex) || elasticTable[key].nodeId.contains((Integer)liveNodes.get(nodeIndex) )) {
				nodeIndex = rn.nextInt(maxLiveNodes);

			}
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
				elasticTable[index].nodeId.set(subIndex, nodeId);
				System.out.println("After Add node : " + elasticTable[index].hashIndex + ":" + previous + ","
						+ elasticTable[index].nodeId.get(subIndex));
			}
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
}
