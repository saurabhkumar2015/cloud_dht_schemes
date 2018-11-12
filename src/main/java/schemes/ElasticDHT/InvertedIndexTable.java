package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.BitSet;
import config.ConfigLoader;
import config.DHTConfig;

public class InvertedIndexTable {

	public ArrayList<InvertedIndexTableInstance> indexInstance = new ArrayList<InvertedIndexTableInstance>();

	// Create a singleton for InvertedIndexTable
	private static InvertedIndexTable single_instance = null;

	private int hashforIndexTables[];

	private int ks[];

	public InvertedIndexTable()
	{

	}

	// Make singleton Instance of routing table
	public static InvertedIndexTable GetInstance()
	{
		if (single_instance == null)
		{
			single_instance = new InvertedIndexTable();
		}
		return single_instance;
	}


	public void CreateInvertedIndexTable()
	{
		ElasticRoutingTableInstance[] rt = RoutingTable.GetInstance().getRoutingTable();
		int nodeId[] = populateNodeId();
		String bits="";
		hashforIndexTables = null;
		DHTConfig config = ConfigLoader.config;
		for(int i = 0;i<(config.nodeIdEnd-config.nodeIdStart);i++) {
			for(int j = 0;j<config.bucketSize;j++) {
				if(replicaPosition(rt[j].hashIndex,nodeId[i])) {
					hashforIndexTables[j] = j+1;
				}
				else {
					hashforIndexTables[j] = j;
				}
			}
			for(int k = 0;k<hashforIndexTables.length;k++) {
				if(hashforIndexTables[k]==k+1) {
					bits = bits+"1";
				}
				else {
					bits = bits+"0";
				}
			}
			BitSet b = fromString(bits);
			InvertedIndexTableInstance e = new InvertedIndexTableInstance(nodeId[i],b);
			indexInstance.add(e);

		}
		// Iterate over the routing table Instance and Create the Inverted Index table accordingly



	}
	private static BitSet fromString(String binary) {
		BitSet bitset = new BitSet(binary.length());
		for (int i = 0; i < binary.length(); i++) {
			if (binary.charAt(i) == '1') {
				bitset.set(i);
			}
		}
		return bitset;
	}
	public int[] populateNodeId() {
		ks = null;
		for(int i = 0;i<(config.ConfigLoader.config.nodeIdEnd-config.ConfigLoader.config.nodeIdStart);i++) {
			ks[i] = config.ConfigLoader.config.nodeIdStart+i;
		}
		return ks;
	}
	boolean replicaPosition( int hashBucket, int nodeId) {
		int s;
		ElasticRoutingTableInstance[] rt = RoutingTable.GetInstance().getRoutingTable();

		for(s=0;s<config.ConfigLoader.config.nodeIdEnd-config.ConfigLoader.config.nodeIdStart;s++) {
			if((Integer)rt[hashBucket].nodeId.get(s)==nodeId) {
				return true;
			}
		}
		return false;
	}

}