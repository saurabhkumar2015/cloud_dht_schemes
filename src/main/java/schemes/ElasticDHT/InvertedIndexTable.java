package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.BitSet;
import config.ConfigLoader;
import config.DHTConfig;

public class InvertedIndexTable {

	public ArrayList<InvertedIndexTableInstance> indexInstance = new ArrayList<InvertedIndexTableInstance>();
	
	// Create a singleton for InvertedIndexTable
	private static InvertedIndexTable single_instance = null;

	private ArrayList<Integer> hashforIndexTables = new ArrayList();

	private ArrayList<Integer> ks =  new ArrayList();; 

	
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
	
	
	public void CreateInvertedIndexTable(ElasticRoutingTableInstance[] rt)
	{
		System.out.println("Entering create Inverted Index");
		ArrayList<Integer> nodeId = populateNodeId();
		String bits="";
		hashforIndexTables = new ArrayList();
		DHTConfig config = ConfigLoader.config;
		for(int i = 0;i<(config.nodeIdEnd-config.nodeIdStart);i++) {
			for(int j = 0;j<config.bucketSize;j++) {
				System.out.println("Entering loop for createinverted index + " + i+" "+j);
				if(replicaPosition(rt[j].hashIndex,nodeId.get(i),rt)) {
				hashforIndexTables.add(j+1);
				}
				else {
					hashforIndexTables.add(0);
				}
			}
			for(int k = 0;k<hashforIndexTables.size();k++) {
				if(hashforIndexTables.get(k)==k+1) {
					bits = bits+"1";
				}
				else {
					bits = bits+"0";
				}
			}
			
			InvertedIndexTableInstance e = new InvertedIndexTableInstance(nodeId.get(i),bits);
			indexInstance.add(e);
			bits = "";
			
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
	public ArrayList<Integer> populateNodeId() {
		ks = new ArrayList();
		DHTConfig config = ConfigLoader.config;
// Get live nodes
		for(int i = 0;i<=(config.nodeIdEnd-config.nodeIdStart);i++) {
			ks.add(config.nodeIdStart+i);
		}
		return ks;
	}
	boolean replicaPosition( int hashBucket, int nodeId,ElasticRoutingTableInstance[] rt) {
		int s;
		

		for(s=0;s<3;s++) {
			if(rt[hashBucket].nodeId.get(s)==nodeId) {
				return true;
			}
		}
		return false;
	}
	
}
