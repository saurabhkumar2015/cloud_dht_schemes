package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.BitSet;

public class InvertedIndexTable {

	public ArrayList<InvertedIndexTableInstance> indexInstance = new ArrayList<InvertedIndexTableInstance>();
	
	// Create a singleton for InvertedIndexTable
	private static InvertedIndexTable single_instance = null;

	private int hashforIndexTables[]; 
	
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
		int nodeId[] = {1,2,3,4,5,6,7};
		String bits="";
		hashforIndexTables = null;
		for(int i = 0;i<7;i++) {
			for(int j = 0;j<7;j++) {
				if(nodeId[i]==rt[j].nodeId1||nodeId[i]==rt[j].nodeId2||nodeId[i]==rt[j].nodeId3) {
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
	
}
