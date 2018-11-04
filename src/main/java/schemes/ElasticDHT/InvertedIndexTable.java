package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.BitSet;

import schemes.ElasticDHT.*;

public class InvertedIndexTable {

	private ArrayList<InvertedIndexTableInstance> indexInstance = new ArrayList<InvertedIndexTableInstance>();
	
	// Create a singleton for InvertedIndexTable
	private static InvertedIndexTable single_instance = null; 
	
	private InvertedIndexTable()
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
		int hashforIndexTable[] = null;
		for(int i = 0;i<7;i++) {
			for(int j = 0;j<7;j++) {
				if(nodeId[i]==rt[j].nodeId1||nodeId[i]==rt[j].nodeId2||nodeId[i]==rt[j].nodeId3) {
					hashforIndexTable[j] = j+1; 
				}
				else {
					hashforIndexTable[j] = j;
				}
			}
			for(int k = 0;k<hashforIndexTable.length;k++) {
				if(hashforIndexTable[k]==k+1) {
					bits = bits+"0";
				}
				else {
					bits = bits+"1";
				}
			}
			BitSet b = fromString(bits);
			InvertedIndexTableInstance e = new InvertedIndexTableInstance(nodeId[i],b);
			boolean add = indexInstance.add(e);
			
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
