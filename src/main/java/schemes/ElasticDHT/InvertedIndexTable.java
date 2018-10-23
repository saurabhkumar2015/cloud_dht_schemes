package schemes.ElasticDHT;

import java.util.ArrayList;

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
		
		// Iterate over the routing table Instance and Create the Inverted Index table accordingly
		
		
		
	}
}
