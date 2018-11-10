package ceph;

import config.DHTConfig;
import java.util.Random;

public class CephRoutingTable {
	
	private DHTConfig config;
	
	private int depth;
	
	private OsdMap mapInstance;
	
	private static CephRoutingTable single_instance = null;
	
    private CephRoutingTable()
    {
    	this.config = new DHTConfig();
        this.depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart, config.cephMaxClusterSize);
        this.mapInstance = OsdMap.getInstance(config.cephMaxClusterSize, depth);
        // BootStrap the table here or not need to think
        
        
    }
    
    
    public static CephRoutingTable getInstance(int maxclusterInaNode, int depth) 
    { 
        if (single_instance == null) 
            single_instance = new CephRoutingTable(); 
  
        return single_instance; 
    }
    
	public OsdMap GetCephRoutingTable()
	{
		return mapInstance;
	}
	
	public void addNode(int nodeId)
	{
		int clusterId = randomClusterNoGenerator();
		mapInstance.AddExtraNodeToOsdMap(clusterId, nodeId);
	}
	
	public int getNodeId(String fileName, int replicaId)
	{
		return mapInstance.findNodeWithRequestedReplica(replicaId,
				HashGenerator.getInstance().getPlacementGroupIdFromFileName(fileName, config.PlacementGroupMaxLimit));
	}
	
	
	private int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster)
	{
		// depth should be log base nodeperCluster (countOfNodes) ceiling value
		return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
	}
	
	private int randomClusterNoGenerator()
	{
		Random r = new Random();
		int low = 1;
		int high = 21;
		int result = r.nextInt(high-low) + low;
		return result;
	}
}
