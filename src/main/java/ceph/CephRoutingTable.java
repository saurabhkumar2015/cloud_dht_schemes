package ceph;

import config.DHTConfig;

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
	
	public void AddNode(int clusterId, int nodeId)
	{
		mapInstance.AddExtraNodeToOsdMap(clusterId, nodeId);
	}
	
	public int GetNodeId(String fileName,int replicaId)
	{
		return mapInstance.findNodeWithRequestedReplica(replicaId,
				HashGenerator.getInstance().getPlacementGroupIdFromFileName(fileName, config.PlacementGroupMaxLimit));
	}
	
	
	private int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster)
	{
		// depth should be log base nodeperCluster (countOfNodes) ceiling value
		return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
	}
}
