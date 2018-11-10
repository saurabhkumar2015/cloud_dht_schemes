package TestCollection;

import config.DHTConfig;

public class CephRoutingTable {
	
	private DHTConfig config;
	
	private int depth;
	
	private OsdMap mapInstance;
	
    private CephRoutingTable()
    {
    	this.config = new DHTConfig();
        this.depth = FindDepthOfOsdMap(config.numNodeIds, config.cephMaxClusterSize);
        this.mapInstance = OsdMap.getInstance(config.cephMaxClusterSize, depth);
        // BootStrap the table here or not need to think
        
        
    }
    
	public OsdMap GetCephRoutingTable()
	{
		return mapInstance;
	}
	
	public void AddNode(int clusterId, int nodeId)
	{
		mapInstance.AddExtraNodeToOsdMap(clusterId, nodeId);
	}
	
	
	private int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster)
	{
		// depth should be log base nodeperCluster (countOfNodes) ceiling value
		return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
	}
}
