package ceph;

import common.IRoutingTable;
import config.DHTConfig;
import java.util.Random;

public class CephRoutingTable implements IRoutingTable {
	
	private DHTConfig config;
	
	private int depth;
	
	private OsdMap mapInstance;
	
	private static CephRoutingTable single_instance = null;
	
	public int VersionNo;
	
    private CephRoutingTable()
    {
    	this.config = new DHTConfig();
        this.depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart, config.cephMaxClusterSize);
        this.mapInstance = OsdMap.getInstance(config.cephMaxClusterSize, depth);
		this.VersionNo = 1;
        // BootStrap the table here or not need to think
        
        
    }
    
    
    public static CephRoutingTable getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new CephRoutingTable(); 
  
        return single_instance; 
    }
    
	public OsdMap GetCephRoutingTable()
	{
		return mapInstance;
	}
	
	public IRoutingTable addNode(int nodeId)
	{
		int clusterId = randomClusterNoGenerator();
		mapInstance.AddExtraNodeToOsdMap(clusterId, nodeId);
		this.VersionNo++;
		return CephRoutingTable.getInstance();
	}

	@Override
	public IRoutingTable deleteNode(int nodeId) {
		return null;
	}

	@Override
	public IRoutingTable loadBalance(int nodeId, double loadFactor) {
		return null;
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
		return r.nextInt(high-low) + low;
	}
}
