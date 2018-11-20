package ceph;

import config.ConfigLoader;
import config.DHTConfig;

public class EntryPoint {

	public void BootStrapCeph() {
     // BootStrapping
     // Step 1 : using no of nodes and cluster size from configuration find the depth of Osd Map
     DHTConfig config = ConfigLoader.config;
     int depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart + 1, config.cephMaxClusterSize);
     System.out.println("Osd Depth is :" + 3);
     // Step 2 : create the Osd Map with Leaf node with weight 
     // Populate the osd map from Configuration
      OsdMap mapInstance = OsdMap.giveInstance(config.cephMaxClusterSize,3);
     
      // 
     // Step 3: populate the internal node weight using the commutative weight of child node
     // step 3.a Populate OsdMap
      PopulateOsdMap(mapInstance, config.nodeIdEnd - config.nodeIdStart + 1, depth,config.cephMaxClusterSize );
      
     //set the internal node weight
      mapInstance.PopulateWeightOfInternalNode();
     
	}
	
	public void PopulateOsdMap(OsdMap mapInstance, int nodeIds, int depth, int clusterSize)
	{
		for(int i = 1; i < 3;i++)
		{
			// populate internal node
			for(int j = 0; j< Math.pow(clusterSize,i); j++)
			{
				mapInstance.AddNodeToOsdMap(-1);
			}
		}
		// Insert regular data node
		for(int k = 1; k<= nodeIds; k++)
		{
			mapInstance.AddNodeToOsdMap(k);
		}
	}

	public int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster)
	{
		// depth should be log base nodeperCluster (countOfNodes) ceiling value
		return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
	}
}
