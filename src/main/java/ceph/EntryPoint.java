package ceph;

import java.util.Random;

import config.ConfigLoader;
import config.DHTConfig;

public class EntryPoint {

	public void BootStrapCeph() {
     // BootStrapping
     // Step 1 : using no of nodes and cluster size from configuration find the depth of Osd Map
     DHTConfig config = ConfigLoader.config;
     int depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart + 1, config.cephMaxClusterSize);
     System.out.println("Osd Depth is :" + depth);
     // Step 2 : create the Osd Map with Leaf node with weight 
     // Populate the osd map from Configuration
      OsdMap mapInstance = OsdMap.getInstance(config.cephMaxClusterSize,depth);
     
      // 
     // Step 3: populate the internal node weight using the commutative weight of child node
     // step 3.a Populate OsdMap
      PopulateOsdMap(mapInstance, config.nodeIdEnd - config.nodeIdStart + 1, depth,config.cephMaxClusterSize );
      
     //set the internal node weight
      mapInstance.PopulateWeightOfInternalNode(mapInstance.root);
     // Till this Point Osd Map is populated and build properly.
     // Find the node containing PG = 5 & replication 2 I can change the name to add file : TODO : Refactor this code
      AddFilesToCephSystem();
      
	}
	
	public void AddFilesToCephSystem()
	{
		DHTConfig config = new DHTConfig();
		int depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart, config.cephMaxClusterSize);
		OsdMap mapInstance = OsdMap.getInstance(config.cephMaxClusterSize,depth);
		
		for(int i = 0; i < 1000; i++)
		{
			Random r = new Random();
			int replicaId = r.nextInt(3) + 1;
			String fileName = "CloudComputing" + i;
			mapInstance.AddFileToCephSystem(fileName, replicaId, config.PlacementGroupMaxLimit);
		}
		
	}
	
	public void PopulateOsdMap(OsdMap mapInstance, int nodeIds, int depth, int clusterSize)
	{
		for(int i = 1; i < depth;i++)
		{
			// populate internal node
			for(int j = 0; j< Math.pow(clusterSize,i); j++)
			{
				mapInstance.AddNodeToOsdMap(randomClusterNoGenerator(), -1);
			}
		}
		// Insert regular data node
		for(int k = 1; k<= nodeIds; k++)
		{
			mapInstance.AddNodeToOsdMap(randomClusterNoGenerator(), k);
		}
	}

	public int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster)
	{
		// depth should be log base nodeperCluster (countOfNodes) ceiling value
		return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
	}
	
	public int randomClusterNoGenerator()
	{
		Random r = new Random();
		int low = 1;
		int high = 21;
		int result = r.nextInt(high-low) + low;
		return result;
	}
}
