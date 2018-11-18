package ceph;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class CephRoutingTable implements IRoutingTable, Serializable {

    private DHTConfig config;

    private int depth;

    public OsdMap mapInstance;

    private static CephRoutingTable single_instance = null;

    public int VersionNo;

    public CephRoutingTable() {
        this.config = ConfigLoader.config;
        this.depth = FindDepthOfOsdMap(config.nodeIdEnd - config.nodeIdStart, config.cephMaxClusterSize);
        this.mapInstance = OsdMap.getInstance(config.cephMaxClusterSize, depth);
        this.VersionNo = 1;
        // BootStrap the table here or not need to think


    }


    public static CephRoutingTable getInstance() {
        if (single_instance == null)
            single_instance = new CephRoutingTable();

        return single_instance;
    }

    public OsdMap GetCephRoutingTable() {
        return mapInstance;
    }

    public IRoutingTable addNode(int nodeId) {
        int clusterId = randomClusterNoGenerator();
        mapInstance.AddExtraNodeToOsdMap(clusterId, nodeId);
        this.VersionNo++;
        return CephRoutingTable.getInstance();
    }


    public int getNodeId(String fileName, int replicaId) {
        return mapInstance.findNodeWithRequestedReplica(replicaId,
                HashGenerator.getInstance().getPlacementGroupIdFromFileName(fileName, config.PlacementGroupMaxLimit));
    }


    private int FindDepthOfOsdMap(int countOfNodes, int nodePerCluster) {
        // depth should be log base nodeperCluster (countOfNodes) ceiling value
        return (int) Math.ceil((Math.log(countOfNodes) / Math.log(nodePerCluster)));
    }

    private int randomClusterNoGenerator() {
        Random r = new Random();
        int low = 1;
        int high = 21;
        return r.nextInt(high - low) + low;
    }


	public IRoutingTable deleteNode(int nodeId) {
		// TODO Auto-generated method stub
		mapInstance.DeleteNode(nodeId);
		this.VersionNo++;
        return this;
	}


	public IRoutingTable loadBalance(int nodeId, double loadFactor) {
		// First find the node and change the load on the node by loadfactor
		Node nodeToBeBalance = mapInstance.FindNodeInOsdMap(nodeId);
		double initialWeight = nodeToBeBalance.weight;
		nodeToBeBalance.weight = nodeToBeBalance.weight * loadFactor;
		System.out.println("The load of node with nodeId " + nodeId + " changed from " + initialWeight + " to " + nodeToBeBalance.weight);

		// File movement will be handle by Proxy node to send request to each data node for checking their local file.
		Node headNodeOfCluster = mapInstance.findHeadNodeOfTheCluster(nodeId);
		mapInstance.MoveFileInClusterOnNewNodeAddition(headNodeOfCluster);
		this.VersionNo++;
		return this;
	}

    public List<Integer> getLiveNodes() {
        return mapInstance.GetLiveNodes();
    }

    public void printRoutingTable() {

    	this.mapInstance.ShowOsdMap(true);
    }


	public long getVersionNumber() {
		return this.VersionNo;
	}
    
    
}
