package ceph;

import java.io.Serializable;
import java.util.List;
import common.IRoutingTable;
import config.ConfigLoader;

public class CephRoutingTable implements IRoutingTable, Serializable {

    public OsdMap mapInstance;

    private static CephRoutingTable single_instance = null;

    public int VersionNo;

    public CephRoutingTable() {
        this.mapInstance = OsdMap.getInstance(ConfigLoader.config.cephMaxClusterSize, 3);
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
        mapInstance.AddExtraNodeToOsdMap(nodeId);
        mapInstance.PopulateWeightOfInternalNode();
        this.VersionNo++;
        return CephRoutingTable.getInstance();
    }


    public int getNodeId(String fileName, int replicaId) {
        return mapInstance.findNodeWithRequestedReplica(replicaId,
                HashGenerator.getInstance().getPlacementGroupIdFromFileName(fileName, ConfigLoader.config.PlacementGroupMaxLimit));
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
		
		this.VersionNo++;
		// Populate the internalNode weight
		mapInstance.PopulateWeightOfInternalNode();
		return this;
	}

    public List<Integer> getLiveNodes() {
        return mapInstance.GetLiveNodes();
    }

    public void printRoutingTable() {

    	this.mapInstance.ShowOsdMap();
    }


	public long getVersionNumber() {
		return this.VersionNo;
	}
    
    
}
