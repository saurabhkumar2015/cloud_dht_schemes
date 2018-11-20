package ceph;

import java.io.Serializable;
import java.util.List;
import common.IRoutingTable;
import config.ConfigLoader;

import static common.Commons.cephRoutingTable;

public class CephRoutingTable implements IRoutingTable, Serializable {

    public OsdMap mapInstance;

    public int versionNumber;

    public CephRoutingTable() {}

    public static CephRoutingTable giveInstance() {
        if (cephRoutingTable == null) {
            cephRoutingTable = new CephRoutingTable();
            cephRoutingTable.mapInstance = OsdMap.giveInstance(ConfigLoader.config.cephMaxClusterSize, 3);
            cephRoutingTable.versionNumber = 1;
        }
        return cephRoutingTable;
    }

    public OsdMap GetCephRoutingTable() {
        return mapInstance;
    }

    public IRoutingTable addNode(int nodeId) {
        mapInstance.AddExtraNodeToOsdMap(nodeId);
        mapInstance.PopulateWeightOfInternalNode();
        this.versionNumber++;
        return CephRoutingTable.giveInstance();
    }


    public int giveNodeId(String fileName, int replicaId) {
        return mapInstance.findNodeWithRequestedReplica(replicaId,
                HashGenerator.giveInstance().givePlacementGroupIdFromFileName(fileName, ConfigLoader.config.PlacementGroupMaxLimit));
    }

	public IRoutingTable deleteNode(int nodeId) {
		// TODO Auto-generated method stub
		mapInstance.DeleteNode(nodeId);
		this.versionNumber++;
        return this;
	}


	public IRoutingTable loadBalance(int nodeId, double loadFactor) {
		// First find the node and change the load on the node by loadfactor
		Node nodeToBeBalance = mapInstance.FindNodeInOsdMap(nodeId);
		double initialWeight = nodeToBeBalance.weight;
		nodeToBeBalance.weight = nodeToBeBalance.weight * loadFactor;
		System.out.println("The load of node with nodeId " + nodeId + " changed from " + initialWeight + " to " + nodeToBeBalance.weight);
		
		this.versionNumber++;
		// Populate the internalNode weight
		mapInstance.PopulateWeightOfInternalNode();
		return this;
	}

    public List<Integer> giveLiveNodes() {
        return mapInstance.giveLiveNodes();
    }

    public void printRoutingTable() {

    	this.mapInstance.ShowOsdMap();
    }


	public long getVersionNumber() {
		return this.versionNumber;
	}


    @Override
    public String toString() {
        return "CephRoutingTable{" +
                "mapInstance=" + mapInstance +
                ", versionNumber=" + versionNumber +
                '}';
    }
}
