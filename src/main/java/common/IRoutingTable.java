package common;

import java.util.List;

public interface IRoutingTable {

    int getNodeId(String fileName, int replicaId);
    IRoutingTable addNode(int nodeId);
    IRoutingTable deleteNode(int nodeId);
    IRoutingTable loadBalance(int nodeId, double loadFactor);
    List<Integer> getLiveNodes();
    void printRoutingTable();
    long getVersionNumber();


}
