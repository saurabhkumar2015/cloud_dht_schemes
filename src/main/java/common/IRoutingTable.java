package common;

public interface IRoutingTable {

    int getNodeId(String fileName, int replicaId);
    void addNode(int nodeId);
    void deleteNode(int nodeId);
    void loadBalance(int nodeId, double loadFactor);

}
