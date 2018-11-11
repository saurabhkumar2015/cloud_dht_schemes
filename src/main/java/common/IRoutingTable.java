package common;

public interface IRoutingTable {

    public int getNodeId(String fileName, int replicaId);
    public void addNode(int nodeId);
    public void deleteNode(int nodeId);
    public void loadBalance(int nodeId, double loadFactor);

}
