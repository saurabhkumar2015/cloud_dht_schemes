package common;

public interface IRoutingTable {

    public int getNodeId(String fileName, int replicaId);
    public IRoutingTable addNode(int nodeId);
    public IRoutingTable deleteNode(int nodeId);
    public IRoutingTable loadBalance(int nodeId, double loadFactor);

}
