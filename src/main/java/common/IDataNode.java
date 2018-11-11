package common;

public interface IDataNode {

    /**
     * modify IRoutingTable to include the new node
     * call writeFile on Data Nodes
     * call deleteFile on DataNodes
     * @param fileName
     * @param replicaId
     */
    void writeFile(String fileName, int replicaId);


    void deleteFile(String fileName);

    void addNode(int nodeId);

    void deleteNode(int nodeId);

    void loadBalance(int nodeId, double loadFraction);

}
