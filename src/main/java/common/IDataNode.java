package common;

public interface IDataNode {

	/**
	 * modify IRoutingTable to include the new node
	 * call writeFile on Data Nodes
	 * call deleteFile on DataNodes
	 * @param fileName
	 * @param replicaId
	 */
	public void writeFile(String fileName, int replicaId);


	public void deleteFile(String fileName);

	public void addNode(String nodeId);
	
	public void deleteNode(String nodeId);
	
	public void loadBalance(String nodeId, float loadFraction);
	
}
