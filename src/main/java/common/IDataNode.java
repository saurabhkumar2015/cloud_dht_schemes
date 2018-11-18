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

	public void addNode(int nodeId);
	
	public void deleteNode(int nodeId);
	
	public void loadBalance(int nodeId, double loadFraction);
	
	public void MoveFiles(int clusterIdofNewNode,String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance);
	
	public void UpdateRoutingTable(IRoutingTable cephrtTable);

	public IRoutingTable getRoutingTable();

}

