package ring;

public interface IDataNode {
	
	public void addNode(String nodeId);
	
	public void deleteNode(String nodeId);
	
	public void loadBalance(String nodeId, float loadFraction);
	
}
