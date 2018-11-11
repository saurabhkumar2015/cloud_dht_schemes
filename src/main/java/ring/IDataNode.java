package ring;

public interface IDataNode {
	
	public void addNode(int nodeId);
	
	public void deleteNode(int nodeId);
	
	public void loadBalance(int nodeId, double loadFraction);
	
}
