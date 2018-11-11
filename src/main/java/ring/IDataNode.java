package ring;

public interface IDataNode {
	
	void addNode(int nodeId);
	
	void deleteNode(int nodeId);
	
	void loadBalance(int nodeId, double loadFraction);
	
}
