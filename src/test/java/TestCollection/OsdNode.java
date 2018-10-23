package TestCollection;

public class OsdNode {
	
	public Node headNode;
	
	public int clusterCountInLevel = 0;
	
	// Add node to that level of OSD Map
	public void AddNode(double weight, int clusterId, int nodeid)
	{
		// Increment the no of cluster after every addition
		this.clusterCountInLevel++;
		
		// check if the node level have any node added or not
		if(headNode == null)
		{
			Node node = new Node(weight,clusterId, nodeid);
			
			headNode = node;
		}
		else
		{
		 Node tempNode = headNode;
		 while(tempNode.nextNode != null)
			 tempNode = tempNode.nextNode;
		 Node newNode = new Node(weight,clusterId, nodeid);
		 tempNode.nextNode = newNode;
		}
	}
	
	public void IterateToFindOsdLocation()
	{
		
	}
		
	public int GetOsdNodeCount()
	{
		return this.clusterCountInLevel;
	}
	
	public void ShowCurrentNode(OsdNode currentNode, int level)
	{
		if(currentNode == null || currentNode.headNode == null)
			return;
		currentNode.headNode.iterateNodeList(currentNode.headNode, level);
	}
	
}
