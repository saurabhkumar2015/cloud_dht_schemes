package ceph;

public class OsdNode {
	
	public Node headNode;
	
	public int clusterCountInLevel = 0;
	
	// Add node to that level of OSD Map
	public Node AddNode(double weight, int clusterId, int nodeid, int level)
	{
		// Increment the no of cluster after every addition
		this.clusterCountInLevel++;
		
		// check if the node level have any node added or not
		if(headNode == null)
		{
			Node node = new Node(weight,clusterId, nodeid, level);
			
			headNode = node;
			return node;
		}
		else
		{
		 Node tempNode = headNode;
		 while(tempNode.nextNode != null)
			 tempNode = tempNode.nextNode;
		 Node newNode = new Node(weight,clusterId, nodeid, level);
		 tempNode.nextNode = newNode;
		 
		 return newNode;
		}
		
	}
	
	public Node AddNodeAtStartOfList(double weight, int clusterId, int nodeid, int level)
	{
		// Increment the no of cluster after every addition
				this.clusterCountInLevel++;
				
				// check if the node level have any node added or not
				if(headNode == null)
				{
					Node node = new Node(weight,clusterId, nodeid, level);
					
					headNode = node;
					return node;
				}
				else
				{
				 Node tempNode = headNode;
				 Node newNode = new Node(weight,clusterId, nodeid, level);
				 newNode.nextNode = headNode;
				 headNode = newNode;
				 return newNode;
				}
	}
	
	public void IterateToFindOsdLocation()
	{
		
	}
		
	public int GetOsdNodeCount()
	{
		return this.clusterCountInLevel;
	}
	
	public void ShowCurrentNode(OsdNode currentNode)
	{
		if(currentNode == null || currentNode.headNode == null)
			return;
		currentNode.headNode.iterateNodeList(currentNode.headNode);
	}
	
}

