package ceph;
import java.io.Serializable;
import java.util.List;

public class OsdNode implements Serializable {
	
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
		
	public int giveOsdNodeCount()
	{
		return this.clusterCountInLevel;
	}
	
	public void ShowCurrentNode(OsdNode currentNode, List<Integer> liveNodes, boolean isshow)
	{
		if(currentNode == null || currentNode.headNode == null)
			return;
		currentNode.headNode.iterateNodeList(currentNode.headNode, liveNodes, isshow);
	}

    @Override
    public String toString() {
        return "OsdNode{" +
                "headNode=" + headNode +
                ", clusterCountInLevel=" + clusterCountInLevel +
                '}';
    }
}

