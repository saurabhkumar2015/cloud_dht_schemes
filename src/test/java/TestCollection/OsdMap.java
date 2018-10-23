package TestCollection;

import java.util.LinkedList;
import java.util.Queue;

public class OsdMap {
	
	private static OsdMap single_instance = null;
	
	public OsdNode root;
	
	public int maxclusterInlevel = 5;   // This value will come from Configuration file
	
	public Queue<OsdNode> Parentqueue = new LinkedList<>();
	
	
	
	// we can pass the configuration here
	public static OsdMap getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new OsdMap(); 
  
        return single_instance; 
    } 
	
	// Input to populate the osd map need to think
	public void AddNodeToOsdMap(double weight, int clusterId, int nodeId)
	{
		if(root == null)
		{
			OsdNode currentnode = new OsdNode();
			currentnode.AddNode(weight, clusterId, nodeId);
			
			// set root of the Osd map
			root = currentnode;
		}
	if(this.root.clusterCountInLevel < maxclusterInlevel)
	{
		this.root.AddNode(weight, clusterId, nodeId);
	}
	else
	{
			OsdNode leftNode = findTheClusterNodetoAddIterative(root.headNode.leftNode, root.headNode, 1);
			if(leftNode != null)
			{
				leftNode.AddNode(weight, clusterId, nodeId);
			}
	}
	}
	
   public void ShowOsdMap(OsdNode currentNode, int level)
   {
	   //System.out.println("Show the nodes of osd map");
	   
	   if(currentNode == null)
	   {
		   return;
	   }
	   if(root == null)
	   {
		   System.out.println("Osd map is empty");
		   return;
	   }
	   // show the current node 
	   currentNode.ShowCurrentNode(currentNode, level);
	   // iterate over the next node and show their child
	   Node tempNode = currentNode.headNode;
	   while(tempNode != null)
	   {
	     ShowOsdMap(tempNode.leftNode, level + 1);
	     tempNode = tempNode.nextNode;
	   }
   }
   
   public void FindNodeInOsdMap(int nodeId)
   {
	   _findNodeInOsdMap(root,nodeId, 1, true);
   }
   
   public void DeleteNode(int nodeId)
   {
	   _findNodeInOsdMap(root,nodeId, 1, false);
   }
   
   private void _findNodeInOsdMap(OsdNode node, int nodeId, int level, boolean isActive)
   {
	   if(node == null)
	   {
		   return;
	   }
	   if(root == null)
	   {
		   System.out.println("Osd map is empty");
		   return;
	   }
	   // show the current node 
	   boolean isNodeFound = _findNodeInOsdMapAtThisLevel(node,nodeId, level, isActive);
	   if(isNodeFound)
		   return;
	   // iterate over the next node and show their child
	   Node tempNode = node.headNode;
	   while(tempNode != null)
	   {
		 _findNodeInOsdMap(tempNode.leftNode,nodeId, level + 1, isActive);
	     tempNode = tempNode.nextNode;
	   }
   }
   
   private boolean _findNodeInOsdMapAtThisLevel(OsdNode node, int nodeId, int level, boolean isActiveStatus)
   {
	   Node currentNode = node.headNode;
	   while(currentNode != null)
	   {
		   if(currentNode.nodeId == nodeId)
		   {
			   currentNode.isActive = isActiveStatus;
			   System.out.println(String.format("NodeId = %d present at level = %d with Status = %s", nodeId, level, currentNode.isActive));
			   return true;
		   }
		   currentNode = currentNode.nextNode;
	   }
	   return false;
   }
   
   // Private member function
   private OsdNode findTheClusterNodetoAdd(OsdNode currentnode, Node parentnode, int level)
   {
	   if(level > 3)
		   return null;
	   
	   if(currentnode == null)
	   {
		   if(parentnode.leftNode == null) {	
			   OsdNode newlyAddedNode = new OsdNode();
			   parentnode.leftNode = newlyAddedNode;
			   
			   // Add this head node to next level parent queue
			   Parentqueue.add(newlyAddedNode);
			   
		   }
		   return currentnode;
		}
	   
	   if(currentnode.clusterCountInLevel < 5)
		   return currentnode;
	   
	   // Push Parent list to queue to do bfs traversal
	   OsdNode leftNode = null;
	  //if(parentnode.nextNode == null)
	   {
		   
  		   //parentnode = queue.remove().headNode;
		   leftNode = findTheClusterNodetoAdd(parentnode.nextNode.leftNode,parentnode.nextNode, level);
	   }
	   return leftNode;
	   
   }
   
   private OsdNode findTheClusterNodetoAddIterative(OsdNode currentnode, Node parentnode, int level)
   {
   
	   if(currentnode == null)
	   {
		   if(parentnode.leftNode == null) {	
			   OsdNode newlyAddedNode = new OsdNode();
			   parentnode.leftNode = newlyAddedNode;
			   
			   // Add this head node to next level parent queue
			   Parentqueue.add(newlyAddedNode);
			   
		   }
		   return currentnode;
		}
	   
	   if(currentnode.clusterCountInLevel < maxclusterInlevel)
		   return currentnode;
	   
	   // if above condition not satisfied then we need to move to next node of the parent level
	   Node nextParent = parentnode.nextNode;
	   while(nextParent != null)
	   {
		   OsdNode nextChildNode = null;
		    nextChildNode = nextParent.leftNode;
		   if(nextChildNode == null)
		   {
			   OsdNode newlyAddedNode = new OsdNode();
			   nextParent.leftNode = newlyAddedNode;
			   Parentqueue.add(newlyAddedNode);
			   return newlyAddedNode;
		   }
		   else if(nextChildNode.clusterCountInLevel < maxclusterInlevel)
			   return nextChildNode;
		   else
		   {
			   nextParent = nextParent.nextNode;
		   }
	   }
	   
	    //if we reach hear means level 2 is fill , now we need to fill level 3	
	   for(OsdNode nextlevelParent: Parentqueue )
	   {
		 // create the left node if not present
	     if(nextlevelParent.headNode.leftNode == null)
	     {
	    	   OsdNode newlyAddedNode = new OsdNode();
	    	   nextlevelParent.headNode.leftNode = newlyAddedNode;
	    	   return newlyAddedNode;
	     }
	     else if(nextlevelParent.headNode.leftNode.clusterCountInLevel < maxclusterInlevel)
			   return nextlevelParent.headNode.leftNode;
	     else
	     {
	    	 Node nextNodeInLevel = nextlevelParent.headNode.nextNode;
	    	 while(nextNodeInLevel != null)
	    	 {
	    		// create the left node if not present
	    	     if(nextNodeInLevel.leftNode == null)
	    	     {
	    	    	   OsdNode newlyAddedNode = new OsdNode();
	    	    	   nextNodeInLevel.leftNode = newlyAddedNode;
	    	    	   return newlyAddedNode;
	    	     }
	    	     else if(nextNodeInLevel.leftNode.clusterCountInLevel < maxclusterInlevel)
	    			   return nextNodeInLevel.leftNode;
	    	     else
	    	    	 nextNodeInLevel = nextNodeInLevel.nextNode;
	    	 }
	     }
	   
	   }
	   return null;
	   
   }

}
