package ceph;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import common.Commons;
import common.Constants;
import config.ConfigLoader;
import javafx.util.Pair;
import java.io.Serializable;

public class OsdMap implements Serializable{
	
	private static OsdMap single_instance = null;
	
	public OsdNode root= null;
	
	public Node foundNode = null;
	
	public int maxclusterInlevel;   // This value will come from Configuration file
	
	public int depthofOsdMap;  // This is the calculated value using #node & #clusterSize
	
	public List<Integer> liveNodes = new LinkedList<Integer>();
	
	public Queue<OsdNode> Parentqueue = new LinkedList<>();
	
	public HashGenerator hashGenerator;
	
	private OsdMap(int maxclusterInaNode, int depth)
	{
		this.maxclusterInlevel = maxclusterInaNode;
		this.depthofOsdMap = depth;
		hashGenerator = HashGenerator.getInstance();
	}
	
	
	// we can pass the configuration here
	public static OsdMap getInstance(int maxclusterInaNode, int depth) 
    { 
        if (single_instance == null) 
            single_instance = new OsdMap(maxclusterInaNode, depth); 
  
        return single_instance; 
    } 
	
	// Input to populate the osd map need to think
    public void AddNodeToOsdMap(int clusterId, int nodeId)
	{
		if(root == null)
		{
			OsdNode currentnode = new OsdNode();
			if(depthofOsdMap == 1)
			currentnode.AddNode(hashGenerator.randomWeightGenerator(), clusterId, nodeId, 1);
			else
				currentnode.AddNode(0, clusterId, -1, 1);
			
			// set root of the Osd map
			root = currentnode;
		}
		else if(this.root.clusterCountInLevel < maxclusterInlevel)
	   {
		if(depthofOsdMap == 1)
		   this.root.AddNode(hashGenerator.randomWeightGenerator(), clusterId, nodeId,1);
		else
			this.root.AddNode(0, clusterId, -1,1);
	   }
	else
	{
			ParentChildPair leftNodePair = findTheClusterNodetoAddIterative(root.headNode.leftNode, root.headNode, 1);
			if(leftNodePair != null)
			{
				Integer value = leftNodePair.level;
				if(value == depthofOsdMap)
				{
					if(leftNodePair != null)
					{
						OsdNode internalKey = leftNodePair.Child;
						if(internalKey != null)
						{
							double weight = hashGenerator.randomWeightGenerator();
						    internalKey.AddNode(weight, clusterId, nodeId,value);
						}
					}
				}
				else
				{
					if(leftNodePair != null)
					{
						OsdNode internalKey = leftNodePair.Child;
						if(internalKey != null)
						internalKey.AddNode(0, clusterId, -1,value);
					}
				}
			}
	}
	}
	
    // Add extra node after OSD Map Creation
    public void AddExtraNodeToOsdMap(int clusterId, int nodeId)
    {
    	// Guard Clause for root node new node added to root and logic is totally different
    	// Verbose Mode on. Print the log of new node addition
    	System.out.println("New node add request triggered with NodeId: " + nodeId);
    	if(root.clusterCountInLevel < maxclusterInlevel)
    	{
    			Node newlyAddedNode = root.AddNodeAtStartOfList(hashGenerator.randomWeightGenerator(), clusterId, nodeId,1);
    			MoveFileInClusterOnNewNodeAddition(newlyAddedNode);
    	    	return;
    	}
    	ParentChildPair leftNodePair = findTheClusterNodetoAddIterative(root.headNode.leftNode, root.headNode, 1);
		if(leftNodePair != null)
		{
			Integer value = leftNodePair.level;
			OsdNode internalKey = leftNodePair.Child;
			if(value == depthofOsdMap)
			{
				
       			Node newlyAddedNode = internalKey.AddNodeAtStartOfList(hashGenerator.randomWeightGenerator(), clusterId, nodeId,value);
				
				// Now need to set the Osd Map pointer to the newly added node as cluster start point
       			leftNodePair.Parent.leftNode.headNode = newlyAddedNode;
				// Need to move the files from other node in sub cluster
				MoveFileInClusterOnNewNodeAddition(newlyAddedNode);
			}
			else
				internalKey.AddNode(0, clusterId, -1,value);				
		}
    }
    
    public void MoveFileInClusterOnNewNodeAddition(Node newlyAddedNode)
    {
    	// Generally the data nodes should contain the local copies of files
    	// excact file match should run on those Data nodes.
    	// iterate over the cluster and print which Data nodes need to move data
    	Node tempNode = newlyAddedNode;
    	while(tempNode != null)
    	{
    		System.out.println("Files need to moves from Data Node: " + tempNode.nodeId);
    		tempNode = tempNode.nextNode;
    	}
    }
    
    public void MoveFileInClusterOnNodeDeleteion(Node headNode, Node excludedNode)
    {
    	// Generally the data nodes should contain the local copies of files
    	// excact file match should run on those Data nodes.
    	// iterate over the cluster and print which Data nodes need to move data
    	Node tempNode = headNode;
    	while(tempNode != null)
    	{
    		if(tempNode.nodeId != excludedNode.nodeId)
    		System.out.println("Files need to moves from Data Node: " + tempNode.nodeId);
    		tempNode = tempNode.nextNode;
    	}
    }
    
    public void ShowOsdMap(boolean isShow)
    {
    	liveNodes.clear();
    	_showOsdMap(root, isShow);
    	liveNodes.clear();
    }
    
    public List<Integer> GetLiveNodes()
    {
    	liveNodes.clear();
    	_showOsdMap(root, false);
    	return liveNodes;
    }
    
    // Find the given nodeId in the OsdMap
    public Node FindNodeInOsdMap(int nodeId)
    {
 	   this.foundNode = null;
 	   _findNodeInOsdMap(root,nodeId, 1, true);
 	   return this.foundNode;
    }
    
    // Set the activation flag false for given nodeId
    public void DeleteNode(int nodeId)
    {
 	   _findNodeInOsdMap(root,nodeId, 1, false);
 	   Node headNodeOfCluster = this.findHeadNodeOfTheCluster(nodeId);
 	   Node deleteNode = this.FindNodeInOsdMap(nodeId);
 	   // We need to check the files to moves from these nodes.
 	   this.MoveFileInClusterOnNodeDeleteion(headNodeOfCluster, deleteNode);
    }
    
    // Once we have weight distrubuted for leaf node, then populate weight from leaf to root
    public void PopulateWeightOfInternalNode(OsdNode node)
    {
 	   _populateWeight(root);
    }
   
    // Find the node containing given file with replication value
    // Ceph Algorithm for Finding NodeId for given filename and replica
    public int findNodeWithRequestedReplica(int replicaId, int placementGroupId)
    {
 	 //System.out.println("The placement Group for file: " +  fileName + " is : " + placementGroupId + " with replicaId: " + replicaId);
 	 int nodeId =  _findNodeWithRequestedReplica(root, placementGroupId, replicaId, 1);
 	 return nodeId;
    }
    
    public Node findHeadNodeOfTheCluster(int nodeId)
    {
 	   this.foundNode = null;
 	   _findNodeInOsdMap(root,nodeId, 1, true);
 	   Node tempNode = this.foundNode;
 	   while(tempNode.prevNode != null)
 	   {
 		   tempNode = tempNode.prevNode;
 	   }
 	   return tempNode;
    }
    public void AddFileToCephSystem(String fileName, int replicaId, int placementGroupSize)
    {
 	   int placementGroupId = hashGenerator.getPlacementGroupIdFromFileName(fileName, placementGroupSize);
 	   int nodeId = findNodeWithRequestedReplica(replicaId, placementGroupId); 

 	   // we can add files to CephDataNode
 	    CephDataNode.getInstance(nodeId).writeFile(fileName,replicaId);
    }
    
    // Traverse the OSD map and show the snapshot 
   private void _showOsdMap(OsdNode currentNode, boolean isshow)
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
	   currentNode.ShowCurrentNode(currentNode, this.liveNodes, isshow);
	   // iterate over the next node and show their child
	   Node tempNode = currentNode.headNode;
	   while(tempNode != null)
	   {
	     _showOsdMap(tempNode.leftNode, isshow);
	     tempNode = tempNode.nextNode;
	   }
   }
      
   private int _findNodeWithRequestedReplica(OsdNode headNode, int placementGroupId, int replicaId, int level)
   {
	   double totalclusterSum = sumofClusterNode(headNode.headNode);
	   
	   // Now find the hash value of each node in cluster and follow the path
	   // where the hashvalue is less than node weight / Total cluster weight
	   
	   Node tempNode = headNode.headNode;
	   double subClusterSum = totalclusterSum;
	   while(tempNode != null)
	   {
		   double hashval = hashGenerator.generateHashValue(tempNode.clusterId, placementGroupId, replicaId);
		   double weightFactor = hashGenerator.GetWeightFactor(tempNode.weight, subClusterSum);
		   if(hashval < weightFactor)
			   break;
		   subClusterSum = subClusterSum - tempNode.weight;
		   tempNode = tempNode.nextNode;
	   }
	   
	   if(tempNode != null && level < depthofOsdMap)
		   return _findNodeWithRequestedReplica(tempNode.leftNode,placementGroupId, replicaId, level + 1);
	   if(tempNode != null && level == depthofOsdMap && tempNode.isActive)
		   return tempNode.nodeId;
	   else
		   return -2;
   }
   
   private double _populateWeight(OsdNode node)
   {
	   if(node != null && node.headNode.level == depthofOsdMap)
	   {
		   return sumofClusterNode(node.headNode);
	   }
	   if(node == null)
		   return 0;
	   Node tempNode = node.headNode;
	   double sum = 0;
	   while(tempNode != null)
	   {
		   tempNode.weight = _populateWeight(tempNode.leftNode);
		   sum += tempNode.weight;
		   tempNode = tempNode.nextNode;
	   }
	   return sum;
   }
   
   private double sumofClusterNode(Node node)
   {
	   Node temp = node;
	   double sum = 0;
	   while(temp != null)
	   {
		   sum = sum + temp.weight;
		   temp = temp.nextNode;
	   }
	   return sum;
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
	   boolean foundNode = _findNodeInOsdMapAtThisLevel(node,nodeId, level, isActive);
	   if(foundNode)
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
			   this.foundNode = currentNode;
			   return true;
		   }
		   currentNode = currentNode.nextNode;
	   }
	   return false;
   }
   
   // Private member function
   private ParentChildPair findTheClusterNodetoAddIterative(OsdNode currentnode, Node parentnode, int level)
   {
   
	   if(currentnode == null)
	   {
		   if(parentnode.leftNode == null) {	
			   OsdNode newlyAddedNode = new OsdNode();
			   parentnode.leftNode = newlyAddedNode;
			   
			   // Add this head node to next level parent queue
			   Parentqueue.add(newlyAddedNode);
		   return new ParentChildPair(newlyAddedNode,parentnode, parentnode.level +1);
		}
	   }
	   if(currentnode.clusterCountInLevel < maxclusterInlevel)
		   return new ParentChildPair(currentnode,parentnode, parentnode.level+1);
	   
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
			   return new ParentChildPair(newlyAddedNode,nextParent, nextParent.level + 1);
		   }
		   else if(nextChildNode.clusterCountInLevel < maxclusterInlevel)
			   return new ParentChildPair(nextChildNode,nextParent, nextParent.level + 1);
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
	    	   Parentqueue.add(newlyAddedNode);
	    	   return new ParentChildPair(newlyAddedNode,nextlevelParent.headNode, nextlevelParent.headNode.level + 1);
	     }
	     else if(nextlevelParent.headNode.leftNode.clusterCountInLevel < maxclusterInlevel)
			   return new ParentChildPair(nextlevelParent.headNode.leftNode,nextlevelParent.headNode, nextlevelParent.headNode.level + 1);
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
	    	    	   Parentqueue.add(newlyAddedNode);
	    	    	   return new ParentChildPair(newlyAddedNode,nextNodeInLevel, nextNodeInLevel.level + 1);
	    	     }
	    	     else if(nextNodeInLevel.leftNode.clusterCountInLevel < maxclusterInlevel)
	    			   return new ParentChildPair(nextNodeInLevel.leftNode,nextNodeInLevel, nextNodeInLevel.level + 1);
	    	     else
	    	    	 nextNodeInLevel = nextNodeInLevel.nextNode;
	    	 }
	     }
	   
	   }
	   return null;
	   
   }
   }
