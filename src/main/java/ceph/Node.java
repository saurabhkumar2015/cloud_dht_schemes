package ceph;

import java.io.Serializable;
import java.util.List;

public class Node implements Serializable {
  // each cell in the OsdNode is a node
	public double weight;
    
	public int clusterId;
	
	public int nodeId;
	
    public Node nextNode;
    
    public Node prevNode;
    
    public OsdNode leftNode;
    
    public boolean isActive;
    
    public int level;
    
  //  public OsdNode rightNode;
    
    public Node(double weight, int clusterId, int nodeid, int level)
    {
    	this.weight = weight;
    	this.nodeId = nodeid;
    	this.clusterId = clusterId;
    	this.nextNode = null;
        this.prevNode = null;
    	this.leftNode = null;
        this.isActive = true;
        this.level = level;
    }
    
    public void iterateNodeList(Node headNode,  List<Integer> liveNodes, boolean isshow)
    {
    	if(headNode == null)
    	{
    		System.out.println("node list is empty");
    		return;
    	}
    	Node temp = headNode;
    	while(temp != null)
    	{
    		if(!isshow)
    		{
    		if(temp.isActive && temp.nodeId != -1)
    			liveNodes.add(temp.nodeId);
    		}
    		else
    		{
    			if(temp.weight != 0)
    		System.out.println(String.format("weight= %f ClusterId= %d nodeId = %d IsActive = %s at level = %d", temp.weight,temp.clusterId, temp.nodeId,temp.isActive,temp.level));
    		}
    		temp = temp.nextNode;
    	}
    }
}
