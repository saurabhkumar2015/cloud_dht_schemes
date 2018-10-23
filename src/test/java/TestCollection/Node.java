package TestCollection;

public class Node {
  // each cell in the OsdNode is a node
	public double weight;
    
	public int clusterId;
	
	public int nodeId;
	
    public Node nextNode;
    
    public OsdNode leftNode;
    
    public boolean isActive;
    
  //  public OsdNode rightNode;
    
    public Node(double weight, int clusterId, int nodeid)
    {
    	this.weight = weight;
    	this.nodeId = nodeid;
    	this.clusterId = clusterId;
    	this.nextNode = null;
    	this.leftNode = null;
        this.isActive = true;
    }
    
    public void iterateNodeList(Node headNode, int level)
    {
    	if(headNode == null)
    	{
    		System.out.println("node list is empty");
    		return;
    	}
    	Node temp = headNode;
    	while(temp != null)
    	{
    		System.out.println(String.format("weight= %f ClusterId= %d nodeId = %d IsActive = %s at level = %d", temp.weight,temp.clusterId, temp.nodeId,temp.isActive,level));
    		temp = temp.nextNode;
    	}
    }
}
