package ceph;
import java.io.Serializable;

public class ParentChildPair implements Serializable{

	public OsdNode Child;
	
	public Node Parent;
	
	public int level;
	
	public ParentChildPair(OsdNode child, Node Parent, int level)
	{
		this.Child = child;
		this.Parent = Parent;
		this.level = level;
	}
}
