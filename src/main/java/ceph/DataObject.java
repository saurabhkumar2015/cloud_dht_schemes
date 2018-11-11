package ceph;

public class DataObject {
	
	public int placementGroup;
	
	public int replicaId;
	
	public String fileName;
	
	public DataObject(int placementGroup, int replicaId, String fileName)
	{
		this.placementGroup = placementGroup;
		this.replicaId = replicaId;
		this.fileName = fileName;
	}
}
