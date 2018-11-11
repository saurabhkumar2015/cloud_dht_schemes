package ceph;

public class DataObject {

	public String fileName;
	
	public int placementGroup;
	
	public int replicaId;
	
	public DataObject(String fileName, int placementGroup, int replicaId)
	{
		this.fileName = fileName;
		this.placementGroup = placementGroup;
		this.replicaId = replicaId;
	}

	@Override
	public String toString() {
		return "DataObject{" +
				"placementGroup=" + placementGroup +
				", replicaId=" + replicaId +
				", fileName='" + fileName + '\'' +
				'}';
	}
}
