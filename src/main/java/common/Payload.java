package common;

public class Payload {

	public String fileName;
	
	public int placementGroup;
	
	public int replicaId;
	
	public Payload(String fileName, int pgGroup, int replicaId)
	{
		this.fileName = fileName;
		this.placementGroup = pgGroup;
		this.replicaId = replicaId;
	}
}
