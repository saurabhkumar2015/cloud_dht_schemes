package common;

import java.io.Serializable;

public class Payload implements Serializable {

	public String fileName;
	public int replicaId;
	public long versionNumber;
	public int nodeId;
	public int hashBucket;
	
	public Payload(String fileName,int replicaId,long versionNumber) {
		this.fileName = fileName;
		this.replicaId = replicaId;
		this.versionNumber = versionNumber;
		
	}
	
	public Payload(String fileName, int replicaId, long versionNumber, int nodeId,int hashBucket)
	{
		this.fileName = fileName;
		this.replicaId = replicaId;
		this.versionNumber = versionNumber;
		this.nodeId = nodeId;
		this.hashBucket = hashBucket;
	}
	

	
	@Override
	public String toString() {
		return "Payload{" +
				"fileName='" + fileName + '\'' +
				", replicaId=" + replicaId +
				", versionNumber=" + versionNumber +
				'}';
	}
}
