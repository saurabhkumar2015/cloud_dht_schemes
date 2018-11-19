package common;

import java.io.Serializable;

public class Payload implements Serializable {

	public String fileName;
	public int replicaId;
	public long versionNumber;
	
	public Payload(String fileName, int replicaId, long versionNumber)
	{
		this.fileName = fileName;
		this.replicaId = replicaId;
		this.versionNumber = versionNumber;
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
