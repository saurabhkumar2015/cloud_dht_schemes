package common;

public class Payload {

	public String fileName;
	
	public int replicaId;
	
	public Payload(String fileName, int replicaId)
	{
		this.fileName = fileName;
		this.replicaId = replicaId;
	}

	@Override
	public String toString() {
		return "Payload{" +
				"fileName='" + fileName + '\'' +
				", replicaId=" + replicaId +
				'}';
	}
}
