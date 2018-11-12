package common;

import java.io.Serializable;

public class CephPayload implements Serializable {
	
	public String nodeIp;
	
	public int clusterId;
	
	public double nodeWeight;
	
	public double totalWt;
	
	public CephPayload(String nodeIp, int clusterId, double nodeWeight, double totalWt)
	{
		this.nodeIp = nodeIp;
		this.clusterId = clusterId;
		this.nodeWeight = nodeWeight;
		this.totalWt = totalWt;
	}

	@Override
	public String toString() {
		return "Payload{" +
				"nodeIp='" + nodeIp + '\'' +
				", clusterId=" + clusterId +
				", nodeWeight=" + nodeWeight+
				", totalWt=" + totalWt+"}";
	}

}
