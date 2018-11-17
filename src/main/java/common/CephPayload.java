package common;

import java.io.Serializable;

import ceph.CephRoutingTable;

public class CephPayload implements Serializable {
	
	public String nodeIp;
	
	public int clusterId;
	
	public double nodeWeight;
	
	public double totalWt;
	
	public boolean isLoadBalance;
	
	public CephRoutingTable updated_ceph_routing_table;
	
	public CephPayload(String nodeIp, int clusterId, double nodeWeight, double totalWt,boolean isLoadBalance, CephRoutingTable updated_ceph_routing_table)
	{
		this.nodeIp = nodeIp;
		this.clusterId = clusterId;
		this.nodeWeight = nodeWeight;
		this.totalWt = totalWt;
		this.isLoadBalance = isLoadBalance;
		this.updated_ceph_routing_table = updated_ceph_routing_table;
	}

	@Override
	public String toString() {
		return "Payload{" +
				"nodeIp='" + nodeIp + '\'' +
				", clusterId=" + clusterId +
				", nodeWeight=" + nodeWeight+
				", updated_ceph_routing_table=" + updated_ceph_routing_table+
				", totalWt=" + totalWt+"}";
	}

}
