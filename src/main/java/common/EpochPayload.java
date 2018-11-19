package common;

import java.io.Serializable;

import ceph.CephRoutingTable;

public class EpochPayload implements Serializable {
	
	public String status;
	
	public IRoutingTable newRoutingTable;
	
	public EpochPayload(String status, IRoutingTable newRoutingTable) {
		// TODO Auto-generated constructor stub
		
		this.newRoutingTable = newRoutingTable;
		this.status = status;
	}

	@Override
	public String toString() {
		return "Payload{" +
				"newRotuingTable='" + newRoutingTable + '\'' +
				", status=" + status +"}";
	}

}
