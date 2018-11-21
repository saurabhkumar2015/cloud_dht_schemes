package common;

import java.io.Serializable;


public class UpdateRoutingPayload implements Serializable {
	
	//Node id to be added/deleted/load balanced
	public int nodeId;
	
	//Type of request add/delete/load balance
	public String type;
	
	//updated routing table
	public IRoutingTable newRoutingTable;
	

	
	public UpdateRoutingPayload(int nodeId, String type, IRoutingTable newRoutingTable) {
		// TODO Auto-generated constructor stub
		this.nodeId = nodeId;
		this.type = type;
		this.newRoutingTable = newRoutingTable;
	
	}

	@Override
	public String toString() {
		return "Payload{" +
				"nodeId='" + nodeId + '\'' +
				"type='" + type + '\'' +
				"newRoutingTable='" + newRoutingTable + '\'' +
				"}";
	}

}
