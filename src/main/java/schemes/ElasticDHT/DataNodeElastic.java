package schemes.ElasticDHT;

import common.IDataNode;
import common.IRoutingTable;

public class DataNodeElastic implements IDataNode {

	private int nodeId;

	public void writeFile(String fileName, int replicaId) {
		int hashcode = fileName.hashCode()%1024;
		nodeId = 0;
		for(int i = 0; i<schemes.ElasticDHT.RoutingTable.elasticTable.length;i++) {
			if(schemes.ElasticDHT.RoutingTable.elasticTable[i].hashIndex==hashcode) {
				 nodeId = (Integer) schemes.ElasticDHT.RoutingTable.elasticTable[i].nodeId.get(replicaId-1);
				break;
			}
		}
		System.out.println("FIle written to "+nodeId);
		// TODO Auto-generated method stub
		
	}

	public void deleteFile(String fileName) {
		int hashcode =  fileName.hashCode();
		for(int i = 0;i<schemes.ElasticDHT.RoutingTable.elasticTable.length;i++) {
			if(schemes.ElasticDHT.RoutingTable.elasticTable[i].hashIndex==hashcode) {
				System.out.println("File deleted from all the replicas");
			}
		}
		// TODO Auto-generated method stub
		
	}


	public void addNode(int nodeId) {
		RoutingTable.GetInstance().addNode(nodeId);
		// TODO Auto-generated method stub
		
	}

	public void deleteNode(int nodeId) {
		RoutingTable.GetInstance().deleteNode(nodeId);
		// TODO Auto-generated method stub
		
	}

	public void loadBalance(int nodeId, double loadFraction) {
		RoutingTable.GetInstance().loadBalance(nodeId, loadFraction);

		// TODO Auto-generated method stub
		
	}

	@Override
	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight) {

	}

	@Override
	public void UpdateRoutingTable(IRoutingTable cephrtTable) {

	}


}
