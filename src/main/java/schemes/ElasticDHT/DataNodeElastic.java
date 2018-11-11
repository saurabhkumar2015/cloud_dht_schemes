package schemes.ElasticDHT;
import java.io.IOException;

import common.IDataNode;

public class DataNodeElastic implements IDataNode {

	private int nodeId;

	public void writeFile(String fileName, int replicaId) {
		int hashcode = fileName.hashCode();
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

	public void addNode(String nodeId) {
		try {
			RoutingTable.GetInstance().addNode(Integer.parseInt(nodeId));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		// TODO Auto-generated method stub
		
	}

	public void deleteNode(String nodeId) {
		RoutingTable.GetInstance().deleteNode(Integer.parseInt(nodeId));
		// TODO Auto-generated method stub
		
	}

	public void loadBalance(String nodeId, float loadFraction) {
		RoutingTable.GetInstance().loadBalance(Integer.parseInt(nodeId), (float)loadFraction);
		// TODO Auto-generated method stub
		
	}

	

}
