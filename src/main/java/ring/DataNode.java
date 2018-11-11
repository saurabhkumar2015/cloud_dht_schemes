package ring;

import java.util.LinkedList;

public class DataNode implements IDataNode {

    RingRoutingTable routingTableObj;

    public DataNode(RingDHTScheme ring) {
    	this.routingTableObj = ring.routingTableObj;
    }

    //nodeId = ip:port
    public void addNode(int nodeId) {
    	routingTableObj.addNode(nodeId);
    }
	
	public void deleteNode(int nodeId) {
		routingTableObj.deleteNode(nodeId);
	}
	
	public void loadBalance(int nodeId, double loadFraction) {
		routingTableObj.loadBalance(nodeId, loadFraction);
	}

	public void writeFile(String fileName, int replicaId) {
		System.out.println("\n");
		System.out.println("FileName: "+fileName);
		int hashVal = fileName.hashCode();
		System.out.println("FileHash Value:"+hashVal);
		LinkedList<Integer> listOfAssociatedHashes =  routingTableObj.modifiedBinarySearch(hashVal);
		for(int start=0; start<routingTableObj.replicationFactor;start++) {
			System.out.println("File written into node id: "+routingTableObj.routingMap.get(listOfAssociatedHashes.get(start))+" Replication Id:"+(start+1));
		}
	}

	public void deleteFile(String fileName) {

		System.out.println("\n");
		System.out.println("FileName: "+fileName);
		int hashVal = fileName.hashCode();
		System.out.println("FileHash Value:"+hashVal);
		LinkedList<Integer> listOfAssociatedHashes =  routingTableObj.modifiedBinarySearch(hashVal);
		for(int start=0; start<routingTableObj.replicationFactor;start++) {
			System.out.println("File deleted from node id: "+routingTableObj.routingMap.get(listOfAssociatedHashes.get(start))+" Replication Id:"+(start+1));
		}
		
	}
	
}
