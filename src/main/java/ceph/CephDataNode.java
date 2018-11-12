package ceph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import common.IDataNode;
import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;
import socket.IMessageSend;

public class CephDataNode  implements IDataNode{
    public ArrayList<DataObject> dataList = new ArrayList<DataObject>();

    private HashGenerator hashGenerator;
    
    private DHTConfig config;
    
    public int NodeId;
    
    public IRoutingTable cephRtTable;
    
    private static CephDataNode single_instance = null;
    
    public CephDataNode(int nodeId)
    {
    	this.hashGenerator = HashGenerator.getInstance();
    	this.config = ConfigLoader.config;
    	this.NodeId = nodeId;
    	cephRtTable = CephRoutingTable.getInstance();
    }
    
    public static CephDataNode getInstance(int nodeId) {
        if (single_instance == null)
            single_instance = new CephDataNode(nodeId);

        return single_instance;
    }
    
	public void writeFile(String fileName, int replicaId) {
		//step 1. find the placementGroupId for file
		int placementGroupId = this.hashGenerator.getPlacementGroupIdFromFileName(fileName, config.PlacementGroupMaxLimit);
		
		// Step 2: push the Data to the DataNodeDeleted
		DataObject obj = new DataObject(placementGroupId, replicaId,fileName);
		dataList.add(obj);
		
	}

	public void deleteFile(String fileName) {
		// TODO Auto-generated method stub				
				// Step 1: Remove the Data from the DataNodeDeleted
				for(DataObject obj : dataList)
				{
					if(obj.fileName == fileName)
						dataList.remove(obj);
				}
	}

	public void addNode(int nodeId) {
		// TODO Auto-generated method stub
		try {
			this.cephRtTable.addNode(nodeId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteNode(int nodeId) {
		// TODO Auto-generated method stub
		
	}

	public void loadBalance(int nodeId, double loadFraction) {
		// TODO Auto-generated method stub
		
	}
    
    
    
}


