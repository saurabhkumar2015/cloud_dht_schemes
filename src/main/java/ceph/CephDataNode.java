package ceph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.Commons;
import common.Constants;
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
		
		System.out.println("Write file request received for FileName: " + fileName + " replicaId: " + replicaId );
		// Step 2: push the Data to the DataNode if not present in DataList
		DataObject obj = new DataObject(placementGroupId, replicaId,fileName);
		dataList.add(obj);
		
	}

	public void deleteFile(String fileName) {
		// TODO Auto-generated method stub				
				// Step 1: Remove the Data from the DataNode
		System.out.println("Delete file request received for FileName: " + fileName);
				for(DataObject obj : dataList)
				{
					if(obj.fileName == fileName)
						dataList.remove(obj);
				}
	}

	public void addNode(int nodeId) {
		// TODO Auto-generated method stub
		System.out.println("Add new node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.addNode(nodeId);
	}

	public void deleteNode(int nodeId) {
		// On delete set the node Active status to false.
		System.out.println("Delete node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.deleteNode(nodeId);
		
	}

	public void loadBalance(int nodeId, double loadFraction) {
		// First find the node and change the load on the node by loadfactor
		System.out.println("Load balance request received at nodeId " + nodeId + " with loadFactor of " + loadFraction);
		this.cephRtTable = cephRtTable.loadBalance(nodeId, loadFraction);
	}
    
    public void MoveFiles(int clusterId,String nodeIp, double newnodeWeight, double clusterWeight, boolean isLoadbalance)
    {

    	if(isLoadbalance)
    	{
    	   this.OnLoadBalanceMovement(clusterId, newnodeWeight, clusterWeight);	
    	}
    	else
    	{
    	// iterate on local file copy and move the file accordingly    	
    	List<DataObject> filesToremove = new LinkedList<DataObject>();
    	for(DataObject obj : dataList)
    	{
    		double hashvalue = HashGenerator.getInstance().generateHashValue(clusterId, obj.placementGroup, obj.replicaId);
			double weightFactor = HashGenerator.getInstance().GetWeightFactor(newnodeWeight, clusterWeight);
			
			if(hashvalue < weightFactor)
			{
				System.out.println("File with pGroup " + obj.placementGroup + " replica " + obj.replicaId + " moves to new node " + nodeIp);
				Commons.messageSender.sendMessage(nodeIp, Constants.WRITE_FILE,Commons.GeneratePayload(obj.fileName, obj.replicaId));
				filesToremove.add(obj);
			}
    	}
       
    	// Now remove the files from local copy of data node.
    	dataList.removeAll(filesToremove);
       }
    }
    
    private void OnLoadBalanceMovement(int clusterId, double nodeWeight, double clusterWeight)
    {
    	List<DataObject> filesToremove = new LinkedList<DataObject>();
    	for(DataObject obj : dataList)
    	{
    		double hashvalue = HashGenerator.getInstance().generateHashValue(clusterId, obj.placementGroup, obj.replicaId);
			double weightFactor = HashGenerator.getInstance().GetWeightFactor(nodeWeight, clusterWeight);
			
			if(hashvalue < weightFactor)
			{
			  System.out.println("File movement is not needed for file: " + obj.fileName + " with replica " + obj.replicaId);
			}
			else
			{
				int nodeidToMoveFile = this.cephRtTable.getNodeId(obj.fileName, obj.replicaId);
				if(nodeidToMoveFile != -2)
				{

				System.out.println("File with pGroup " + obj.placementGroup + " replica " + obj.replicaId + " moves to node " + nodeidToMoveFile);
				String nodeIp = config.nodesMap.get(nodeidToMoveFile);
				Commons.messageSender.sendMessage(nodeIp, Constants.WRITE_FILE,Commons.GeneratePayload(obj.fileName, obj.replicaId));
				System.out.println("File with pGroup " + obj.placementGroup + " replica " + obj.replicaId + " moves to node " + nodeIp);
				filesToremove.add(obj);
				}
				else
				{
					System.out.println("The file is out of range for Osd Map with fileName " + obj.fileName + " replica " + obj.replicaId);
				}
			}
    	}
    	
    	// Now remove the files from local copy of data node.
    	dataList.removeAll(filesToremove);
    }
    
    
    public void OnDeleteNodeMoveFile()
    {
    	// Iterate over their the files and for each file check if the file with filename and replica exist then fine otherwise increment the 
    	// replica and add the file to ceph system.
    	for(DataObject obj : dataList)
    	{
    		int count = 0;
    		for(int i = 1; i <= obj.replicaId; i++)
    		{
    			int nodeWithrequestFileAndReplica = this.cephRtTable.getNodeId(obj.fileName,i);
    			// if File present with active node then its good call
    			if(nodeWithrequestFileAndReplica != -2)
    			{
    				count++;
    			}
    		}
    		// if file with some intermediate replica is not present then add the fie with same filename with incremented replicaId
    		if(count < obj.replicaId)
    		{
    			if(this.cephRtTable.getNodeId(obj.fileName,obj.replicaId+1) == -2)
    			{
    			 System.out.println("Add file to ceph system with Pgroup : " + obj.placementGroup + " and replica = " + obj.replicaId + 1 );
    			((CephRoutingTable)this.cephRtTable).mapInstance.AddFileToCephSystem(obj.fileName, obj.replicaId + 1, config.PlacementGroupMaxLimit);
    		    }
    		}
    	}
    	
    }
    
    public void UpdateRoutingTable(IRoutingTable cephrtTable)
    {
    	this.cephRtTable = cephrtTable;
		CephRoutingTable rt = (CephRoutingTable)cephrtTable;
    	System.out.println("OSD Routing table is updated::" + rt.VersionNo);
    }

	@Override
	public IRoutingTable getRoutingTable() {
		return cephRtTable;
	}

	@Override
	public void setRoutingTable(IRoutingTable table) {
		synchronized (this) {
			this.cephRtTable = table;
		}
	}

}

