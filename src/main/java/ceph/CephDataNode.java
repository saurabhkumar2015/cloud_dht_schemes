package ceph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import common.Commons;
import common.Constants;
import common.IDataNode;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;

public class CephDataNode  implements IDataNode{

	public ArrayList<DataObject> dataList = new ArrayList<DataObject>();

    public HashGenerator hashGenerator;

    public int NodeId;
    
    public IRoutingTable cephRtTable;
    
    private static CephDataNode single_instance = null;
    
    public CephDataNode()
    {
    	
    }
    
    public CephDataNode(int nodeId)
    {
    	this.hashGenerator = HashGenerator.giveInstance();
    	this.NodeId = nodeId;
    	EntryPoint entryPoint = new EntryPoint();
        entryPoint.BootStrapCeph();
    	cephRtTable = CephRoutingTable.giveInstance();
    }
    
    public static CephDataNode getInstance(int nodeId) {
        if (single_instance == null)
            single_instance = new CephDataNode(nodeId);

        return single_instance;
    }
    
	public boolean writeFile(String fileName, int replicaId) {
		//step 1. find the placementGroupId for file
		int placementGroupId = this.hashGenerator.givePlacementGroupIdFromFileName(fileName, ConfigLoader.config.PlacementGroupMaxLimit);
		
		// Find the node on which it should go.
		int destinationNodeId = this.cephRtTable.giveNodeId(fileName, replicaId);

		
		if(destinationNodeId != this.NodeId)
			return false;
		
		System.out.println("Write file request received for FileName: " + fileName + " pGroup: " + placementGroupId + " replicaId: " + replicaId + " on node " + (destinationNodeId) );

		// Step 2: push the Data to the DataNode if not present in DataList
		DataObject obj = new DataObject(placementGroupId, replicaId,fileName);
		dataList.add(obj);
		return true;
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
    
       
    public void UpdateRoutingTable(IRoutingTable cephrtTable, String updateType)
    {
    	this.cephRtTable = cephrtTable;
		CephRoutingTable rt = (CephRoutingTable)cephrtTable;
    	System.out.println("OSD Routing table is updated::" + rt.versionNumber);
    	
    	// Trigger file movement on this DataNode
    	System.out.println("File Movement has been triggered at node: " + this.NodeId);
    	if(updateType.equals(Constants.ADD_NODE) || updateType.equals(Constants.LOAD_BALANCE))
    	this.MoveFilesOnWeightChangeInOsdMap();
    	else
    		this.MoveFilesOnNodeDeletion();
    }
  
	public IRoutingTable getRoutingTable() {
		// return the routing table Instance of the Data Node
		return this.cephRtTable;
	}

	public void setRoutingTable(IRoutingTable table) {
		// set the routing table Instance for the Data node.
		this.cephRtTable = table;
		
	}

	public void addHashRange(String hashRange) {
		// TODO Auto-generated method stub
		
	}

	public void newUpdatedRoutingTable(int nodeId, String type, IRoutingTable rt) {

	}

	public void MoveFiles(int clusterIdofNewNode, String nodeIp, double newnodeWeight, double clusterWeight,
			boolean isLoadbalance) {
		// TODO Auto-generated method stub
		
	}
	
	private void MoveFilesOnWeightChangeInOsdMap()
	{
		Map<Integer, List<DataObject>> addMap = new HashMap<>();
		List<DataObject> removedFiles = new LinkedList<>();
		for(DataObject obj : this.dataList)
		{
            int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(obj.replicaId, obj.placementGroup);
            if(destinationNodeId != -2 && this.NodeId != destinationNodeId)
            {
            	List<DataObject> list = addMap.get(destinationNodeId);
        		if(list == null) list = new ArrayList<>();
        		list.add(obj);
        		addMap.put(destinationNodeId, list);
        		
        		// Delete file from current node.
        		removedFiles.add(obj);
            }
            
		}
		
		// Delete the files from current node.
		this.dataList.removeAll(removedFiles);
		
		// Send request to other data node.
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("file need to move from " +  this.NodeId + " to node " + e.getKey());
    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
    		List<Payload> filesTobeMove = new LinkedList<>();
    		filesTobeMove.clear();
    		for(DataObject obj : e.getValue())
    		{
    			System.out.println(" Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    			filesTobeMove.add(new Payload(obj.fileName, obj.replicaId, this.cephRtTable.getVersionNumber()));
    		}
    		
    		// send the aggregated request to the destination node. 
    		Commons.messageSender.sendMessage(destinationNodeIp, Constants.ADD_FILES, filesTobeMove);
    	}		
	}
	
	private void MoveFilesOnNodeDeletion()
	{
		int replicaFactor = ConfigLoader.config.replicationFactor;
		Map<Integer, List<DataObject>> addMap = new HashMap<>();
		for(DataObject obj : this.dataList)
		{
            int noOfReplicaPresent = 0;
            int currentreplicaValue = 1;
            while(noOfReplicaPresent != replicaFactor)
            {
            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(currentreplicaValue, obj.placementGroup);
            	if(destinationNodeId != -2)
            	{
            		noOfReplicaPresent++;
            	}

        		currentreplicaValue++;
            }
            
            // If replicationFactor value == currentReplicaValue then no file need to be written otherwise need to write file with replica equal to currentreplicaValue;
            if(replicaFactor + 1 < currentreplicaValue )
            {
            	// need to add file with current replica value
            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(currentreplicaValue, obj.placementGroup);
            	while(destinationNodeId == -2)
            	{
            		destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(currentreplicaValue++, obj.placementGroup);	
            	}
            	if(this.NodeId != destinationNodeId )
            	{
            		List<DataObject> list = addMap.get(destinationNodeId);
            		if(list == null) list = new ArrayList<>();
            		list.add(new DataObject(obj.placementGroup, currentreplicaValue, obj.fileName));
            		addMap.put(destinationNodeId, list);
            		
                }
            }
		}
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("file need to added to node " + e.getKey());
    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
    		List<Payload> filesTobeMove = new LinkedList<>();
    		filesTobeMove.clear();
    		for(DataObject obj : e.getValue())
    		{
    			System.out.println(" Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    			filesTobeMove.add(new Payload(obj.fileName, obj.replicaId, this.cephRtTable.getVersionNumber()));
    		}
    		
    		// send the aggregated request to the destination node. 
    		Commons.messageSender.sendMessage(destinationNodeIp, Constants.ADD_FILES, filesTobeMove);
    		
    	}
	}
}

