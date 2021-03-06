package ceph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import common.Commons;
import common.Constants;
import common.IDataNode;
import common.IRoutingTable;
import common.Payload;
import config.ConfigLoader;

public class CephDataNode  implements IDataNode{

	public Set<DataObject> dataList = new HashSet<DataObject>();

    public HashGenerator hashGenerator;

    public int NodeId;
    
    public IRoutingTable cephRtTable;
    
    private static CephDataNode single_instance = null;
	
    //map to hold hash range is locked or not
    public Map<List<Integer>,Boolean> fileLckMap;
    
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
	fileLckMap= new HashMap<List<Integer>,Boolean>();
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
		
		//System.out.println("Write file request received for FileName: " + fileName + " pGroup: " + placementGroupId + " replicaId: " + replicaId + " on node " + (destinationNodeId) );

		// Step 2: push the Data to the DataNode if not present in DataList
		DataObject obj = new DataObject(placementGroupId, replicaId,fileName);
		dataList.add(obj);
		return true;
	}
	
	public boolean writeAllFiles(List<Payload> payloads) {
		//step 1. find the placementGroupId for file
		Set<DataObject> pgSet = new HashSet<>();
		for(Payload pload : payloads)
		{
			//	int placementGroupId = this.hashGenerator.givePlacementGroupIdFromFileName(pload.fileName, ConfigLoader.config.PlacementGroupMaxLimit);
				
				// Find the node on which it should go.
				int destinationNodeId = this.cephRtTable.giveNodeId(pload.fileName, pload.replicaId);
				
				//System.out.println("Write file request received for FileName: " + pload.fileName + " pGroup: " + placementGroupId + " replicaId: " + pload.replicaId + " on node " + (destinationNodeId) );

				// Step 2: push the Data to the DataNode if not present in DataList
				DataObject obj = new DataObject(Integer.parseInt(pload.fileName), pload.replicaId, pload.fileName);
				dataList.add(obj);
				pgSet.add(obj);
		}
		
		for(DataObject pload : pgSet)
		{
			System.out.println("write pgroup: " + pload.placementGroup + " with replica: " + pload.replicaId);
		}
				return true;
	}

	public void deleteFile(String fileName) {
		// TODO Auto-generated method stub				
				// Step 1: Remove the Data from the DataNode
		System.out.println("Delete file request received for FileName: " + fileName);
		int placementGroupForFile = this.hashGenerator.givePlacementGroupIdFromFileName(fileName, ConfigLoader.config.PlacementGroupMaxLimit);
				for(DataObject obj : dataList)
				{
					if(obj.placementGroup == placementGroupForFile)
						dataList.remove(obj);
				}
	}

	public void addNode(int nodeId) {
		// TODO Auto-generated method stub
		System.out.println("Add new node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.addNode(nodeId);
		showDataNodeState();
	}

	public void deleteNode(int nodeId) {
		// On delete set the node Active status to false.
		System.out.println("Delete node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.deleteNode(nodeId);
		showDataNodeState();
	}

	public void loadBalance(int nodeId, double loadFraction) {
		// First find the node and change the load on the node by loadfactor
		System.out.println("Load balance request received at nodeId " + nodeId + " with loadFactor of " + loadFraction);
		this.cephRtTable = cephRtTable.loadBalance(nodeId, loadFraction);
		showDataNodeState();
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
		return this.cephRtTable;
	}

	public void setRoutingTable(IRoutingTable table) {
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
	
	public void showDataNodeState()
	{
		System.out.println("Data Node " + this.NodeId + " contains the following PlacementGroup->");
		for(DataObject pload : this.dataList)
		{
			System.out.println("Placementgroup: " + pload.placementGroup + " with replica: " + pload.replicaId);
		}
	}
	
	private void MoveFilesOnWeightChangeInOsdMap()
	{
		Map<Integer, List<DataObject>> addMap = new HashMap<>();
		HashSet<DataObject> removedFiles = new HashSet<>();
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
		//--------------------------------------------for locking-------------------------------------------------------------------------------------
		// Send request to other data node.
				for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
		    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
		    		List<Payload> filesTobeMove = new LinkedList<>();
		    		filesTobeMove.clear();
		    		Set<DataObject> pgSet = new HashSet<>();
		    		for(DataObject obj : e.getValue())
		    		{   
		    			System.out.println(" Moving Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId+ "to Node "+e.getKey());
		    			filesTobeMove.add(new Payload(Integer.toString(obj.placementGroup), obj.replicaId, this.cephRtTable.getVersionNumber()));
		    			pgSet.add(obj);
		    		}
		    		
		    		// send the aggregated request to the destination node. 
		    		Commons.messageSender.sendMessage(destinationNodeIp, Constants.FILE_LIST, filesTobeMove);
		    	}		
				
		try {
			Thread.sleep(ConfigLoader.config.sleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//----------------------------------------------------for locking------------------------------------------------------------------------------------
		
		
		// Send request to other data node.
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("Files need to move from " +  this.NodeId + " to node " + e.getKey());
    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
    		List<Payload> filesTobeMove = new LinkedList<>();
    		filesTobeMove.clear();
    		Set<DataObject> pgSet = new HashSet<>();
    		for(DataObject obj : e.getValue())
    		{
    			//System.out.println(" Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    			filesTobeMove.add(new Payload(Integer.toString(obj.placementGroup), obj.replicaId, this.cephRtTable.getVersionNumber()));
    			pgSet.add(obj);
    		}
    		
    		// Print at console for visibility
    		for(DataObject pload : pgSet)
    		{
    			System.out.println("Move pgroup: " + pload.placementGroup + " with replica: " + pload.replicaId);
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
            int currentreplicaValue = 0;
            while(noOfReplicaPresent != replicaFactor)
            {
            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(++currentreplicaValue, obj.placementGroup);
            	if(destinationNodeId != -2)
            	{
            		noOfReplicaPresent++;
            	}
            	
        		if(currentreplicaValue > 30)
        			break;
            }
            
            // no need to process this file.
            if(currentreplicaValue > 30)
    			continue;
            
            // If replicationFactor value == currentReplicaValue then no file need to be written otherwise need to write file with replica equal to currentreplicaValue;
            if(replicaFactor  < currentreplicaValue )
            {
            	// need to add file with current replica value
            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(currentreplicaValue, obj.placementGroup);
            	int thresholdCounter = 1;
            	while(destinationNodeId == -2 || thresholdCounter > 30)
            	{
            		destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(++currentreplicaValue, obj.placementGroup);	
            		thresholdCounter++;
            	}
            	if(thresholdCounter > 30)
            		continue;
            	if(this.NodeId != destinationNodeId )
            	{
            		List<DataObject> list = addMap.get(destinationNodeId);
            		if(list == null) list = new ArrayList<>();
            		list.add(new DataObject(obj.placementGroup, currentreplicaValue, obj.fileName));
            		addMap.put(destinationNodeId, list);
            		
                }
            }
		}
		
		//--------------------------------------------------------------for locking--------------------------------------------------------------------
		// Send request to other data node.
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
    		List<Payload> filesTobeMove = new LinkedList<>();
    		filesTobeMove.clear();
    		Set<DataObject> pgSet = new HashSet<>();
    		for(DataObject obj : e.getValue())
    		{
    			System.out.println(" Move Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId+ "to Node "+e.getKey());
    			filesTobeMove.add(new Payload(Integer.toString(obj.placementGroup), obj.replicaId, this.cephRtTable.getVersionNumber()));
    			pgSet.add(obj);
    		}
    		
    		// send the aggregated request to the destination node. 
    		Commons.messageSender.sendMessage(destinationNodeIp, Constants.FILE_LIST, filesTobeMove);
    	}		
		
		try {
			Thread.sleep(ConfigLoader.config.sleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//------------------------------------------------------------for locking------------------------------------------------------------------------
		
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("Files need to added to node " + e.getKey());
    		String destinationNodeIp = ConfigLoader.config.nodesMap.get(e.getKey());
    		List<Payload> filesTobeMove = new LinkedList<>();
    		filesTobeMove.clear();
    		Set<DataObject> pgSet = new HashSet<>();
    		for(DataObject obj : e.getValue())
    		{
    			//System.out.println("fileName" + obj.fileName + " Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    			filesTobeMove.add(new Payload(Integer.toString(obj.placementGroup), obj.replicaId, this.cephRtTable.getVersionNumber()));
    			pgSet.add(obj);
    		}
    		// Print at console for visibility
    		for(DataObject pload : pgSet)
    		{
    			System.out.println("Move pgroup: " + pload.placementGroup + " with replica: " + pload.replicaId);
    		}
    		// send the aggregated request to the destination node. 
    		Commons.messageSender.sendMessage(destinationNodeIp, Constants.ADD_FILES, filesTobeMove);
    		
    	}
	}
	

	@Override
	public int getNodeId() {
		// TODO Auto-generated method stub
		return this.NodeId;
	}
}

