package ceph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

public class CephDataNodeStandalone {
	public HashMap<Integer,ArrayList<DataObject>> dataList = new HashMap<Integer, ArrayList<DataObject>>();

    private HashGenerator hashGenerator;
    
    private DHTConfig config;
    
    public int NodeId;
    
    public IRoutingTable cephRtTable;
    
    private static CephDataNode single_instance = null;
    
    public CephDataNodeStandalone()
    {
    	this.hashGenerator = HashGenerator.giveInstance();
    	this.config = ConfigLoader.config;
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
		int placementGroupId = this.hashGenerator.givePlacementGroupIdFromFileName(fileName, config.PlacementGroupMaxLimit);
		
		// Find the node on which it should go.
		int destinationNodeId = this.cephRtTable.giveNodeId(fileName, replicaId);
		
		System.out.println("Write file request received for FileName: " + fileName + " replicaId: " + replicaId + " on node " + (destinationNodeId) );

		// Add the file to the hashmap with the given node id
		if(dataList.containsKey(destinationNodeId))
		{
			ArrayList<DataObject> filebucket = dataList.get(destinationNodeId);
			filebucket.add(new DataObject(placementGroupId,replicaId,fileName));
		}
		else
		{
			ArrayList<DataObject> filebucket = new ArrayList();
			filebucket.add(new DataObject(placementGroupId,replicaId,fileName));
			dataList.put(destinationNodeId,filebucket );
		}
		
		return true;
	}

	public void deleteFile(String fileName) {
		// TODO Auto-generated method stub				
	}

	public void addNode(int nodeId) {
		// TODO Auto-generated method stub
		System.out.println("Add new node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.addNode(nodeId);
		this.MoveFilesOnWeightChangeInOsdMap();
	}

	public void deleteNode(int nodeId) {
		// On delete set the node Active status to false.
		System.out.println("Delete node request received with nodeId " + nodeId);
		this.cephRtTable = this.cephRtTable.deleteNode(nodeId);
		this.MoveFilesOnNodeDeletion();
	}

	public void loadBalance(int nodeId, double loadFraction) {
		// First find the node and change the load on the node by loadfactor
		System.out.println("Load balance request received at nodeId " + nodeId + " with loadFactor of " + loadFraction);
		this.cephRtTable = cephRtTable.loadBalance(nodeId, loadFraction);
		this.MoveFilesOnWeightChangeInOsdMap();
	}
    
       
    public void UpdateRoutingTable(IRoutingTable cephrtTable)
    {
    	this.cephRtTable = cephrtTable;
		CephRoutingTable rt = (CephRoutingTable)cephrtTable;
    	System.out.println("OSD Routing table is updated::" + rt.versionNumber);
    	
    	// Trigger file movement on this DataNode
    	System.out.println("File Movement has been triggered at node: " + this.NodeId);
    	this.MoveFilesOnWeightChangeInOsdMap();
    }
  
	public IRoutingTable getRoutingTable() {
		// return the routing table Instance of the Data Node
		return this.cephRtTable;
	}

	public void setRoutingTable(IRoutingTable table) {
		// set the routing table Instance for the Data node.
		this.cephRtTable = table;
		
	}
	
	private void MoveFilesOnWeightChangeInOsdMap()
	{
		Map<Integer, List<DataObject>> addMap = new HashMap<>();
		for (Integer key : this.dataList.keySet())
			{
		        for(DataObject obj : this.dataList.get(key))
		        {
                  int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(obj.replicaId, obj.placementGroup);
                  if(destinationNodeId != -2 && destinationNodeId != key)
                  {
                	  List<DataObject> list = addMap.get(destinationNodeId);
	            		if(list == null) list = new ArrayList<>();
	            		list.add(obj);
	            		addMap.put(destinationNodeId, list);
                  }
            
		        }
			
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("file need to move from " +  key + " to node " + e.getKey());
    		for(DataObject obj : e.getValue())
    		{
    			System.out.println(" Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    		}
    		
    	}
			}
		
	}
	
	private void MoveFilesOnNodeDeletion()
	{
		Map<Integer, List<DataObject>> addMap = new HashMap<>();
		for (Integer key : this.dataList.keySet())
		{
			int replicaFactor = ConfigLoader.config.replicationFactor;
			for(DataObject obj : this.dataList.get(key))
			{
	            int noOfReplicaPresent = 0;
	            int currentreplicaValue = 0;
	            while(noOfReplicaPresent != replicaFactor)
	            {
	            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(++currentreplicaValue, obj.placementGroup);
	            	if(destinationNodeId != -2)
	            	{
	            		noOfReplicaPresent++;
	            	};
	            }
	            
	            // If replicationFactor value == currentReplicaValue then no file need to be written otherwise need to write file with replica equal to currentreplicaValue;
	            if(replicaFactor  < currentreplicaValue )
	            {
	            	// need to add file with current replica value
	            	int destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(currentreplicaValue, obj.placementGroup);
	            	while(destinationNodeId == -2)
	            	{
	            		destinationNodeId = ((CephRoutingTable)this.cephRtTable).mapInstance.findNodeWithRequestedReplica(++currentreplicaValue, obj.placementGroup);	
	            	}
	            	if(key != destinationNodeId )
	            	{
	            		List<DataObject> list = addMap.get(destinationNodeId);
	            		if(list == null) list = new ArrayList<>();
	            		list.add(new DataObject(obj.placementGroup, currentreplicaValue, obj.fileName));
	            		addMap.put(destinationNodeId, list);
	            		//System.out.println("file need to added to node " + destinationNodeId + " with replication Factor: " + currentreplicaValue);
	            		
	                }
	            }
	            
			}
			
		}
		for (Entry<Integer, List<DataObject>> e: addMap.entrySet()) {
    		System.out.println("file need to added to node " + e.getKey());
    		for(DataObject obj : e.getValue())
    		{
    			System.out.println(" Pgroup : " + obj.placementGroup + " replica Factor: " + obj.replicaId);
    		}
    	}
		
	}

}
