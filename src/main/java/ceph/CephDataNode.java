package ceph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CephDataNode {
    public Map<Integer, ArrayList<DataObject>> map = new HashMap<Integer, ArrayList<DataObject>>();
    
    private static CephDataNode single_instance = null;
   // we can pass the configuration here
 	public static CephDataNode getInstance() 
     { 
         if (single_instance == null) 
             single_instance = new CephDataNode(); 
   
         return single_instance; 
     } 
 	
    
    public void addDataToNode(int nodeId, String fileName, int placementGroup, int replicaId)
    {
    	// Create the Data Node 
    	DataObject newlyAddedfile = new DataObject(fileName, placementGroup, replicaId);
    	// If node is already there then append the Data object to the Node List
    	ArrayList<DataObject> value = map.get(nodeId);
    	if (value != null) {
    	    value.add(newlyAddedfile);	
    	} else {
    	    // No such key exists
    		map.put(nodeId, new ArrayList<DataObject>() {{add(newlyAddedfile);}});
    	}
    }
    
    public void ShowNodeContainer()
    {
    	for (Map.Entry<Integer,ArrayList<DataObject>> entry : map.entrySet())
    	{
    		System.out.println("Node : -> " + entry.getKey());
    		ArrayList<DataObject> fileObject = entry.getValue();
    		for( DataObject dobj : fileObject)
    		{
    			System.out.println("fileName : " + dobj.fileName + " PlacementGroup : " + dobj.placementGroup + " ReplicaId : " + dobj.replicaId);
    		}
    	}
    }
    
    public void ShowNodeContainer(int nodeId)
    {
    	ArrayList<DataObject> fileObject = map.get(nodeId);
    	if(fileObject != null)
    	{
    	for( DataObject dobj : fileObject)
    	{
    		System.out.println("fileName : " + dobj.fileName + " PlacementGroup : " + dobj.placementGroup + " ReplicaId : " + dobj.replicaId);
    	}
    	}    	
    }
    
}


