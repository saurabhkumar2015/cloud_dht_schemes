package ceph;

public class DataObject {
	
	public int placementGroup;
	
	public int replicaId;
	
	public String fileName;
	
	public DataObject(int placementGroup, int replicaId, String fileName)
	{
		this.placementGroup = placementGroup;
		this.replicaId = replicaId;
		this.fileName = fileName;
	}
	
	@Override 
	 public boolean equals (Object obj)
	  {
	   if (this==obj) return true;
	   if (this == null) return false;
	   if (this.getClass() != obj.getClass()) return false;
	   // if pgroup and replica is equal.
	   DataObject dbobj = (DataObject) obj ;
	   return (this.placementGroup == dbobj.placementGroup && this.replicaId == dbobj.replicaId);
	   }
	
	   @Override
       public int hashCode() {
           return 0;
       }
}
