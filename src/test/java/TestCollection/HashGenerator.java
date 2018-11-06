package TestCollection;

public class HashGenerator {
	
private static HashGenerator single_instance = null;
	
	// we can pass the configuration here
	public static HashGenerator getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new HashGenerator(); 
  
        return single_instance; 
    } 
	
	
	public double generateHashValue(int clusterId, int PlacementGroupId, int replicaId, double weight)
	{
		// logic to write hashing strategy using input parameters
		// get the hash code using ClusterId, PGId, ReplicaId
		StringBuilder sb = new StringBuilder();
		sb.append(clusterId); sb.append(PlacementGroupId); sb.append(replicaId);
		String str = sb.toString();
		int hashval = str.hashCode();
		double resultHash = (hashval % weight) / weight;
		return resultHash;
	}
	
	public int getPlacementGroupIdFromFileName(String fileName, int PlacementGroupLimit)
	{
		return fileName.hashCode() % PlacementGroupLimit;
	}
}
