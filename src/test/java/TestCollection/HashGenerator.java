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
	
	
	public double generateHashValue(String filename, int replicationId, int clusterId)
	{
		// logic to write hashing strategy using input parameters
		return 0.4;
		
		//return 0;
	}
}
