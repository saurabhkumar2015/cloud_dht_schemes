package ceph;

import java.util.Random;

public class HashGenerator {
	
    private static HashGenerator single_instance = null;
	
    private final long MAX_VALUE = 0xFFFFFFFFL;
    private final double MAX_NODE = 15359.0;
    
	// we can pass the configuration here
	public static HashGenerator getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new HashGenerator(); 
  
        return single_instance; 
    } 
	
	public double randomWeightGenerator()
	{
		long seed = 123L;
		Random rand = new Random(seed);
		return rand.nextDouble();
	}
	
	public double generateHashValue(int clusterId, int PlacementGroupId, int replicaId)
	{
		// logic to write hashing strategy using input parameters
		// get the hash code using ClusterId, PGId, ReplicaId
	   // double hashCode = Math.pow(clusterId * PlacementGroupId,replicaId); 
		//double resultHash = hashCode / Math.pow(2, 31);
		
		// use well defined hashing for fair distribution of Files
		double resultHash = rushHash(Integer.toString(PlacementGroupId), replicaId, Integer.toString(clusterId));
		return resultHash;
	}
	
	public double GetWeightFactor(double currentNodeWeight, double subClusterWeight)
	{
		return currentNodeWeight / subClusterWeight;
	}
	
	public int getPlacementGroupIdFromFileName(String fileName, int PlacementGroupLimit)
	{
		return fileName.hashCode() % PlacementGroupLimit;
	}
	
	private double rushHash(String s1,int r,String cid){

        long a = s1.hashCode();
        long b = r & MAX_VALUE;
        long c = cid.hashCode();

        a = subtract(a, b); a = subtract(a, c); a = xor(a, c >> 13);
        b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 8));
        c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 13));
        a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 12));
        b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 16));
        c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 5));
        a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 3));
        b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 10));
        c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 15));


        return (c%MAX_NODE)/MAX_NODE;
    }
	
	 private long subtract(long val, long subtract) {
	        return (val - subtract) & MAX_VALUE;
	    }
	 
	 private long xor(long val, long xor) {
	        return (val ^ xor) & MAX_VALUE;
	    }
	 
	 private long leftShift(long val, int shift) {
	        return (val << shift) & MAX_VALUE;
	    }
}