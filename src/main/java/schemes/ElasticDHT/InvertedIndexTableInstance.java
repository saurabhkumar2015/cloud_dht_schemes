package schemes.ElasticDHT;
import java.util.*;

public class InvertedIndexTableInstance {
	public int nodeId;
	public BitSet usedHashedIndex = new BitSet(7);
	
	
	// Constructor which could be parameterized later
	public InvertedIndexTableInstance()
	{
		
	}


	public InvertedIndexTableInstance(int i, BitSet parseInt) {
		nodeId = i;
		usedHashedIndex = parseInt;
		// TODO Auto-generated constructor stub
	}
	

}
