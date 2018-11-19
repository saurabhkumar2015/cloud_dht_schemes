package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import config.ConfigLoader;
import config.DHTConfig;

public class ElasticRoutingTable {
	private int rint;
	private ArrayList<Integer> l;
	private Random rno;

	@SuppressWarnings("unchecked")
	public ElasticRoutingTableInstance[] populateRoutingTable() {
		
		
		DHTConfig config = ConfigLoader.config;
		int size = config.bucketSize;
		int r = config.replicationFactor;
		
		ElasticRoutingTableInstance[] initialTable  = new ElasticRoutingTableInstance[size];
		//for loop till size-1  var i
		//node id list is : config.nideIdStart till config.nodeIdEnd
		// randomly get a nodeId between start and end
		// i is the hashbucket
		// config.replicationFactor
		rno = new Random(config.seed);
		int range = config.nodeIdEnd-config.nodeIdStart +1;
		for(int k = 0;k<size;k++) {
			 l = new ArrayList<Integer>();

			rint = rno.nextInt(range)+config.nodeIdStart;
			Set<Integer> ids = new HashSet<Integer>();
			l.add(rint);
			ids.add(rint);
			for(int i = 1;i<r;i++) {
				boolean b= false;
				while(b == false ) {
					rno  = new Random();
					rint = rno.nextInt(range)+config.nodeIdStart;
					b = !ids.contains(rint);
				}
				l.add(rint);
				ids.add(rint);
				
			}
			
			initialTable[k] =  new ElasticRoutingTableInstance(k,l);
		
		}
	 //make it a list
	
	
		

		return initialTable;
		
		
	}

	@SuppressWarnings("unused")
	private boolean check(int rint2) {
		int size = l.size();
		for(int i = 0;i<size;i++) {
			if(rint2==(Integer)l.get(i)) {
				break;
			}
			else {
				return true;
			}
		}
		
		// TODO Auto-generated method stub
		return false;
	}

}
