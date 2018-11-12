package schemes.ElasticDHT;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import config.ConfigLoader;
import config.DHTConfig;

public class ElasticRoutingTable {
	private int rint;
	private ArrayList<Integer> l;
	private Random rno;

	public ElasticRoutingTableInstance[] populateRoutingTable() {


		DHTConfig config = ConfigLoader.config;
		int size = config.bucketSize;
		l = new ArrayList();
		int r = config.replicationFactor;

		ElasticRoutingTableInstance[] initialTable  = new ElasticRoutingTableInstance[size];
		//for loop till size-1  var i
		//node id list is : config.nideIdStart till config.nodeIdEnd
		// randomly get a nodeId between start and end
		// i is the hashbucket
		// config.replicationFactor
		for(int k = 0;k<size;k++) {
			l.clear();
			rno = new Random(config.seed);
			rint = rno.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;

			l.add(rint);
			for(int i = 1;i<r;i++) {
				boolean b= false;
				while(b== false) {
					rno = new Random();
					rint = rno.nextInt(config.nodeIdEnd-config.nodeIdStart)+config.nodeIdStart;
					b = check(rint);
				}
				l.add(rint);

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