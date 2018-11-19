package schemes.ElasticDHT;

import java.io.IOException;

import common.Commons;
import config.ConfigLoader;

public class Test {
	@SuppressWarnings("static-access")
	public static void main(String arg[]) {
		try {
			ConfigLoader.init("//Users//sreekrishnasridhar//cloud_dht_schemes//config.conf");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Start loading routing Table");
		schemes.ElasticDHT.RoutingTable r = new schemes.ElasticDHT.RoutingTable();
		System.out.println("End loading routing table");
		r.GetInstance().addNode(11);
		r.GetInstance().addNode(12);
		r.GetInstance().addNode(13);
		r.GetInstance().addNode(14);
		r.GetInstance().addNode(16);
		r.GetInstance().addNode(15);

		// r.GetInstance().addNode(12);
		// r.GetInstance().addNode(13);
		r.GetInstance().deleteNode(3);
		r.GetInstance().deleteNode(11);
		r.GetInstance().deleteNode(1);
//		r.printRoutingTable();
//		System.exit(-1);
//		r.GetInstance().addNode(11);
//
//		r.GetInstance().deleteNode(12);
//		r.GetInstance().addNode(12);
//		r.GetInstance().deleteNode(11);
//		r.GetInstance().deleteNode(12);
//		r.GetInstance().deleteNode(13);
//		r.GetInstance().deleteNode(14);
//		r.GetInstance().deleteNode(15);
//		r.GetInstance().deleteNode(16);
//		r.GetInstance().addNode(1);
//		r.GetInstance().addNode(3);
//		r.GetInstance().addNode(7);

		System.out.println(r.GetInstance().getLiveNodes());

		System.out.println("Exiting add and entering delete");
		//r.GetInstance().deleteNode(7);
		r.GetInstance().loadBalance(6, 0.1);
		r.GetInstance().loadBalance(2,0.5);
		r.GetInstance().loadBalance(3, 0.8);
		r.GetInstance().loadBalance(4, 1);
		//r.GetInstance().loadBalance(1, 0.4);
		r.GetInstance().printRoutingTable();
	}

}
