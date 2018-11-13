package schemes.ElasticDHT;
import java.io.IOException;

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
		r.GetInstance().getRoutingTable();
		System.out.println("End loading routing table");
		System.out.println(r.GetInstance().getNodeId("abc", 2));
		//r.GetInstance().addNode(101);
		
		System.out.println("Exiting add and entering delete");
		//r.GetInstance().deleteNode(7);
		r.GetInstance().loadBalance(4, 0.7);
	}

}
