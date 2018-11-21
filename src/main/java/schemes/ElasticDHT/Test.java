package schemes.ElasticDHT;

import java.io.IOException;

import common.Commons;
import config.ConfigLoader;
import socket.MockMessageSender;

public class Test {
	@SuppressWarnings("static-access")
	public static void main(String arg[]) {
		try {
			ConfigLoader.init("//Users//sreekrishnasridhar//cloud_dht_schemes//config.conf");
			Commons.messageSender = new MockMessageSender();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Start loading routing Table");
		ERoutingTable r = new ERoutingTable();
		System.out.println("End loading routing table");
		r.giveInstance().addNode(11);
		r.giveInstance().addNode(12);
		r.giveInstance().addNode(13);
		r.giveInstance().addNode(14);
		r.giveInstance().addNode(16);
		r.giveInstance().addNode(15);

		// r.giveInstance().addNode(12);
		// r.giveInstance().addNode(13);
		r.giveInstance().deleteNode(3);
		r.giveInstance().deleteNode(11);
		r.giveInstance().deleteNode(1);
//		r.printRoutingTable();
//		System.exit(-1);
//		r.giveInstance().addNode(11);
//
//		r.giveInstance().deleteNode(12);
//		r.giveInstance().addNode(12);
//		r.giveInstance().deleteNode(11);
//		r.giveInstance().deleteNode(12);
//		r.giveInstance().deleteNode(13);
//		r.giveInstance().deleteNode(14);
//		r.giveInstance().deleteNode(15);
//		r.giveInstance().deleteNode(16);
//		r.giveInstance().addNode(1);
//		r.giveInstance().addNode(3);
//		r.giveInstance().addNode(7);

		System.out.println(r.giveInstance().giveLiveNodes());

		System.out.println("Exiting add and entering delete");
		//r.giveInstance().deleteNode(7);
		r.giveInstance().loadBalance(6, 0.1);
		r.giveInstance().loadBalance(2,0.5);
		r.giveInstance().loadBalance(3, 0.8);
		r.giveInstance().loadBalance(4, 1);
		//r.giveInstance().loadBalance(1, 0.4);
		r.giveInstance().printRoutingTable();
	}

}
