package schemes.ElasticDHT;

import java.io.IOException;

import common.Commons;
import config.ConfigLoader;
import socket.MockMessageSender;

public class Test {
	@SuppressWarnings("static-access")
	public static void main(String arg[]) {
		try {
			ConfigLoader.init("//Users//sreekrishnasridhar/cloud_dht_schemes/config.conf");
			Commons.messageSender = new MockMessageSender();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Start loading routing Table");
		ERoutingTable r = ERoutingTable.giveInstance();
		//r.giveInstance();
		//r.printRoutingTable();
		System.out.println("End loading routing table");
		r.giveInstance().addNode(11);
		r.giveInstance().printRoutingTable();
		r.giveInstance().loadBalance(11, 0.8);
		r.giveInstance().printRoutingTable();

		

		System.out.println("Exiting add and entering delete");
		//r.giveInstance().deleteNode(7);
	
	}

}
