package ceph;

import java.io.IOException;
import java.util.Random;

import config.ConfigLoader;

public class TestCeph {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
				 ConfigLoader.init("C:\\cloud\\config.conf");
				 
			      // Create a Data node for ceph to test Delete 
				 CephDataNodeStandalone cephNode = new CephDataNodeStandalone();
				 AddFilesToCephSystem(cephNode);
				
				//  ((CephRoutingTable)cephNode.cephRtTable).mapInstance.ShowOsdMap();
				// cephNode.addNode(14);
				// ((CephRoutingTable)cephNode.cephRtTable).mapInstance.ShowOsdMap();
				//cephNode.loadBalance(4, .8);
				// cephNode.addNode(14);
				 cephNode.deleteNode(5);
	}
	
	public static void AddFilesToCephSystem(CephDataNodeStandalone cephNode)
	{	
		for(int i = 0; i < 1000; i++)
		{
			Random r = new Random();
			int replicaId = r.nextInt(3) + 1;
			String fileName = "CloudComputing" + i;
			cephNode.writeFile(fileName, replicaId);
		}
		
	}

}
