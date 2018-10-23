package TestCollection;

import java.util.Random;

public class EntryPoint {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
     System.out.println("OsdMap status");
     
     
     // Populate the osd map from Configuration
     OsdMap mapInstance = OsdMap.getInstance();
     
     // Populate OsdMap
     PopulateOsdMap(mapInstance);
     
     // Show OsdMap status
     OsdMap.getInstance().ShowOsdMap(mapInstance.root,1);
    
     // Add extra node to Osd Map
     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 75);
     
     // Check the added node 75 in OsdMap
     mapInstance.FindNodeInOsdMap(68);
     
     // Mark a node failure
     mapInstance.DeleteNode(73);
     
	}
	
	public static void PopulateOsdMap(OsdMap mapInstance)
	{
		 mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 1, 2);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 2, 3);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 4);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 5);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 6);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 7);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 8);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 9);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 10);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 11);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 12);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 13);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 14);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 15);
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 1, 16);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 2, 17);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 18);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 19);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 20);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 21);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 22);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 23);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 24);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 25);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 26);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 27);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 28);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 29);
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 30);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 31);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 32);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 33);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 34);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 35);
	      
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 36);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 37);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 38);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 39);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 40);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 41);
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 42);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 43);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 44);
	     
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 1, 46);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 2, 47);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 48);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 49);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 50);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 51);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 52);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 53);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 54);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 55);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 56);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 57);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 58);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 59);
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 60);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 61);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 3, 62);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 4, 63);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 64);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 65);
	      
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 66);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 67);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 68);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 69);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 70);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 71);
	     
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 72);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 73);
	     mapInstance.AddNodeToOsdMap(randomWeightGenerator(), 5, 74);
	}

	public static double randomWeightGenerator()
	{
		Random rand = new Random();
		return rand.nextDouble();
	}
}
