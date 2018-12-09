package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class DataNodeStart {
	
		public static void main(String args[]) throws Exception{
			if(args.length != 4) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path " +
					"\n 2) Jar file location " +
					"\n 3) Nodes.csv location"
					+ "\n 4) IP value"
					+ "\n 5) node Id start "
					+ "\n 6) node Id end ");
		     
		     //Fetching nodes.csv file location
		     String nodesMapLoc = args[2];
		     File file = new File(nodesMapLoc);
		     BufferedReader br = new BufferedReader(new FileReader(file));
		     String line;
		     
		     //Fetching current Host Address
		     String currentHostAddress = args[3];

		     while ((line = br.readLine()) != null) {
		    	  String[] arr = line.split(",");
		    	  String nodeId = ((arr[1]).split(":"))[0];
		    	  if(nodeId.equals(currentHostAddress)) {
		    		  List<String> command = new ArrayList<String>();
		    		  command.add("java");
		    		  command.add("-cp");
		  		      command.add(args[1]);
		  		      command.add("common.DataNodeLoader");
		  		      command.add(args[0]);
		  		      command.add(arr[0]);
		    		  ProcessBuilder builder = new ProcessBuilder(command);
					  System.out.println("Node id started : "+ builder.command());
		    		  Process process = builder.start();
					  System.out.println("Node id started : "+ arr[0]+ command.toString() + process.isAlive());
		    	  }
		     }
		}
	}



