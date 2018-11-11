package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


public class DataNodeProcess {
	
	public static void main(String args[]) throws Exception{
		
		if(args.length!=3) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n 2) Jar file location \n 3)Nodes.csv location");
	
	     
	     //Fetching nodes.csv file location
	     String nodesMapLoc = args[2];
	     
	     File file = new File(nodesMapLoc);
	     BufferedReader br = new BufferedReader(new FileReader(file));
	     String line;
	     
	     //Fetching current Host Address
	     String currentHostAddress = InetAddress.getLocalHost().getHostAddress();
	     
	     while ((line = br.readLine()) != null) {
	    	       
	    	  String[] arr = line.split(",");
	    	  
	    	  String nodeId = ((arr[1]).split(":"))[0];
	    	  
	    	  if(nodeId.equals(currentHostAddress)) {
	    		  
	    		  List<String> command = new ArrayList<String>();
	  		    
	    		  command.add("java");
	    		  command.add("-cp");
	  		      command.add("-jar");
	  		      command.add(args[1]);
	  		      command.add("common.DataNodeLoader");
	  		      command.add(args[0]);
	  		      command.add(arr[0]);
	  		      
	    		  ProcessBuilder builder = new ProcessBuilder(command);		    
	    		  Process process = builder.start();
	    		  
	    		
	    	  }
	    	  
	    	  
	    	 
	     }
	    	
	     
	}

}
