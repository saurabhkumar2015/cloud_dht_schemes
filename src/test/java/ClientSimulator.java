

import org.json.simple.JSONObject;


import clients.RegularClient;
import models.RingRoutingTable;
import rabbitMQ.IMessageSend;
import rabbitMQ.MessageSendImpl;


public class ClientSimulator {

	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) 
    {
		
		 JSONObject obj =new JSONObject();
		 obj.put("name", "tulika");
		 
		 IMessageSend msgSend = new MessageSendImpl();
		 msgSend.sendMessage("localhost", "read", obj);
		 
		
    }
	
	
}
