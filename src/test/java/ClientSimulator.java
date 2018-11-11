

import org.json.simple.JSONObject;

import socket.IMessageSend;
import socket.MessageSendImpl;


public class ClientSimulator {

	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) 
    {
		
		 JSONObject obj =new JSONObject();
		 obj.put("name", "tulika");
		 
		 IMessageSend msgSend = new MessageSendImpl();
		 msgSend.sendMessage("localhost:5000", "read", obj);
		 
		 JSONObject obj1 =new JSONObject();
		 obj1.put("name", "tulika2");
		 
		 msgSend.sendMessage("localhost:5000", "read", obj1);
    }
	
	
}
