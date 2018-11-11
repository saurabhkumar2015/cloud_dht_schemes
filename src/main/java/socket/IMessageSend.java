package socket;

public interface IMessageSend {
	
	void sendMessage(String NodeId, String type, Object payload);

}
