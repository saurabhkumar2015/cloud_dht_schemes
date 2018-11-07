package socket;

public interface IMessageSend {
	
	public void sendMessage(String NodeId, String type, Object payload);

}
