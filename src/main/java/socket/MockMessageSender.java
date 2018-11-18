package socket;

public class MockMessageSender implements IMessageSend {
    @Override
    public void sendMessage(String nodeId, String type, Object payload) {
        System.out.println("Message "+ type + " sent to " + nodeId);
    }
}
