package socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MessageSendImpl implements IMessageSend {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void sendMessage(String nodeAddress, String type, Object payload) {

        // Wrapping request into Request Object
        Request request = new Request();
        request.setType(type);
        request.setPayload(payload);


        Socket socket = null;
        DataInputStream input = null;
        DataOutputStream out = null;

        // Extracting address and port from NodeId
        String[] arr = nodeAddress.split(":");
        String address = arr[0];
        int port = Integer.parseInt(arr[1]);

        try {
            socket = new Socket(address, port);
            out = new DataOutputStream(socket.getOutputStream());

            byte[] stream = null;
            // ObjectOutputStream is used to convert a Java object into OutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(request);
            stream = baos.toByteArray();
            out.write(stream);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
