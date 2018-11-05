package rabbitMQ;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.SerializationUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MessageSendImpl implements IMessageSend {

	
	 public String call(Request message, String NodeId) throws IOException, InterruptedException, TimeoutException, ClassNotFoundException {
		    final String corrId = UUID.randomUUID().toString();

		    ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			
			Connection connection;
			Channel channel = null;
			String requestQueueName = "rpc_queue";
			
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			
			
		    String replyQueueName = channel.queueDeclare().getQueue();
		    AMQP.BasicProperties props = new AMQP.BasicProperties
		            .Builder()
		            .correlationId(corrId)
		            .replyTo(replyQueueName)
		            .build();

		    byte[] stream = null;
		    // ObjectOutputStream is used to convert a Java object into OutputStream
		    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
		            ObjectOutputStream oos = new ObjectOutputStream(baos);) {
		        oos.writeObject(message);
		        stream = baos.toByteArray();
		    } catch (IOException e) {
		        // Error in serialization
		        e.printStackTrace();
		    }
		    
		    long start =  System.nanoTime();
		    channel.basicPublish("", requestQueueName, props, stream);
		    
		    
		    
		    final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

		    String ctag = channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
		      @Override
		      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		        if (properties.getCorrelationId().equals(corrId)) {
		          response.offer(new String(body, "UTF-8"));
		          
		        }
		      }
		    });

		    long end =  System.nanoTime();
		    System.out.println("Time taken inside: " + (end-start));
		    String result = response.take();
		    channel.basicCancel(ctag);
		    return result;
		  }
	 
	@Override
	public void sendMessage(String NodeId, String type, Object payload) {
		
		// TODO Auto-generated method stub
		Request request = new Request();
		request.setType(type);
		request.setPayload(payload);
		
		String response = null;
		try {
			response = call(request,NodeId);
			System.out.println(" [.] Got '" + response + "' ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     
		
	}

}
