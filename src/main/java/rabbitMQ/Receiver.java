package rabbitMQ;


import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.SerializationUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Receiver {
	
	private static final String RPC_QUEUE_NAME = "rpc_queue";
	public static void main(String[] argv) {
		
		
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");

	    Connection connection = null;
	    
	    try {
	      connection = factory.newConnection();
	      final Channel channel = connection.createChannel();

	      channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
	      channel.queuePurge(RPC_QUEUE_NAME);

	      channel.basicQos(1);

	      System.out.println(" [x] Awaiting RPC requests");

	      Consumer consumer = new DefaultConsumer(channel) {
	        @Override
	        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	          AMQP.BasicProperties replyProps = new AMQP.BasicProperties
	                  .Builder()
	                  .correlationId(properties.getCorrelationId())
	                  .build();

	          String response = "";

	          try {
	        	  
	        	Request message = SerializationUtils.deserialize(body);
	        	
	            System.out.println("type: "+message.getType()+" "+message.getPayload().toString());
	            response += "received "+ message.getPayload().toString();
	          }
	          catch (RuntimeException e){
	            System.out.println(" [.] " + e.toString());
	          }
	          finally {
	            channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
	            channel.basicAck(envelope.getDeliveryTag(), false);
	            // RabbitMq consumer worker thread notifies the RPC server owner thread 
	            synchronized(this) {
	            	this.notify();
	            }
	          }
	        }
	      };

	      channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
	      // Wait and be prepared to consume the message from RPC client.
	      while (true) {
	      	synchronized(consumer) {
	      		try {
	      			consumer.wait();
	      	    } catch (InterruptedException e) {
	      	    	e.printStackTrace();	    	
	      	    }
	      	}
	      }
	    } catch (IOException | TimeoutException e) {
	      e.printStackTrace();
	    }
	    finally {
	      if (connection != null)
	        try {
	          connection.close();
	        } catch (IOException _ignore) {}
	    }
	  }

}
