package com.emirweb.utilities;

import java.io.IOException;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class RemoteMessenger {

	private final static String EXCHANGE = "4168228694";
	private final static String PROVIDER_BINDING = "provider";
	private final static String PROVIDER_QUEUE_NAME = "provider";
	private static final String FANOUT = "fanout";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		final ConnectionFactory factory = new ConnectionFactory();

		factory.setHost("localhost");
		factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setPort(5672);

		factory.setRequestedHeartbeat(5);

		try {
			final Connection connection = factory.newConnection();
			connection.addShutdownListener(new ShutdownListener() {

				@Override
				public void shutdownCompleted(ShutdownSignalException shutdownSignalException) {
					System.out.println("shutdownSignalException: " + shutdownSignalException);
				}
			});
			final Channel channel = connection.createChannel();
			
			final DefaultConsumer consumer = new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, com.rabbitmq.client.AMQP.BasicProperties properties, byte[] body) throws IOException {
					try {

						final String message = new String(body);
						final String routingKey = envelope.getRoutingKey();

						System.out.println("message: " + message + " routingKey: " + routingKey);
						final boolean success = true;
						final long deliverTag = envelope.getDeliveryTag();
						try {
							if (success) {
								channel.basicAck(deliverTag, false);
							} else {
								channel.basicNack(deliverTag, false, true);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					} catch (ShutdownSignalException e) {
						e.printStackTrace();
					} catch (ConsumerCancelledException e) {
						e.printStackTrace();
					}
				}

				@Override
				public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
					System.out.println("consumerTag: " + consumerTag + " sig.getMessage(): " + sig.getMessage());
				}
			};
			
			channel.exchangeDeclare(EXCHANGE, FANOUT);
			channel.queueDeclare(PROVIDER_QUEUE_NAME, true, false, false, null);
			channel.basicConsume(PROVIDER_QUEUE_NAME, false, consumer);
			channel.queueBind(PROVIDER_QUEUE_NAME, EXCHANGE, PROVIDER_BINDING);
			

			final byte[] message = "Hello World!".getBytes();
			channel.basicPublish(EXCHANGE, PROVIDER_BINDING, null, message);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}