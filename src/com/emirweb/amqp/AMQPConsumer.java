package com.emirweb.amqp;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class AMQPConsumer {

	private static final String FANOUT = "fanout";

	private final String mQueuename;
	private final String mExchange;
	private final String mBinding;
	private final ConcurrentHashMap<Long, AMQPConsumerListener> mAmqpConsumerListeners = new ConcurrentHashMap<Long, AMQPConsumerListener>();

	private Channel mChannel;

	public AMQPConsumer(final Channel channel, String exchange, String queuename, String binding) throws IOException {
		final DefaultConsumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, com.rabbitmq.client.AMQP.BasicProperties properties, byte[] body) throws IOException {
				synchronized (AMQPConsumer.this) {
					try {

						final String message = new String(body);
						final String routingKey = envelope.getRoutingKey();

						System.out.println("message: " + message + " routingKey: " + routingKey);
						final boolean success = send(message);
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
			}

			@Override
			public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
				System.out.println("consumerTag: " + consumerTag + " sig.getMessage(): " + sig.getMessage());
			}
		};
		channel.exchangeDeclare(exchange, FANOUT);
		channel.queueDeclare(queuename, true, false, false, null);
		channel.queueBind(queuename, exchange, binding);
		channel.basicConsume(queuename, false, consumer);

		mQueuename = queuename;
		mExchange = exchange;
		mBinding = binding;
		mChannel = channel;
	}

	private synchronized boolean send(final String message) {
		if (mAmqpConsumerListeners.isEmpty())
			return false;

		for (AMQPConsumerListener amqpConsumerListener : mAmqpConsumerListeners.values())
			amqpConsumerListener.onMessage(message);

		return true;
	}

	public synchronized boolean add(long id, AMQPConsumerListener amqpConsumerListener) {
		if (mAmqpConsumerListeners.containsKey(id))
			return false;
		mAmqpConsumerListeners.put(id, amqpConsumerListener);
		return true;
	}

	public synchronized boolean stopConsume(long id) {
		return mAmqpConsumerListeners.remove(id) != null;
	}

	public synchronized boolean isConsuming() {
		return !mAmqpConsumerListeners.isEmpty();
	}

	public String getExchange() {
		return mExchange;
	}

	public String getQueuename() {
		return mQueuename;
	}

	public String getBinding() {
		return mBinding;
	}

	public synchronized Set<Long> getIds() {
		return mAmqpConsumerListeners.keySet();
	}

	public synchronized void close() {
		if (mChannel == null)
			return;
		try {
			mChannel.queueUnbind(mQueuename, mExchange, mBinding);
		} catch (IOException e) {
			e.printStackTrace();
		}

		mChannel = null;
		mAmqpConsumerListeners.clear();
	}
}
