package com.emirweb.amqp;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.emirweb.utilities.IdCreator;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class AMQPManager {

	private IdCreator mIdCreator = new IdCreator();
	private ShutdownListener mShutDownListener;
	private Connection mConnection;
	private Channel mPublisherChannel;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, AMQPConsumer>>> mAMQPConsumers = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, AMQPConsumer>>>();
	private ConcurrentHashMap<Long, AMQPConsumer> mAMQPConsumerIdMappings = new ConcurrentHashMap<Long, AMQPConsumer>();

	private static final String INVALID_SHUT_DOWN_LISTENER = "Invalid shut down listener";
	private static final String INVALID_USERNAME = "Invalid username";
	private static final String INVALID_PASSWORD = "Invalid password";
	private static final String INVALID_HOST = "Invalid host";
	private static final int HEART_BEAT = 5;
	private static final int PORT = 5672;

	public AMQPManager(final String host, final String username, final String password, final ShutdownListener shutdownListener) throws IOException {
		if (shutdownListener == null)
			throw new IllegalArgumentException(INVALID_SHUT_DOWN_LISTENER);

		if (host == null)
			throw new IllegalArgumentException(INVALID_HOST);

		if (username == null)
			throw new IllegalArgumentException(INVALID_USERNAME);

		if (password == null)
			throw new IllegalArgumentException(INVALID_PASSWORD);

		mShutDownListener = shutdownListener;

		final ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setUsername(username);
		factory.setPassword(password);
		factory.setPort(PORT);
		factory.setRequestedHeartbeat(HEART_BEAT);

		mConnection = factory.newConnection();
		mConnection.addShutdownListener(new ShutdownListener() {

			@Override
			public void shutdownCompleted(ShutdownSignalException shutdownSignalException) {
				shutdown(shutdownSignalException);
			}
		});
	}

	public synchronized void close() {
		try {
			mConnection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		mConnection = null;
		mPublisherChannel = null;
		mShutDownListener = null;
	}

	private synchronized void shutdown(ShutdownSignalException shutdownSignalException) {
		final ShutdownListener shutdownListener = mShutDownListener;
		close();
		shutdownListener.shutdownCompleted(shutdownSignalException);
	}

	public synchronized boolean isOpen() {
		return mConnection != null && mConnection.isOpen();
	}

	public synchronized Channel getChannel() throws IOException {
		if (!isOpen())
			return null;

		if (mPublisherChannel == null)
			mPublisherChannel = mConnection.createChannel();

		return mPublisherChannel;
	}

	public synchronized long startConsume(final String exchange, final String queuename, final String binding, final AMQPConsumerListener amqpConsumerListener) throws IOException {
		final Channel channel = getChannel();

		ConcurrentHashMap<String, ConcurrentHashMap<String, AMQPConsumer>> queues = mAMQPConsumers.get(exchange);
		if (queues == null) {
			queues = new ConcurrentHashMap<String, ConcurrentHashMap<String, AMQPConsumer>>();
			mAMQPConsumers.put(exchange, queues);
		}

		ConcurrentHashMap<String, AMQPConsumer> amqpConsumers = queues.get(queuename);
		if (amqpConsumers == null) {
			amqpConsumers = new ConcurrentHashMap<String, AMQPConsumer>();
			queues.put(queuename, amqpConsumers);
		}

		AMQPConsumer amqpComsumer = amqpConsumers.get(binding);
		if (amqpComsumer == null) {
			try {
				amqpComsumer = new AMQPConsumer(channel, exchange, queuename, binding);
				remove(exchange, queuename, binding);
			} catch (IOException e) {
				e.printStackTrace();
			}
			amqpConsumers.put(binding, amqpComsumer);
		}

		final long id = mIdCreator.getNewId();
		final boolean collision = amqpComsumer.add(id, amqpConsumerListener);

		if (collision)
			return -1;

		mAMQPConsumerIdMappings.put(id, amqpComsumer);
		return id;
	}

	private synchronized AMQPConsumer remove(String exchange, String queuename, String binding) {
		ConcurrentHashMap<String, ConcurrentHashMap<String, AMQPConsumer>> queues = null;
		ConcurrentHashMap<String, AMQPConsumer> amqpConsumers = null;

		try {
			queues = mAMQPConsumers.remove(exchange);

			if (queues == null)
				return null;

			amqpConsumers = queues.remove(queuename);
			if (amqpConsumers == null)
				return null;

			AMQPConsumer amqpComsumer = amqpConsumers.remove(binding);
			if (amqpComsumer == null)
				return null;

			return amqpComsumer;
		} finally {
			if (amqpConsumers != null && amqpConsumers.isEmpty() && queues != null)
				queues.remove(queuename);

			if (queues != null && queues.isEmpty())
				mAMQPConsumers.remove(exchange);
		}
	}

	public synchronized boolean stopConsume(final long id) {
		final AMQPConsumer amqpConsumer = mAMQPConsumerIdMappings.get(id);

		if (amqpConsumer == null)
			return false;

		final boolean result = amqpConsumer.stopConsume(id);
		final boolean isConsuming = amqpConsumer.isConsuming();

		if (!isConsuming)
			removeConsumer(amqpConsumer);

		return result;
	}

	private synchronized boolean removeConsumer(AMQPConsumer amqpConsumer) {
		if (amqpConsumer == null)
			return false;

		final Set<Long> ids = amqpConsumer.getIds();

		for (final long id : ids)
			mAMQPConsumerIdMappings.remove(id);

		final String exchange = amqpConsumer.getExchange();
		final String queuename = amqpConsumer.getQueuename();
		final String binding = amqpConsumer.getBinding();

		final AMQPConsumer removedAmqpConsumer = remove(exchange, queuename, binding);

		return removedAmqpConsumer == amqpConsumer;
	}

	public void publish(final String exchange, final String binding, final String message, final PublishListener publishListener) {
		(new Thread(new Runnable() {

			@Override
			public void run() {
				publishHelper(exchange, binding, message, publishListener);
			}
		})).start();
	}

	private synchronized void publishHelper(final String exchange, final String binding, final String message, final PublishListener publishListener) {
		if (!isOpen()) {
			publishListener.onFailure(new Exception("AMQP closed"));
			return;
		}

		Channel channel;
		try {
			channel = getChannel();
			channel.basicPublish(exchange, binding, null, message.getBytes());
			publishListener.onSuccess();
		} catch (IOException e) {
			e.printStackTrace();
			publishListener.onFailure(e);
		}
	}
}
