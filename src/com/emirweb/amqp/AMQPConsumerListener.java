package com.emirweb.amqp;

public interface AMQPConsumerListener {
	boolean onMessage(final String message);
	void onClose(final Exception exception);
}
