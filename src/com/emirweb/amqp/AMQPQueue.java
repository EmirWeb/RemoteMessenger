package com.emirweb.amqp;

import java.util.concurrent.ConcurrentHashMap;

public class AMQPQueue {

	private ConcurrentHashMap<String, AMQPConsumer> mAMQPConsumers = new ConcurrentHashMap<String, AMQPConsumer>();
	
	
}
