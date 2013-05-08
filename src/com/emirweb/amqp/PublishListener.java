package com.emirweb.amqp;

public interface PublishListener {
	void onSuccess();
	void onFailure(Exception exception);
}
