package com.vsct.dt.nsq.callbacks;

import com.vsct.dt.nsq.NSQMessage;

@FunctionalInterface
public interface NSQMessageCallback {

	public void message(NSQMessage message);
}
