package fr.vsct.dt.nsq.callbacks;

import fr.vsct.dt.nsq.NSQMessage;

@FunctionalInterface
public interface NSQMessageCallback {

	public void message(NSQMessage message);
}
