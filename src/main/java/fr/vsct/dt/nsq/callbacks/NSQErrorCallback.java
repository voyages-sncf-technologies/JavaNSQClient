package fr.vsct.dt.nsq.callbacks;

import fr.vsct.dt.nsq.exceptions.NSQException;

@FunctionalInterface
public interface NSQErrorCallback {

    void error(NSQException x);
}
