package com.vsct.dt.nsq.callbacks;

import com.vsct.dt.nsq.exceptions.NSQException;

@FunctionalInterface
public interface NSQErrorCallback {

    void error(NSQException x);
}
