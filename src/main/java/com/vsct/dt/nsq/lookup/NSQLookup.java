package com.vsct.dt.nsq.lookup;

import com.vsct.dt.nsq.ServerAddress;

import java.util.Set;

public interface NSQLookup {
    Set<ServerAddress> lookup(String topic);

    void addLookupAddress(String addr, int port);
}
