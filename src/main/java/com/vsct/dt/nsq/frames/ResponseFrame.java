package com.vsct.dt.nsq.frames;

import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;

public class ResponseFrame extends NSQFrame {
    private final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ResponseFrame.class);

    public String getMessage() {
        try {
            return new String(getData(), "utf8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Caught", e);
        }
        return null;
    }

    public String toString() {
        return "RESPONSE: " + this.getMessage();
    }
}
