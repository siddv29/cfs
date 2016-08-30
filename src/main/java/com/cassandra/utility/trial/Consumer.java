package com.cassandra.utility.trial;

import com.datastax.driver.core.ResultSet;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/8/16.
 */
public class Consumer {
    private final LinkedBlockingQueue<ResultSet> queue;

    public Consumer(LinkedBlockingQueue<ResultSet> queue) {
        this.queue = queue;
    }
}
