package com.cassandra.utility.exception;

import com.datastax.driver.core.Host;

/**
 * Created by siddharth on 29/8/16.
 */
public class HostDownException extends RuntimeException {
    public HostDownException(Host hostName){
        super(hostName.getAddress()+" is down");
    }
}
