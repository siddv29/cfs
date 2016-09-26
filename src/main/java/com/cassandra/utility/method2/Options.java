package com.cassandra.utility.method2;

import com.datastax.driver.core.ConsistencyLevel;

import java.util.ArrayList;

/**
 * Created by siddharth on 7/9/16.
 */
public class Options {
    private String username;
    private String password;
    private ConsistencyLevel consistencyLevel;
    private int numberOfThreads;
    private String dc;
    private int personalQueueSize;
    private ArrayList<String> columnNames;
    private int sleepMilliSeconds;
    private int fetchSize;

    public int getFetchSize() {
        return fetchSize;
    }

    public Options setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getSleepMilliSeconds() {
        return sleepMilliSeconds;
    }

    public Options setSleepMilliSeconds(int sleepMilliSeconds) {
        this.sleepMilliSeconds = sleepMilliSeconds;
        return this;
    }

    public Options() {
        username = "nothing";
        password = "nothing";
        consistencyLevel = ConsistencyLevel.LOCAL_ONE;
        numberOfThreads = 16;
        dc=null;
        personalQueueSize = Integer.MAX_VALUE;
        columnNames = new ArrayList<>();
        columnNames.add("*");
        sleepMilliSeconds = 1;
    }

    //return this added in setters, because it helps us in chaining
    //example,
    // new CassandraFastFullTableScan("my_keyspace.big_table",linkedBlockingQueue, new Option().setUsername("admin").setPassword("admin_password").setConsistencyLevel(Consistency.LOCAL_QUORUM));
    public Options setUsername(String username) {
        this.username = username;
        return this;
    }

    public Options setPassword(String password) {
        this.password = password;
        return this;
    }

    public Options setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public Options setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        return this;
    }

    public Options setDc(String dc) {
        this.dc = dc;
        return this;
    }


    public Options setPersonalQueueSize(int personalQueueSize) {
        this.personalQueueSize = personalQueueSize;
        return this;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }

    public int getPersonalQueueSize() {
        return personalQueueSize;
    }


    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public String getDc() {
        return dc;
    }

    @Override
    public String toString() {
        return "Options{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", numberOfThreads=" + numberOfThreads +
                ", dc='" + dc + '\'' +
                ", personalQueueSize=" + personalQueueSize +
                ", columnNames=" + columnNames +
                ", sleepMilliSeconds=" + sleepMilliSeconds +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
