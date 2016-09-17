package com.cassandra.utility.trial;

/**
 * Created by siddharth on 1/9/16.
 */
public class Constants {
    public final static String IP;
    public final static String USERNAME;
    public final static String PASSWORD;
    public final static String DESCRIBERING_FILE;
    public /*final*/ static String KEYSPACE;
    public /*final*/ static String TABLE_NAME;
    public /*final*/ static String PARTITION_KEY;
    public static final String CONSUMERS_PER_NODE;
    public static String FETCH_SIZE;

    static {
        IP ="10.41.55.117";
        USERNAME = "cassandra";
        PASSWORD = "cassandra";
        DESCRIBERING_FILE = "/home/siddharth/Downloads/describering.txt";
        KEYSPACE = "myks";
        TABLE_NAME = "mytable";
        PARTITION_KEY = "partkey";
        CONSUMERS_PER_NODE = "4";
        FETCH_SIZE = "5000";


    }
}
