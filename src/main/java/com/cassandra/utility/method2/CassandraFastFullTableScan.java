package com.cassandra.utility.method2;

import com.cassandra.utility.WhiteListPolicyWithOnePriorityNode;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 7/9/16.
 */
public class CassandraFastFullTableScan {
    private final String tableIdentifier;
    private final String contactPoint;
    private final String username;
    private final String password;
    private final ConsistencyLevel consistencyLevel;
    private final int numberOfThreads;
    private final LinkedBlockingQueue<Row> resultQueue;
    private final String dc;
    private final String partitionKey;
    private final String keyspace;
    private final String tableName;
    private final Thread producerThreads[];
    private final CountDownLatch latch;
    private final int personalQueueSizePerProducer;
    private final ArrayList<String> columns;
    private final int sleepMilliSeconds;
    private final int fetchSize;
//    private final boolean enableWhiteListPolicy;

    public CassandraFastFullTableScan(String tableIdentifier, String contactPoint,LinkedBlockingQueue<Row> resultQueue, Options options/*, boolean enableWhiteListPolicy*/) {
        System.out.println(options);
        this.tableIdentifier = tableIdentifier;
        this.contactPoint = contactPoint;
        this.resultQueue = resultQueue;
        this.username = options.getUsername();
        this.password = options.getPassword();
        this.consistencyLevel = options.getConsistencyLevel()/*ConsistencyLevel.ALL*/;
        this.dc = options.getDc();
//        this.enableWhiteListPolicy=enableWhiteListPolicy/*false*/;
        LoadBalancingPolicy loadBalancingPolicy;
        if(dc != null){
            loadBalancingPolicy = DCAwareRoundRobinPolicy.builder().withLocalDc(dc).build();
        }else{
            loadBalancingPolicy = new RoundRobinPolicy();
        }
        Cluster cluster = Cluster.builder().addContactPoint(contactPoint)
                        .withQueryOptions(new QueryOptions().setFetchSize(5000))
                        .withCredentials(username,password)
                        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                        .withLoadBalancingPolicy(new TokenAwarePolicy(loadBalancingPolicy))
                        .build();
        /*
        Story 1:
        Hi, if you are reading this story, then I guess you decided to go through the code base, for CFS(Cassandra Fast full table Scan), the name at the moment.
        I think, I would be settling on this name. Earlier, when I started it, I decided the name multiple cassandra consumer(producer).
        It would have causes a lot of ambiguity, whether it is producer/ works as consumer etc etc.
        So I dropped it.
        Oh, back to the story, if you read the above text, it means you decided to go through the code. Try using it maybe?
        It is good. Trust me, I am not trying to sell this. It is really good.
        Want to contribute? Again not selling it, there is a LOT which can be done. If interested, mail me :-). We can discuss what to do.

        I forgot, i started this comment with the intention of explaining why I would be commenting TokenAware and use WhiteListPolicy instead.
        (Hence, create a cluster and attach specific whitelist policy with all of them, and pass those sessions from producer.

        The program scans token ranges, and token aware isn't so intelligent, yet, to decide which node to send it on.
        It's current decision making capacity is when you specify the complete primary key(partition key, to be precise).
        Now, we don't want that 1 producer, to hit some node as coordinator and that node has NO copy of that token range.
        What we would like is, the producer should hit that node which has one copy(out of many copies of data,replication).

        I am a little lost here, whether this would actually help? Maybe, a little.
        My Arguments :
        1. P1 would hit N3, but data is on n1
        2. Similarly in worst case, all P's might be hitting in a cris-cross manner.
        3. Now, it appears that overall, there might be no problem, as each node would be saturated.(possible, serving requests of data not on it)
           However, say, N3 would be coordinator for P1's hit, but it would have to fetch the data from another node.
           This might increase the inter cassandra cluster traffic
            {
                we can use TokenAware with setRoutingKey
                but it doesn't support on bound statement with multiple arguments
            }
        4. So, I think WhiteList policy might be good.
        5. Still open to discussions, if any.
        6. Let it be flag based for now, let's call that flag enableWhileListPolicy
         */

        //get partition key.
        //not tested on composite partition key
        String temp[] = tableIdentifier.split("\\.");
        this.keyspace = temp[0];
        this.tableName = temp[1];
        this.partitionKey = getPartitionKey(cluster);
        this.columns = options.getColumnNames();
        this.fetchSize = options.getFetchSize();
        this.personalQueueSizePerProducer = options.getPersonalQueueSize();
        this.sleepMilliSeconds = options.getSleepMilliSeconds();
        Producer.setStaticData(consistencyLevel,sleepMilliSeconds);
        this.numberOfThreads = options.getNumberOfThreads();
        this.latch = new CountDownLatch(numberOfThreads);
        this.producerThreads = readyProducers(cluster);
        new Thread(){
          @Override
            public void run(){
              try{latch.await();}catch (Exception e){}
              cluster.close();
          }
        }.start();
    }

    private Thread[] readyProducers(Cluster cluster) {
        Thread producerThreads[] = new Thread[numberOfThreads];
        Set<TokenRange> tokenRangeSetForOneProducer = new HashSet<>();
        int numberOfTokensPerConsumer = cluster.getMetadata().getTokenRanges().size() / numberOfThreads;
        int numberOfProducersWithExtraToken = cluster.getMetadata().getTokenRanges().size() % numberOfThreads;
        int tokensAddedForCurrentConsumer = 0;
        int indexOfProducer = 0;
        StringBuffer selectionColumnsBuffer = new StringBuffer();
        for(String column : columns){
            selectionColumnsBuffer.append(column+",");
        }
        String selectionColumns = selectionColumnsBuffer.substring(0,selectionColumnsBuffer.length()-1);

        /*
        tokenRange :
        1.      -9207785194558378121 to -9204547573912250796
        2.      -9204547573912250796 to -9199054268853034612
        ...
        1536.   9219444290392454365 to -9207785194558378121
        Thus, start inclusive, end exclusive
         */
        String fetchStatement = "select "+selectionColumns+" from "+keyspace+ "." +tableName +" where token("+partitionKey+") >= ? and token("+partitionKey+") < ? ";
        String fetchStatementLastTokenRange = "select "+selectionColumns+" from "+keyspace+ "." +tableName +" where token("+partitionKey+") >= ? ";//  and token("+partitionKey+") <= ? ";
        //I think, this is required. Not sure yet, open for discussion.

        /*
        Example 1:
        token range size : 38
        Producers : 7
        1   2  3  4  5  6
        7   8  9 10 11 12
        13 14 15 16 17 18
        19 20 21 22 23
        24 25 26 27 28
        29 30 31 32 33
        34 35 36 37 38
        Example 2:
        token range size : 16
        Producers : 2
        1  2  3  4  5  6  7  8
        9 10 11 12 13 14 15 16
         */
        Producer.setStaticData(consistencyLevel,sleepMilliSeconds);
        System.out.println("TOKEN_PERSONAL_RANGE:"+cluster.getMetadata().getTokenRanges().size()+" FULL COUNT.");
        /*
            tried this when manual paging gave error.
            Still not resolved
            Cluster personalCluster = Cluster.builder().addContactPoint(contactPoint)
                .withQueryOptions(new QueryOptions().setFetchSize(5000))
                .withCredentials(username,password)
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();*/
        TreeSet<TokenRange> tokenRanges = new TreeSet<>(cluster.getMetadata().getTokenRanges());
        Long startValueOfToken = (Long)(tokenRanges.first().getStart().getValue());

        for(TokenRange tokenRange : tokenRanges){
            if(tokensAddedForCurrentConsumer == numberOfTokensPerConsumer){
                if(numberOfProducersWithExtraToken== 0) {
                    producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);
                    /*
                    tried this when manual paging gave error.
                    Still not resolved
                    personalCluster = Cluster.builder().addContactPoint(contactPoint)
                            .withQueryOptions(new QueryOptions().setFetchSize(5000))
                            .withCredentials(username,password)
                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                            .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                            .build();*/
                    tokenRangeSetForOneProducer = new HashSet<>();
                    tokenRangeSetForOneProducer.add(tokenRange);
                    tokensAddedForCurrentConsumer = 1;
                    ++indexOfProducer;
                }else{
                    tokenRangeSetForOneProducer.add(tokenRange);
                    producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);
                    /*
                    tried this when manual paging gave error.
                    Still not resolved
                    personalCluster = Cluster.builder().addContactPoint(contactPoint)
                            .withQueryOptions(new QueryOptions().setFetchSize(5000))
                            .withCredentials(username,password)
                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                            .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                            .build();*/
                    --numberOfProducersWithExtraToken;
                    tokenRangeSetForOneProducer = new HashSet<>();
                    tokensAddedForCurrentConsumer = 0;
                    ++indexOfProducer;
                }
            }else{
                tokenRangeSetForOneProducer.add(tokenRange);
                ++tokensAddedForCurrentConsumer;
            }
        }
        producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);

        return producerThreads;
    }

    public CountDownLatch start(){
        System.out.println("CFS Started");
        for(Thread thread : producerThreads){
            thread.start();
        }
        return latch;

    }

    private String getPartitionKey(Cluster cluster) {
        StringBuffer partitionKeyTemp = new StringBuffer();
        for(ColumnMetadata partitionKeyPart : cluster.getMetadata().getKeyspace(keyspace).getTable(tableName).getPartitionKey()){
            partitionKeyTemp.append(partitionKeyPart.getName()+",");
        }
        String partitionKey = partitionKeyTemp.substring(0,partitionKeyTemp.length()-1);
        return partitionKey;
    }
}
