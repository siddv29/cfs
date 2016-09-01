package com.cassandra.utility.trial;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/8/16.
 */
public class Producer extends Thread {
    private final Cluster cluster;
    private final Session session;
    private final HashSet<MyTokenRange> personalTokenRange;
    private final BoundStatement boundStatement;
    private final String keyspace;
    private final String tablename;
    private final String partitionKey;
    private final LinkedBlockingQueue<ResultSet> queue;
    private final CountDownLatch latch;

    public Producer(Cluster cluster, HashSet<MyTokenRange> personalTokenRange, String tableIdentifier, LinkedBlockingQueue<ResultSet> queue, CountDownLatch latch) {
        this.cluster = cluster;
        this.queue = queue;
        this.latch = latch;
        this.session = cluster.connect();
        this.personalTokenRange = personalTokenRange;
        String temp[]= tableIdentifier.split("\\.");
        this.keyspace = temp[0];
        this.tablename = temp[1];
        /*StringBuffer partitionKey = new StringBuffer();
        for(ColumnMetadata partitionKeyPart : cluster.getMetadata().getKeyspace(keyspace).getTable(tablename).getPartitionKey()){
            partitionKey.append(partitionKeyPart.getName()+",");
        }*/
        this.partitionKey = /*partitionKey.substring(0,partitionKey.length()-1).toString();*/"supc";
        System.out.println("preparing :"+"select * from "+tableIdentifier+" where token("+partitionKey+") >= ? and token("+partitionKey+")< ?");
        BoundStatement boundStatement = null;
        try{
            boundStatement = new BoundStatement(session.prepare("select * from "+tableIdentifier+" where token("+partitionKey+") >= ? and token("+partitionKey+")< ?"));
        }catch (Exception e){
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
        this.boundStatement= boundStatement;
        this.boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        this.boundStatement.setFetchSize(5000);
    }

    private void fetchLoop(MyTokenRange myTokenRange)throws InterruptedException{
        String currentPageInfo = null;
        boolean oneFetchDone = false;
        boundStatement.bind(myTokenRange.getStart(),myTokenRange.getEnd());
        do {
            try {
                System.out.println("Hitting..." + currentPageInfo + "...");
                if (currentPageInfo != null) {
                    boundStatement.setPagingState(PagingState.fromString(currentPageInfo));
                }
                ResultSet rs = session.execute(boundStatement);
                System.out.println("Pushed to queue");
//                queue.put(rs);
                oneFetchDone = true;
                PagingState nextPage = rs.getExecutionInfo().getPagingState();
                String nextPageInfo = null;
                if (nextPage != null) {
                    nextPageInfo = nextPage.toString();
                }
                currentPageInfo = nextPageInfo;
            } catch (NoHostAvailableException e) {
                System.out.println("No host available exception... going to sleep for 1 sec");
                try {Thread.sleep(1000 * 1);} catch (Exception e2) {}
            }
            System.out.println("Finished while loop");

        } while (!oneFetchDone && currentPageInfo != null);
    }
    public void run(){
        for(MyTokenRange tokenRange : personalTokenRange){
            try {
                fetchLoop(tokenRange);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        latch.countDown();
    }
}
