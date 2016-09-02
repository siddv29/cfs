package com.cassandra.utility.trial;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/8/16.
 */
public class Producer extends Thread {
    private final Host host;
    private final Cluster cluster;
    private final Session session;
    private final Set<MyTokenRange> personalTokenRange;
    private final BoundStatement boundStatement;
    private final String keyspace;
    private final String tablename;
    private final String partitionKey;
    private final LinkedBlockingQueue<ResultSet> queue;
    private final CountDownLatch latch;
    private long totalCount;

    public Producer(Host host,Cluster cluster, Set<MyTokenRange> personalTokenRange, String tableIdentifier, LinkedBlockingQueue<ResultSet> queue, CountDownLatch latch) {
        this.totalCount = 0l;
        this.host = host;
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
        this.partitionKey = /*partitionKey.substring(0,partitionKey.length()-1).toString();*/Constants.PARTITION_KEY;
        System.out.println("preparing :"+"select * from "+tableIdentifier+" where token("+partitionKey+") >= ? and token("+partitionKey+")< ?");
        BoundStatement boundStatement = null;
        try{
            boundStatement = new BoundStatement(session.prepare("select * from "+tableIdentifier+" where token("+partitionKey+") >= ? and token("+partitionKey+")< ? "));
        }catch (Exception e){
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
        this.boundStatement= boundStatement;
        this.boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        this.boundStatement.setFetchSize(5000);
    }

    private void fetchLoop(int debugCounter_tokenRange,MyTokenRange myTokenRange)throws /*InterruptedException*/Exception{
        String currentPageInfo = null;
        boolean oneFetchDone = false;
        boundStatement.bind(myTokenRange.getStart(),myTokenRange.getEnd());
        /*
        Doesn;t work. Why? Not known.
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

        } while ((!oneFetchDone && currentPageInfo == null) ||(currentPageInfo != null));*/
        int currentPtr = 0;
        for(Row row : session.execute(boundStatement)){
            ++totalCount;
            if((++currentPtr)%5000 == 0){
                System.out.println("one completed full");
                currentPtr = 0;
            }
        }
        System.out.println("one completed");

    }
    public void run() {
        try {
            int debugCounter_tokenRange = 0;
            for (MyTokenRange tokenRange : personalTokenRange) {
                try {
                    System.out.println("DEBUG" + host+"=="+tokenRange);
                    fetchLoop(++debugCounter_tokenRange,tokenRange);
                } catch (Exception e) {
                    System.out.println("EXCEPTION IN FETCH LOOP" + host+"=="+tokenRange);
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            System.out.println("COUNT=="+host+"=="+totalCount);
            System.out.println("ENDED : "+host+":"+new Date());
            latch.countDown();
        }catch (Exception e){
            System.out.println("error occured in "+Thread.currentThread().getName()+"; "+e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
