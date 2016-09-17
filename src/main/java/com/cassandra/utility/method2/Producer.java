package com.cassandra.utility.method2;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 7/9/16.
 */
/*public*/ class Producer extends Thread {
    private final Set<TokenRange> tokenRangeSetForProducer;
    private final Session session;
    private final CountDownLatch latch;
    private final LinkedBlockingQueue</*ResultSet*/Row> personalQueue;
    private final LinkedBlockingQueue<Row> mainQueue;
    private final Thread dedicatedConsumer;
    private final BoundStatement boundStatement;
    private static ConsistencyLevel consistencyLevel;
    private static int sleepMilliSeconds;
    protected static boolean printDebugStatements;
    private final int fetchSize;

    protected static void  setStaticData(ConsistencyLevel consistencyLevel, int sleepMilliSeconds){
        Producer.consistencyLevel = consistencyLevel;
        Producer.sleepMilliSeconds = sleepMilliSeconds;
        Producer.printDebugStatements = true;
    }



    protected Producer(String threadName,Set<TokenRange> tokenRangeSetForProducer, Session session/*Cluster cluster*/, CountDownLatch latch, LinkedBlockingQueue mainQueue, int personalQueueSize,String fetchStatement,int fetchSize) {
        super(threadName);
        System.out.println("TOKEN_PERSONAL_RANGE:"+tokenRangeSetForProducer.size()+ "" +Thread.currentThread().getName());
        this.tokenRangeSetForProducer = tokenRangeSetForProducer;
        this.session = /*cluster.connect()*/session;
        this.latch = latch;
        this.mainQueue = mainQueue;
        this.personalQueue = new LinkedBlockingQueue(personalQueueSize);
        boundStatement = new BoundStatement(session.prepare(fetchStatement));
        this.fetchSize = fetchSize;
        this.dedicatedConsumer = new DedicatedConsumer("DedicatedConsumer_FOR_"+threadName,personalQueue,this.mainQueue,this.latch);
    }

    public void run(){
        dedicatedConsumer.start();
        for(TokenRange tokenRange : tokenRangeSetForProducer){
            if(printDebugStatements) System.out.println("TOKEN_RANGE : "+tokenRange.getStart()+";"+tokenRange.getEnd()+ " " +Thread.currentThread().getName());
            fetchLoopWithAutomaticPaging(tokenRange);
        }

        try{
            personalQueue.put(/*new ProducerEnd()*//*null*/new RowTerminal());
        }catch (InterruptedException e){
            System.out.println("Interrupted Excpetion while pushing terminal value to personalQueue. "+Thread.currentThread().getName()+" thread.");
            System.exit(1);
        }

        try{
            dedicatedConsumer.join();
        }catch (InterruptedException e){
            System.out.println("Dedicated consumer interrupted for "+Thread.currentThread().getName()+" thread. Join failed");
            System.exit(1);
        }
        session.close();
//        try{
//            latch.await();
//        }catch (InterruptedException e){
//            System.out.println("Latch interrupted. "+Thread.currentThread().getName()+" thread.");
//            System.exit(1);
//        }
    }

    //works.
    //but try to make manualpaging work
    //it might be faster
    private void fetchLoopWithAutomaticPaging(TokenRange tokenRange){
        boundStatement.bind(tokenRange.getStart().getValue(),tokenRange.getEnd().getValue());
        boundStatement.setConsistencyLevel(consistencyLevel);
        boundStatement.setFetchSize(/*10*//*5000*/fetchSize);
        //add retry logic, if needed
        ResultSet resultSet = session.execute(boundStatement);
        boolean completed = false;
        Token lastProcessedToken=null;
        while(!completed) {
            try {
                for (Row row : resultSet) {
                    try {
//                        lastProcessedToken = row.getPartitionKeyToken();
                        personalQueue.put(row);
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while pushing row to personal queue." + Thread.currentThread().getName() + " thread.");
                        System.exit(1);
                    }
                }
                completed = true;
            }catch (Exception e){
                System.out.println("READ EXCEPTION." + Thread.currentThread().getName() + " thread. Trying again."+e);
                e.printStackTrace();
//                boundStatement.bind((long)lastProcessedToken.getValue()+1,tokenRange.getEnd().getValue());
                resultSet = session.execute(boundStatement);
                try{Thread.sleep(1000);}catch (Exception e1){}
            }
        }

    }
    private void fetchLoopWithManualPaging(TokenRange tokenRange) {
        //doesn't work
        //fails.
        /*
        Exception in thread "Producer-0" com.datastax.driver.core.exceptions.ServerError: An unexpected error occurred server side on /30.0.229.167:9042: java.lang.AssertionError: [DecoratedKey(-5837859558714651203, 53444c343130343438333136),min(-8551554565574869962)]
            at com.datastax.driver.core.exceptions.ServerError.copy(ServerError.java:63)
            at com.datastax.driver.core.exceptions.ServerError.copy(ServerError.java:25)
            at com.datastax.driver.core.DriverThrowables.propagateCause(DriverThrowables.java:37)
            at com.datastax.driver.core.DefaultResultSetFuture.getUninterruptibly(DefaultResultSetFuture.java:245)
            at com.datastax.driver.core.AbstractSession.execute(AbstractSession.java:64)
            at com.cassandra.utility.Producer.fetchLoop(Producer.java:84)
            at com.cassandra.utility.Producer.run(Producer.java:48)
        Caused by: com.datastax.driver.core.exceptions.ServerError: An unexpected error occurred server side on /30.0.229.167:9042: java.lang.AssertionError: [DecoratedKey(-5837859558714651203, 53444c343130343438333136),min(-8551554565574869962)]
            at com.datastax.driver.core.Responses$Error.asException(Responses.java:108)
            at com.datastax.driver.core.RequestHandler$SpeculativeExecution.onSet(RequestHandler.java:500)
            at com.datastax.driver.core.Connection$Dispatcher.channelRead0(Connection.java:1012)
            at com.datastax.driver.core.Connection$Dispatcher.channelRead0(Connection.java:935)
            at io.netty.channel.SimpleChannelInboundHandler.channelRead(SimpleChannelInboundHandler.java:105)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.timeout.IdleStateHandler.channelRead(IdleStateHandler.java:266)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.codec.MessageToMessageDecoder.channelRead(MessageToMessageDecoder.java:102)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.codec.ByteToMessageDecoder.fireChannelRead(ByteToMessageDecoder.java:293)
            at io.netty.handler.codec.ByteToMessageDecoder.channelRead(ByteToMessageDecoder.java:267)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1280)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:890)
            at io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:131)
            at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:564)
            at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:505)
            at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:419)
            at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:391)
            at io.netty.util.concurrent.SingleThreadEventExecutor$2.run(SingleThreadEventExecutor.java:112)
            at java.lang.Thread.run(Thread.java:745)

         */
        boundStatement.bind(tokenRange.getStart().getValue(),tokenRange.getEnd().getValue());
        boundStatement.setConsistencyLevel(consistencyLevel);
        boundStatement.setFetchSize(10);
        String currentPageInfo = null;
//        boolean oneFetchDone = false;
        do {
            try {
                if(printDebugStatements) System.out.println("Hitting..." + currentPageInfo + "..."+Thread.currentThread().getName()+" thread.");
                if (currentPageInfo != null) {
                    boundStatement.setPagingState(PagingState.fromString(currentPageInfo));
                }
                ResultSet rs = session.execute(boundStatement);
//                oneFetchDone = true;
                if(printDebugStatements) System.out.println("Pushed to queue");
                try{
                    personalQueue.put(/*rs*/null);
                }catch (InterruptedException e){
                    System.out.println("Interrupted Exception while pushing result set to personal queue."+Thread.currentThread().getName()+" thread.");
                    System.exit(1);
                }
                PagingState nextPage = rs.getExecutionInfo().getPagingState();
                String nextPageInfo = null;
                if (nextPage != null) {
                    nextPageInfo = nextPage.toString();
                }
                currentPageInfo = nextPageInfo;
            } catch (NoHostAvailableException e) {
                if(printDebugStatements) System.out.println("No host available exception... going to sleep for 1 sec"+Thread.currentThread().getName()+" thread.");
                try {Thread.sleep(sleepMilliSeconds);} catch (Exception e2) {/*nothing*/}
            }
            if(printDebugStatements) System.out.println("Finished while loop"+Thread.currentThread().getName()+" thread.");
        } while (/*(!oneFetchDone && currentPageInfo == null) ||*//*(!oneFetchDone)||*/(currentPageInfo != null));
    }
}
