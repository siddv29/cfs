package com.cassandra.utility.method1;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 7/9/16.
 */
/*public*/ class DedicatedConsumer extends Thread{
    private final LinkedBlockingQueue</*ResultSet*/Row> personalQueue;
    private final LinkedBlockingQueue<Row> mainQueue;
    private final CountDownLatch latch;
    private static PrintStream loggingFile;

    protected static void  setStaticData(PrintStream loggingFile){
        DedicatedConsumer.loggingFile= loggingFile;
    }

    public DedicatedConsumer(String dedicatedConsumerName, LinkedBlockingQueue</*ResultSet*/Row> personalQueue, LinkedBlockingQueue<Row> mainQueue, CountDownLatch latch) {
        super(dedicatedConsumerName);
        this.personalQueue = personalQueue;
        this.mainQueue = mainQueue;
        this.latch = latch;
    }

    public void run(){
        if(Producer.printDebugStatements) loggingFile.println(Thread.currentThread().getName()+" started.");
        ResultSet rs=null;
        Row row = null;

        do {
            //logic for processing ResultSet
            /*try {
                rs = personalQueue.take();
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception while getting data from personal queue."+Thread.currentThread().getName()+" thread.");
                e.printStackTrace();
                System.exit(1);
            }
            if (rs instanceof ProducerEnd) {
                break;
            }
            int remaining = rs.getAvailableWithoutFetching();
            if(Producer.printDebugStatements) System.out.println("Consuming " + remaining+"."+Thread.currentThread().getName()+" thread.");
            if (remaining != 0) {
                for (Row row : rs) {
                    try{
                        mainQueue.put(row);
                    }catch (InterruptedException e){
                        System.out.println("Interrupted Exception while pushing row to main queue."+Thread.currentThread().getName()+" thread.");
                        System.exit(1);
                    }
                    if (--remaining == 0) {
                        break;
                    }
                }
            }*/
            try {
                row = personalQueue.take();
            } catch (InterruptedException e) {
                loggingFile.println("Interrupted exception while getting row from personal queue."+Thread.currentThread().getName()+" thread.");
                e.printStackTrace();
                System.exit(1);
            }
            if(row instanceof RowTerminal){
                break;
            }
            try{
                mainQueue.put(row);
            }catch (InterruptedException e){
                loggingFile.println("Interrupted Exception while pushing row to main queue."+Thread.currentThread().getName()+" thread.");
                System.exit(1);
            }

        } while (true);
//        try{
            latch.countDown();
//            mainQueue.put(new RowTerminal());
            //no need.
            //added once when cluster about to be closed.
//        }catch (InterruptedException e){
//            System.out.println("Interrupted Exception while pushing terminal row to main queue."+Thread.currentThread().getName()+" thread.");
//            System.exit(1);
//        }
    }
}
