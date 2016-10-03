package com.cassandra.utility.trial;

import com.cassandra.utility.method1.CassandraFastFullTableScan;
import com.cassandra.utility.method1.Options;
import com.cassandra.utility.method1.RowTerminal;
import com.datastax.driver.core.Row;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 8/9/16.
 */
public class Main2 {
    public static void main(String... args){
        if(args.length == 0){
            args = new String [7];
            args[0]="cams.filter_attribute_mapping"; //table identifier
            /*
            Cluster 1
            10.41.55.111
            10.41.55.113
            10.41.55.112
            10.41.55.115
            10.41.55.114
            10.41.55.116

            Cluster 2
            10.41.55.117
            10.41.55.119
            10.41.55.118
             */
            args[1]="10.41.55.117"/*"localhost"*/; //contactPoint
            args[2]="cassandra"; //username
            args[3]="cassandra"; //password
            args[4]="1";//number of consumers
            args[5]="100";//fetch size
            args[6]=null;
        }
        LinkedBlockingQueue<Row> resultQueue = new LinkedBlockingQueue<>();
        Thread dummyConsumer = new DummyMainConsumer(resultQueue);
        dummyConsumer.start();
        CassandraFastFullTableScan cfs = new CassandraFastFullTableScan(args[0],args[1],resultQueue,new Options().setUsername(args[2]).setPassword(args[3]).setNumberOfThreads(Integer.parseInt(args[4])).setFetchSize(Integer.parseInt(args[5])).setDc(args[6]==null?null:args[6])/*,false*/,System.out);
        CountDownLatch countDownLatch = cfs.start();

        try {
            countDownLatch.await();
            dummyConsumer.join();
        }catch (Exception ignore){
            //ignore
        }
        System.out.println("End of main");


    }
    static class DummyMainConsumer extends Thread {
        LinkedBlockingQueue<Row> resultQueue;
        long countRows=0;
        long secondsCounter = 0;
        boolean stopCounting = false;
        public DummyMainConsumer(LinkedBlockingQueue<Row> resultQueue) {
            this.resultQueue = resultQueue;
        }

        public void run() {
            Row row = null;
            new Thread(new Runnable(){
                public void run(){
                    System.out.println("COUNT STARTED");
                    while(!stopCounting){
                        System.out.println("DEBUG:"+secondsCounter+":"+countRows);
                        try{Thread.sleep(1000);}catch (Exception e){}
                        ++secondsCounter;
                    }
                    System.out.println("COUNT FINISHED:"+secondsCounter+":"+countRows);
                }
            }).start();

            while(!stopCounting){
                try {
                    row = resultQueue.take();
                }catch (Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
                if(row instanceof RowTerminal){
                    break;
                }else {
                    ++countRows;
                }
            }
            stopCounting = true;
        }
    }
}
