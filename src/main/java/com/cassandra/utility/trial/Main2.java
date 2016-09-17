package com.cassandra.utility.trial;

import com.cassandra.utility.method2.CassandraFastFullTableScan;
import com.cassandra.utility.method2.Options;
import com.cassandra.utility.method2.RowTerminal;
import com.datastax.driver.core.Row;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 8/9/16.
 */
public class Main2 {
    public static void main(String... args){
        if(args.length == 0){
            args = new String [6];
            args[0]="cams.product_filter_mapping";
            args[1]=/*"30.0.3.79"*/"localhost";
            args[2]="cams_team";
            args[3]="cams0all";
            args[4]="2";
            args[5]="5000";
        }
        LinkedBlockingQueue<Row> resultQueue = new LinkedBlockingQueue<>();
        Thread dummyConsumer = new DummyMainConsumer(resultQueue,Integer.parseInt(args[4]));
        dummyConsumer.start();
        CassandraFastFullTableScan cfs = new CassandraFastFullTableScan(args[0],args[1],resultQueue,new Options().setUsername(args[2]).setPassword(args[3]).setNumberOfThreads(Integer.parseInt(args[4])).setFetchSize(Integer.parseInt(args[5])));
        CountDownLatch countDownLatch = cfs.start();

        try {
            countDownLatch.await();
            dummyConsumer.join();
        }catch (Exception ignore){
            //ignore
        }
        System.out.println("End of main");
        cfs.closeCluster();

    }
    static class DummyMainConsumer extends Thread {
        LinkedBlockingQueue<Row> resultQueue;
        int sentinelRowMarker;
        long countRows=0;
        long secondsCounter = 0;
        boolean stopCounting = false;
        public DummyMainConsumer(LinkedBlockingQueue<Row> resultQueue,int sentinelRowMarker) {
            this.resultQueue = resultQueue;
            this.sentinelRowMarker = sentinelRowMarker;
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
                    System.out.println("COUNT FINISHED");
                }
            }).start();
            while(sentinelRowMarker > 0){
                try {
                    row = resultQueue.take();
                }catch (Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
                if(row instanceof RowTerminal){
                    --sentinelRowMarker;
                }else {
                    ++countRows;
                }
            }
            stopCounting = true;
        }
    }
}
