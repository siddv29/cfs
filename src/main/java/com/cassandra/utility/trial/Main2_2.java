package com.cassandra.utility.trial;

import com.cassandra.utility.method1.CassandraFastFullTableScan;
import com.cassandra.utility.method1.Options;
import com.cassandra.utility.method1.RowTerminal;
import com.datastax.driver.core.Row;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/9/16.
 */
public class Main2_2 {
    public static void main(String... args) throws Exception{
        LinkedBlockingQueue<Row> queue =new LinkedBlockingQueue<>();
        CassandraFastFullTableScan cfs = new CassandraFastFullTableScan("mykeyspace.table_name","10.41.55.111",queue,new Options().setUsername("cassandra").setPassword("cassandra"),new PrintStream(new File("/tmp/cfs_round_1.log")));
        CountDownLatch countDownLatch = cfs.start();
        new NotifyWhenCFSFinished(countDownLatch).start();
        int remainingTerminals=16;//default number of terminals to be added to queue
        Row row;
        int counter=0;
        while(! ((row = queue.take()) instanceof RowTerminal)){
            System.out.println(++counter+":"+row);
        }
    }

    static class NotifyWhenCFSFinished extends Thread{
        CountDownLatch latch;

        public NotifyWhenCFSFinished(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run(){
            System.out.println("Await started");
            try{
                latch.await();
            }catch (Exception e1){
                //what to do
            }
            System.out.println("End of latch awaiting");
            System.out.println("Proceed");

        }

    }

}

