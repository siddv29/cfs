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
public class Main {
    public static void main(String... args) throws Exception{
        LinkedBlockingQueue<Row> queue =new LinkedBlockingQueue<>();

        CassandraFastFullTableScan cfs =
                new CassandraFastFullTableScan(args[0],
                        args[1],queue,
                        new Options().setUsername("cassandra").setPassword("cassandra"),
                        new PrintStream(new File(args[2])));

        CountDownLatch countDownLatch = cfs.start();

        new NotifyWhenCFSFinished(countDownLatch).start();

        Row row;
        int counter=0;
        while(! ((row = queue.take()) instanceof RowTerminal)){
            System.out.println(++counter+":"+row);
            /*
              you can use row.getString("column1") and so on
            */
        }
    }

    static class NotifyWhenCFSFinished extends Thread{
        CountDownLatch latch;

        public NotifyWhenCFSFinished(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run(){
            System.out.println("Waiting for CFS to complete");
            try{
                latch.await();
            }catch (Exception e1){
                //ignore
            }
            System.out.println("CFS completed");
        }

    }

}
