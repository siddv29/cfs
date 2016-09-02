package com.cassandra.utility.trial;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/8/16.
 */
public class Main1 {
    public static void main(String... args) throws IOException, InterruptedException {
        System.out.println("STARTED : "+new Date());
        if(args.length == 0){
            args=new String [4];
            args[0]= Constants.IP;
            args[1]= Constants.USERNAME;
            args[2]= Constants.PASSWORD;
            args[3]= Constants.DESCRIBERING_FILE;
        }
        Trials trials = new Trials(args[0],args[1],args[2],args[3]);
        Map<Host,Cluster> hostClusterMap = trials.getHostToCluster();
        LinkedBlockingQueue<ResultSet> queue = new LinkedBlockingQueue<>();
        int numberOfThreads = hostClusterMap.size();
        String tableIdentifier = Constants.KEYSPACE+"."+Constants.TABLE_NAME;
        CountDownLatch latch = new CountDownLatch(hostClusterMap.size());
        System.out.println("LATCH VALUE"+latch.getCount());
        for(Host host : hostClusterMap.keySet()){
            Producer p =new Producer(host,hostClusterMap.get(host),trials.getHostToTokenRange().get(host.getAddress().toString().substring(1)),tableIdentifier,queue,latch)/*.start()*/;
//            p.run();
            p.start();
//            break;
        }
//        queue.put(new ConsumerEnd());
        latch.await();
        System.out.println("MAIN: await completed");

    }
}
