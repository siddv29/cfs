package com.cassandra.utility.trial;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TokenRange;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by siddharth on 30/8/16.
 */
public class Main1 {
    public static void main(String... args) throws IOException, InterruptedException {
        System.out.println("STARTED : "+new Date());
        if(args.length == 0){
            args=new String [9];
            args[0]= Constants.IP;
            args[1]= Constants.USERNAME;
            args[2]= Constants.PASSWORD;
            args[3]= Constants.DESCRIBERING_FILE;
            args[4]=Constants.CONSUMERS_PER_NODE;
            args[5]="cams";
            args[6]="product_filter_mapping";
            args[7]="supc";
            args[8]=Constants.FETCH_SIZE;
        }
        Constants.KEYSPACE = args[5];
        Constants.TABLE_NAME=args[6];
        Constants.PARTITION_KEY = args[7];
        Constants.FETCH_SIZE = args[8];
        Trials trials = new Trials(args[0],args[1],args[2],args[3],Integer.parseInt(args[4]));
        Map<Host,List<Cluster>> hostClusterMap = trials.getHostToClusters();
        LinkedBlockingQueue<ResultSet> queue = new LinkedBlockingQueue<>();
        int numberOfThreads = hostClusterMap.size();
        String tableIdentifier = Constants.KEYSPACE+"."+Constants.TABLE_NAME;
        CountDownLatch latch = new CountDownLatch(hostClusterMap.size()*Integer.parseInt(args[4]));
        System.out.println("LATCH VALUE"+latch.getCount());
        Thread[] threads = new Thread[(int)latch.getCount()];
        int threadCounter=0;
        for(Host host : hostClusterMap.keySet()){
            //give Producer cluster and token Range set(subset of entire) 256/4
            HashMap<Integer,Cluster> clusterIndex = new HashMap<>();
            HashMap<Integer,Set<MyTokenRange>> tokenRangeIndex = new HashMap<>();
            int i=0;
            for(Cluster individualCluster : hostClusterMap.get(host)){
                clusterIndex.put(i,individualCluster);
                i++;
            }
            int groupSize = trials.getHostToTokenRange().get(host.getAddress().toString().substring(1)).size()/(Integer.parseInt(args[4]));
            int counter=groupSize-1;
            i=-1;
            //groupSize+=(trials.getHostToTokenRange().get(host.getAddress().toString().substring(1)).size()%(Integer.parseInt(args[4]))==0?0:1);
            //assumed, size divided equally
            tokenRangeIndex.put(0,new HashSet<>());
            for(MyTokenRange myTokenRange : trials.getHostToTokenRange().get(host.getAddress().toString().substring(1))){
                ++counter;
                if(counter == groupSize){
                    counter= 0;
                    ++i;
                    tokenRangeIndex.put(i,new HashSet<>());
                }
                tokenRangeIndex.get(i).add(myTokenRange);
            }
            for(i=0;i<(Integer.parseInt(args[4]));i++){
                System.out.println(host+","+tokenRangeIndex.get(i).size());
                Producer p =new Producer(host,clusterIndex.get(i),tokenRangeIndex.get(i),tableIdentifier,queue,latch);
                threads[threadCounter]=p;
                ++threadCounter;
                System.out.println("THREAD_READY");
            }



//            Producer p =new Producer(host,hostClusterMap.get(host),trials.getHostToTokenRange().get(host.getAddress().toString().substring(1)),tableIdentifier,queue,latch)/*.start()*/;
//            p.run();
//            p.start();
//            break;
        }
        for(int i = 0;i<threads.length;i++){
            System.out.println("THREAD_STARTED:"+i);
            threads[i].start();
        }
//        queue.put(new ConsumerEnd());
        latch.await();
        System.out.println("MAIN: await completed");
        System.exit(1);

    }
}
