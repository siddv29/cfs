package com.cassandra.utility.trial;

import com.cassandra.utility.policy.LocalOnlyPolicy;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

/**
 * Created by siddharth on 29/8/16.
 */
public class Trials {
//    private final Map<String,String> properties;
    private final Map<String,HashSet<MyTokenRange>> hostToTokenRange;
    private final Map<Host,Cluster> hostToCluster;
//    private final String contactPoint;
//    private final String username;
//    private final String password;
    Cluster tempClusterObj;
    Trials(String contactPoint,String username,String password, String nodetoolDescriberingFile) throws IOException {
//        this.contactPoint = contactPoint;
//        this.username = username;
//        this.password = password;
        hostToTokenRange  = new HashMap<>();
        parseNodetoolDescriberingFile(nodetoolDescriberingFile);
        hostToCluster = initHostToCluster(contactPoint,username,password);

    }



    private Map<Host, Cluster> initHostToCluster(String contactPoint,String username,String password) {
        LocalOnlyPolicy localOnlyPolicy = new LocalOnlyPolicy(contactPoint,username,password);
        final Map<Host,Cluster> hostToCluster = new HashMap<>();
        for(Host host : LocalOnlyPolicy.getHostsByClusterName(LocalOnlyPolicy.getClusterNameByHostIp(contactPoint))){
            Cluster individualHostCluster = Cluster.builder().
                    addContactPoint(contactPoint)
                    .withQueryOptions(new QueryOptions().setFetchSize(5000))
                    .withCredentials(username, password)
                    .withLoadBalancingPolicy(new LocalOnlyPolicy(host.toString().substring(1).split(":")[0],username,password))
                    .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000*60*60).setConnectTimeoutMillis(1000*60*60))
                    .build();
            hostToCluster.put(host,individualHostCluster);
        }
        return hostToCluster;
    }

    public Map<Host,Cluster> getHostToCluster(){
        return hostToCluster;
    }

    public Map<String, HashSet<MyTokenRange>> getHostToTokenRange() {
        return hostToTokenRange;
    }

    private void parseNodetoolDescriberingFile(String nodetoolDescriberingFile) throws IOException {
        BufferedReader fileIn = new BufferedReader(new FileReader(nodetoolDescriberingFile));
        String line;
        while((line = fileIn.readLine())!=null){

//            	line = "TokenRange(start_token:1943978523300203561, end_token:2137919499801737315, endpoints:[127.0.0.3, 127.0.0.6, 127.0.0.7, 127.0.0.2, 127.0.0.5, 127.0.0.1], rpc_endpoints:[127.0.0.3, 127.0.0.6, 127.0.0.7, 127.0.0.2, 127.0.0.5, 127.0.0.1], endpoint_details:[EndpointDetails(host:127.0.0.3, datacenter:dc1, rack:r1), EndpointDetails(host:127.0.0.6, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.7, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.2, datacenter:dc1, rack:r1), EndpointDetails(host:127.0.0.5, datacenter:dc2, rack:r1), EndpointDetails(host:127.0.0.1, datacenter:dc1, rack:r1)])";

            //no sanity done
            MyTokenRange myTokenRange = new MyTokenRange(Long.parseLong(line.split("start_token:")[1].split(",")[0]),Long.parseLong(line.split("end_token:")[1].split(",")[0]));
            String hostIP = line.split("endpoints:")[1].split(",")[0].substring(1);
            hostToTokenRange.putIfAbsent(hostIP,new HashSet<>());
            hostToTokenRange.get(hostIP).add(myTokenRange);
        }
    }


}
