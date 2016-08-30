package com.cassandra.utility.policy;

import com.cassandra.utility.exception.HostDownException;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import java.util.*;

/**
 * Created by siddharth on 29/8/16.
 */
public class LocalOnlyPolicy implements LoadBalancingPolicy {

    private final String personalHostIp;
    private final Host personalHost;
//    private final Iterator<Host> personalHostIterator;


    private static Map<String,String> hostIpToClusterName = new HashMap<>();
    private static Map<String,Set<Host>> clusterNameToHosts = new HashMap<>();
    private static Map<String,Host> hostIpToHostMap = new HashMap<>();

    public LocalOnlyPolicy(String personalHostIp, String userName, String password) {
        this.personalHostIp = personalHostIp;
        synchronized (LocalOnlyPolicy.class){
            if(!hostIpToClusterName.containsKey(personalHostIp)) {
                hostIpToHostMap = new HashMap<>();
                Cluster cluster = Cluster.builder().addContactPoint(personalHostIp)
                        .withCredentials(userName, password)
                        .build();
                String clusterName = cluster.getMetadata().getClusterName();

                for (Host host : cluster.getMetadata().getAllHosts()) {
                    hostIpToClusterName.put(host.getAddress().toString().substring(1), clusterName);
                    hostIpToHostMap.put(host.getAddress().toString().substring(1),host);
                    clusterNameToHosts.putIfAbsent(clusterName,new HashSet<>());
                    clusterNameToHosts.get(clusterName).add(host);
                }
                cluster.close();

            }
        }
        personalHost = hostIpToHostMap.get(personalHostIp);
//        personalHostIterator = ;
        /*
        Add code for multiple reader, one writer.
         */
    }

    public static Set<Host> getHostsByClusterName(String clusterName){
        return clusterNameToHosts.get(clusterName);
    }

    public static String getClusterNameByHostIp(String hostIp){
        return hostIpToClusterName.get(hostIp);
    }




    @Override
    public void init(Cluster cluster, Collection<Host> collection) {
        //right now, not doing anything
    }

    @Override
    public HostDistance distance(Host host) {
        if(host.equals(personalHostIp)){
            return HostDistance.LOCAL;
        }else{
            return HostDistance.IGNORED;
        }
    }

    @Override
    public Iterator<Host> newQueryPlan(String s, Statement statement) {
        //return personalHostIterator;
        return Collections.singletonList(personalHost).iterator();
    }

    @Override
    public void onAdd(Host host) {
//        throw new HostDownException(host);
        /*
        optimise later.
        distribute other node's token range to this node too

        change exception also
         */
    }

    @Override
    public void onUp(Host host) {
        throw new HostDownException(host);
        /*
        optimise later.
        distribute this host's range to the rest of consumers

        change exception also
         */
    }

    @Override
    public void onDown(Host host) {
        throw new HostDownException(host);
        /*
        optimise later.
        distribute other node's token range to this node too
         */
    }

    @Override
    public void onRemove(Host host) {
        throw new HostDownException(host);
        /*
        optimise later.
        distribute other node's token range to this node too

        change exception also
         */
    }

    @Override
    public void close() {
        //do nothing
        //same as DCAwareRoundRobinPolicy
    }
}
