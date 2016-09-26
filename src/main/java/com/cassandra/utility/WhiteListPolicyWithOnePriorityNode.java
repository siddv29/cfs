package com.cassandra.utility;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by siddharth on 23/9/16.
 */
public class WhiteListPolicyWithOnePriorityNode extends WhiteListPolicy {
    Host primaryHost; // give priority to primary token owner
    Collection<Host> remainingNodes; // rest to contact in case primary owner is busy
    public WhiteListPolicyWithOnePriorityNode(/*LoadBalancingPolicy childPolicy, */Host primaryHost, Collection<Host> allNodesWithReplica, Collection<InetSocketAddress> hostAddress) {
        super(new RoundRobinPolicy(), hostAddress);
        this.primaryHost = primaryHost;
//        allNodesWithReplica.remove(primaryHost); // no need to do this.
        this.remainingNodes = allNodesWithReplica;
    }

    public HostDistance distance(Host host) {
        if(host == primaryHost){
            return HostDistance.LOCAL;
        }else if (remainingNodes.contains(host)){
            return HostDistance.REMOTE;
        }else{
            return HostDistance.IGNORED;
        }
    }

}
