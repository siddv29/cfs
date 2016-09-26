package com.cassandra.utility.method2;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Created by siddharth on 26/9/16.
 */
public class ExecutionDataForTokenRange {
//    private final Cluster cluster;
    private final Session session;
    private final BoundStatement boundStatementRestOfTokenRange;
    private final BoundStatement boundStatementLastTokenRange;

    public ExecutionDataForTokenRange(Session session,String fetchStatementRestOfTokenRange, String fetchStatementLastTokenRange) {
        this.session = session;
        boundStatementRestOfTokenRange = new BoundStatement(session.prepare(fetchStatementRestOfTokenRange));
        boundStatementLastTokenRange = new BoundStatement(session.prepare(fetchStatementLastTokenRange));
    }

    public Session getSession() {
        return session;
    }

    public BoundStatement getBoundStatementRestOfTokenRange() {
        return boundStatementRestOfTokenRange;
    }

    public BoundStatement getBoundStatementLastTokenRange() {
        return boundStatementLastTokenRange;
    }
}
