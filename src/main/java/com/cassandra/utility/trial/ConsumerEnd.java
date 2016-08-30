package com.cassandra.utility.trial;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

/**
 * Created by siddharth on 30/8/16.
 */
class ConsumerEnd implements ResultSet {

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return null;
    }

    @Override
    public boolean isExhausted() {
        return false;
    }

    @Override
    public Row one() {
        return null;
    }

    @Override
    public List<Row> all() {
        return null;
    }

    @Override
    public Iterator<Row> iterator() {
        return null;
    }

    @Override
    public int getAvailableWithoutFetching() {
        return 0;
    }

    @Override
    public boolean isFullyFetched() {
        return false;
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        return null;
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return null;
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return null;
    }

    @Override
    public boolean wasApplied() {
        return false;
    }
}
