package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;

public class DelegatingQuerySubscriber implements QuerySubscriber {
    private final QuerySubscriber querySubscriber;

    public DelegatingQuerySubscriber(QuerySubscriber querySubscriber) {
        this.querySubscriber = querySubscriber;
    }

    @Override
    public void onResult(int numberOfFields) throws Exception {
        querySubscriber.onResult(numberOfFields);
    }

    @Override
    public void onRecord() throws Exception {
        querySubscriber.onRecord();
    }

    @Override
    public void onField(int offset, AnyValue value) throws Exception {
        querySubscriber.onField(offset, value);
    }

    @Override
    public void onRecordCompleted() throws Exception {
        querySubscriber.onRecordCompleted();
    }

    @Override
    public void onError(Throwable throwable) throws Exception {
        querySubscriber.onError(throwable);
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        querySubscriber.onResultCompleted(statistics);
    }
}
