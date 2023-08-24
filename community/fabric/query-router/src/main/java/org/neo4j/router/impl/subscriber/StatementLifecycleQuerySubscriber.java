package org.neo4j.router.impl.subscriber;

import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.DelegatingQuerySubscriber;
import org.neo4j.kernel.impl.query.QuerySubscriber;

public class StatementLifecycleQuerySubscriber extends DelegatingQuerySubscriber {

    private final QueryStatementLifecycles.StatementLifecycle statementLifecycle;

    public StatementLifecycleQuerySubscriber(
            QuerySubscriber querySubscriber, QueryStatementLifecycles.StatementLifecycle statementLifecycle) {
        super(querySubscriber);
        this.statementLifecycle = statementLifecycle;
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        super.onResultCompleted(statistics);
        statementLifecycle.endSuccess();
    }

    @Override
    public void onError(Throwable throwable) throws Exception {
        super.onError(throwable);
        statementLifecycle.endFailure(throwable);
    }
}
