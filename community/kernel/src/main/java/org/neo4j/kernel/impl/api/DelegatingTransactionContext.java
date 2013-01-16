package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

public class DelegatingTransactionContext implements TransactionContext
{
    private final TransactionContext delegate;

    public DelegatingTransactionContext( TransactionContext delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public StatementContext newStatementContext()
    {
        return delegate.newStatementContext();
    }

    @Override
    public TransactionContext success()
    {
        return delegate.success();
    }

    @Override
    public void finish()
    {
        delegate.finish();
    }
}
