package org.neo4j.kernel;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.core.TransactionState;

public class BeansAPITransaction implements Transaction
{
    private final TransactionContext txCtx;
    private final TransactionState state;
    private final ThreadToStatementContextBridge bridge;

    public BeansAPITransaction( TransactionContext txCtx, TransactionState state, ThreadToStatementContextBridge bridge )
    {
        this.txCtx = txCtx;
        this.state = state;
        this.bridge = bridge;
    }

    @Override
    public void failure()
    {
        txCtx.failure();
    }

    @Override
    public void success()
    {
        txCtx.success();
    }

    @Override
    public void finish()
    {
        txCtx.finish();
        bridge.clearThisThread();
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return state.acquireWriteLock( entity );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return state.acquireReadLock( entity );
    }
}
