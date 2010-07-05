package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.event.TransactionEventHandler;

public interface TxEventSyncHookFactory
{
    /**
     * Creates a new {@link TransactionEventsSyncHook} instance of there
     * are any registered {@link TransactionEventHandler}s, else {@code null}.
     * @return a new {@link TransactionEventsSyncHook} or {@code null} if
     * there were no registered {@link TransactionEventHandler}s.
     */
    TransactionEventsSyncHook create();
}
