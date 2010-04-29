package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class TransactionEventsSyncHook<T> implements Synchronization
{
    private final Collection<TransactionEventHandler<T>> handlers;
    private final NodeManager nodeManager;
    private final Transaction transaction;

    /**
     * This is null at construction time, then populated in beforeCompletion and
     * used in afterCompletion.
     */
    private List<HandlerAndState> states;
    private TransactionData transactionData;

    public TransactionEventsSyncHook(
            NodeManager nodeManager, Transaction transaction,
            Collection<TransactionEventHandler<T>> transactionEventHandlers )
    {
        this.nodeManager = nodeManager;
        this.transaction = transaction;
        this.handlers = transactionEventHandlers;
    }

    public void beforeCompletion()
    {
        this.transactionData = nodeManager.getTransactionData();
        states = new ArrayList<HandlerAndState>();
        for ( TransactionEventHandler<T> handler : this.handlers )
        {
            try
            {
                T state = handler.beforeCommit( transactionData );
                states.add( new HandlerAndState( handler, state ) );
            }
            catch ( Throwable t )
            {
                // TODO Do something more than calling failure and
                // throw exception?
                transaction.failure();
                
                // This will cause the transaction to throw a
                // TransactionFailureException
                throw new RuntimeException( t );
            }
        }
    }

    public void afterCompletion( int status )
    {
        if ( status == Status.STATUS_COMMITTED )
        {
            for ( HandlerAndState state : this.states )
            {
                state.handler.afterCommit( this.transactionData, state.state );
            }
        }
        else if ( status == Status.STATUS_ROLLEDBACK )
        {
            for ( HandlerAndState state : this.states )
            {
                state.handler.afterRollback( this.transactionData, state.state );
            }
        }
        else
        {
            throw new RuntimeException( "Unknown status " + status );
        }
    }

    private class HandlerAndState
    {
        private final TransactionEventHandler<T> handler;
        private final T state;

        public HandlerAndState( TransactionEventHandler<T> handler, T state )
        {
            this.handler = handler;
            this.state = state;
        }
    }
}
