package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class TransactionEventsSyncHook implements Synchronization
{
    private final Collection<TransactionEventHandler<?>> handlers;
    private final NodeManager nodeManager;

    /**
     * This is null at construction time, then populated in beforeCompletion and
     * used in afterCompletion.
     */
    private List<HandlerAndState> states;
    private TransactionData transactionData;
    private final TransactionManager tm;

    public TransactionEventsSyncHook(
            NodeManager nodeManager,
            Collection<TransactionEventHandler<?>> transactionEventHandlers, 
            TransactionManager tm )
    {
        this.nodeManager = nodeManager;
        this.handlers = transactionEventHandlers;
        this.tm = tm;
    }

    public void beforeCompletion()
    {
        this.transactionData = nodeManager.getTransactionData();
        try
        {
            if ( tm.getStatus() != Status.STATUS_ACTIVE ) 
            {
                return;
            }
        }
        catch ( SystemException e )
        {
            e.printStackTrace();
        }
        states = new ArrayList<HandlerAndState>();
        for ( TransactionEventHandler<?> handler : this.handlers )
        {
            try
            {
                Object state = handler.beforeCommit( transactionData );
                states.add( new HandlerAndState( handler, state ) );
            }
            catch ( Throwable t )
            {
                // TODO Do something more than calling failure and
                // throw exception?
                try
                {
                    tm.setRollbackOnly();
                }
                catch ( Exception e )
                {
                    // TODO Correct?
                    e.printStackTrace();
                }
                
                // This will cause the transaction to throw a
                // TransactionFailureException
                throw new RuntimeException( t );
            }
        }
    }

    @SuppressWarnings("unchecked")
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
        @SuppressWarnings("unchecked")
        private final TransactionEventHandler handler;
        private final Object state;

        public HandlerAndState( TransactionEventHandler<?> handler, Object state )
        {
            this.handler = handler;
            this.state = state;
        }
    }
}
