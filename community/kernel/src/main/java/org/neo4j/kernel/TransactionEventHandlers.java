/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Handle the collection of transaction event handlers, and fire events as needed.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class TransactionEventHandlers
    implements Lifecycle
{
    protected final Collection<TransactionEventHandler> transactionEventHandlers = new CopyOnWriteArraySet<TransactionEventHandler>();
    private final TransactionManager txManager;

    public TransactionEventHandlers(
        TransactionManager txManager
    )
    {
        this.txManager = txManager;
    }

    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void start()
        throws Throwable
    {
    }

    @Override
    public void stop()
        throws Throwable
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return handler;
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return unregisterHandler( this.transactionEventHandlers, handler );
    }

    private <T> T unregisterHandler( Collection<?> setOfHandlers, T handler )
    {
        if ( !setOfHandlers.remove( handler ) )
        {
            throw new IllegalStateException( handler + " isn't registered" );
        }
        return handler;
    }

    public boolean hasHandlers()
    {
        return !transactionEventHandlers.isEmpty();
    }

    public void beforeCompletion( TransactionData transactionData,
                                  List<HandlerAndState> states
    )
    {
        for ( TransactionEventHandler<?> handler : this.transactionEventHandlers )
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
                    txManager.setRollbackOnly();
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

    public void afterCompletion( TransactionData transactionData,
                                 int status,
                                 List<HandlerAndState> states
    )
    {
        if ( status == Status.STATUS_COMMITTED )
        {
            for ( HandlerAndState state : states )
            {
                state.handler.afterCommit( transactionData, state.state );
            }
        }
        else if ( status == Status.STATUS_ROLLEDBACK )
        {
            if ( states == null )
            {
                // This means that the transaction was never successful
                return;
            }

            for ( HandlerAndState state : states )
            {
                state.handler.afterRollback( transactionData, state.state );
            }
        }
        else
        {
            throw new RuntimeException( "Unknown status " + status );
        }
    }

    public static class HandlerAndState
    {
        private final TransactionEventHandler handler;
        private final Object state;

        public HandlerAndState( TransactionEventHandler<?> handler, Object state )
        {
            this.handler = handler;
            this.state = state;
        }
    }
}
