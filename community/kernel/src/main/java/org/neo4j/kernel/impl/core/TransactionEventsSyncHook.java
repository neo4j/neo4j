/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
            if ( this.states == null )
            {
                // This means that the transaction was never successful
                return;
            }
            
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
