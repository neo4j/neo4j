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
import java.util.List;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class TransactionEventsSyncHook implements Synchronization
{
    private final TransactionEventHandlers handlers;

    /**
     * This is null at construction time, then populated in beforeCompletion and
     * used in afterCompletion.
     */
    private List<TransactionEventHandlers.HandlerAndState> states;
    private TransactionData transactionData;
    private final AbstractTransactionManager tm;

    public TransactionEventsSyncHook( TransactionEventHandlers transactionEventHandlers, AbstractTransactionManager tm )
    {
        this.handlers = transactionEventHandlers;
        this.tm = tm;
    }

    public void beforeCompletion()
    {
        this.transactionData = tm.getTransactionState().getTransactionData();
        
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

        states = new ArrayList<TransactionEventHandlers.HandlerAndState>();
        handlers.beforeCompletion(transactionData, states);
    }

    public void afterCompletion( int status )
    {
        handlers.afterCompletion(transactionData, status, states);
    }
}
