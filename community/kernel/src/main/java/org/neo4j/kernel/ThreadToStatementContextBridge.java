/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link StatementContext} when it performs operations.
 */
public class ThreadToStatementContextBridge
{

    private final StatementContext readOnlyStatementCtx;
    private final ThreadLocal<TransactionContext> transactionContextForThread = new ThreadLocal<TransactionContext>();
    private final Map<TransactionContext, StatementContext> currentStatementCtx =
            new ConcurrentHashMap<TransactionContext, StatementContext>();

    public ThreadToStatementContextBridge(StatementContext readOnlyStatementCtx)
    {
        this.readOnlyStatementCtx = readOnlyStatementCtx;
    }

    public StatementContext getCtxForReading()
    {
        TransactionContext txCtx = transactionContextForThread.get();
        if(txCtx != null)
        {
            return contextForTransaction(txCtx);
        }

        return readOnlyStatementCtx;
    }
    public StatementContext getCtxForWriting()
    {
        TransactionContext txCtx = transactionContextForThread.get();
        if(txCtx != null)
        {
            return contextForTransaction(txCtx);
        }

        throw new NotInTransactionException( "You have to start a transaction to perform write operations." );
    }

    private StatementContext contextForTransaction( TransactionContext txCtx )
    {
        StatementContext stmtCtx = currentStatementCtx.get( txCtx );
        if(stmtCtx == null)
        {
            stmtCtx = txCtx.newStatementContext();
            currentStatementCtx.put( txCtx, stmtCtx );
        }

        return stmtCtx;
    }

    public void setTransactionContextForThread( TransactionContext ctx )
    {
        transactionContextForThread.set( ctx );
    }

    public void clearThisThread()
    {
        currentStatementCtx.remove( transactionContextForThread.get() );
        transactionContextForThread.remove();
    }
}
