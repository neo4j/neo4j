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
package org.neo4j.server.rest.transactional;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.TxManager;

class TransitionalTxManagementTransactionContext implements TransactionContext
{
    private final TransactionContext ctx;
    private final TxManager txManager;

    private Transaction suspendedTransaction;

    public TransitionalTxManagementTransactionContext( TransactionContext ctx, TxManager txManager )
    {
        this.ctx = ctx;
        this.txManager = txManager;
    }

    @Override
    public StatementContext newStatementContext()
    {
        return ctx.newStatementContext();
    }

    @Override
    public void prepare()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        ctx.commit();
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        ctx.rollback();
    }

    public void suspendSinceTransactionsAreStillThreadBound()
    {
        try
        {
            assert suspendedTransaction == null : "Can't suspend the transaction if it already is suspended.";
            suspendedTransaction = txManager.suspend();
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void resumeSinceTransactionsAreStillThreadBound()
    {
        try
        {
            assert suspendedTransaction != null : "Can't suspend the transaction if it has not first been suspended.";
            txManager.resume( suspendedTransaction );
            suspendedTransaction = null;
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }
}
