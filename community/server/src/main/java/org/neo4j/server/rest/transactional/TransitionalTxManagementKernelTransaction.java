/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

class TransitionalTxManagementKernelTransaction
{
    private final TransactionTerminator txTerminator;
    private final ThreadToStatementContextBridge bridge;

    private TopLevelTransaction suspendedTransaction;

    TransitionalTxManagementKernelTransaction( TransactionTerminator txTerminator, ThreadToStatementContextBridge bridge )
    {
        this.txTerminator = txTerminator;
        this.bridge = bridge;
    }

    public void suspendSinceTransactionsAreStillThreadBound()
    {
        assert suspendedTransaction == null : "Can't suspend the transaction if it already is suspended.";
        suspendedTransaction = bridge.getTopLevelTransactionBoundToThisThread( true );
        bridge.unbindTransactionFromCurrentThread();
    }

    public void resumeSinceTransactionsAreStillThreadBound()
    {
        assert suspendedTransaction != null : "Can't resume the transaction if it has not first been suspended.";
        bridge.bindTransactionToCurrentThread( suspendedTransaction );
        suspendedTransaction = null;
    }

    public void terminate()
    {
        txTerminator.terminate();
    }

    public void rollback()
    {
        try
        {
            KernelTransaction kernelTransactionBoundToThisThread = bridge.getKernelTransactionBoundToThisThread( false );
            kernelTransactionBoundToThisThread.failure();
            kernelTransactionBoundToThisThread.close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            bridge.unbindTransactionFromCurrentThread();
        }
    }

    public void commit()
    {
        try
        {
            KernelTransaction kernelTransactionBoundToThisThread = bridge.getKernelTransactionBoundToThisThread( true );
            kernelTransactionBoundToThisThread.success();
            kernelTransactionBoundToThisThread.close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            bridge.unbindTransactionFromCurrentThread();
        }
    }
}
