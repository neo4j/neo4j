/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppendException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

public class TransactionRepresentationCommitProcess implements TransactionCommitProcess
{
    private final LogicalTransactionStore logicalTransactionStore;
    private final KernelHealth kernelHealth;
    private final TransactionIdStore transactionIdStore;
    private final boolean recovery;
    private final TransactionRepresentationStoreApplier storeApplier;

    public TransactionRepresentationCommitProcess( LogicalTransactionStore logicalTransactionStore,
            KernelHealth kernelHealth, TransactionIdStore transactionIdStore,
            TransactionRepresentationStoreApplier storeApplier, boolean recovery )
    {
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.recovery = recovery;
        this.kernelHealth = kernelHealth;
        this.storeApplier = storeApplier;
    }

    @Override
    public long commit( TransactionRepresentation representation ) throws TransactionFailureException
    {
        long transactionId = persistTransaction( representation );
        // apply changes to the store
        try
        {
            storeApplier.apply( representation, transactionId, recovery );
        }
        // TODO catch different types of exceptions here, some which are OK
        catch ( IOException e )
        {
            throw exception( Status.Transaction.CouldNotCommit, e,
                    "Could not apply the transaction to the store after written to log" );
        }
        finally
        {
            transactionIdStore.transactionClosed( transactionId );
        }
        return transactionId;
    }

    private TransactionFailureException exception( Status status, IOException e, String message )
    {
        kernelHealth.panic( e );
        return new TransactionFailureException( status, e, message );
    }

    private long persistTransaction( TransactionRepresentation tx ) throws TransactionFailureException
    {
        try
        {
            return logicalTransactionStore.getAppender().append( tx );
        }
        catch ( TransactionAppendException e )
        {
            // Special case where a new transaction id was generated, but the transaction failed to be appended
            // and so there's a transaction that would otherwise never be notified as closed. The appender
            // throws a specific exception for this scenario where it contains the generated transaction id if
            // the append method got that far.
            if ( e.hasNewTransactionIdGenerated() )
            {
                transactionIdStore.transactionClosed( e.newTransactionIdGenerated() );
            }
            throw exception( Status.Transaction.CouldNotWriteToLog, e,
                    "Could not write transaction representation to log" );
        }
    }
}
