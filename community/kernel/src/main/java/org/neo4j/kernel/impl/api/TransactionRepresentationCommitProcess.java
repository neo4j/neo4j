/*
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotCommit;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.ValidationFailed;

public class TransactionRepresentationCommitProcess implements TransactionCommitProcess
{
    private final LogicalTransactionStore logicalTransactionStore;
    private final KernelHealth kernelHealth;
    private final TransactionIdStore transactionIdStore;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final IndexUpdatesValidator indexUpdatesValidator;

    public TransactionRepresentationCommitProcess( LogicalTransactionStore logicalTransactionStore,
            KernelHealth kernelHealth, TransactionIdStore transactionIdStore,
            TransactionRepresentationStoreApplier storeApplier, IndexUpdatesValidator indexUpdatesValidator )
    {
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.kernelHealth = kernelHealth;
        this.storeApplier = storeApplier;
        this.indexUpdatesValidator = indexUpdatesValidator;
    }

    @Override
    public long commit( TransactionRepresentation transaction, LockGroup locks, CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        try ( ValidatedIndexUpdates indexUpdates = validateIndexUpdates( transaction, mode ) )
        {
            long transactionId = appendToLog( transaction, commitEvent );
            applyToStore( transaction, locks, commitEvent, indexUpdates, transactionId, mode );
            return transactionId;
        }
    }

    private ValidatedIndexUpdates validateIndexUpdates( TransactionRepresentation transaction,
                                                        TransactionApplicationMode mode)
            throws TransactionFailureException
    {
        try
        {
            return indexUpdatesValidator.validate( transaction, mode );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( ValidationFailed, e, "Validation of index updates failed" );
        }
    }

    private long appendToLog(
            TransactionRepresentation transaction, CommitEvent commitEvent ) throws TransactionFailureException
    {
        long transactionId;
        try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
        {
            transactionId = logicalTransactionStore.getAppender().append( transaction, logAppendEvent );
        }
        catch ( Throwable e )
        {
            throw exception( Status.Transaction.CouldNotWriteToLog, e,
                    "Could not append transaction representation to log" );
        }
        commitEvent.setTransactionId( transactionId );
        return transactionId;
    }

    private void applyToStore(
            TransactionRepresentation transaction, LockGroup locks, CommitEvent commitEvent,
            ValidatedIndexUpdates indexUpdates, long transactionId, TransactionApplicationMode mode )
            throws TransactionFailureException
    {
        try ( StoreApplyEvent storeApplyEvent = commitEvent.beginStoreApply() )
        {
            storeApplier.apply( transaction, indexUpdates, locks, transactionId, mode );
        }
        // TODO catch different types of exceptions here, some which are OK
        catch ( Throwable e )
        {
            throw exception( CouldNotCommit, e,
                    "Could not apply the transaction to the store after written to log" );
        }
        finally
        {
            transactionIdStore.transactionClosed( transactionId );
        }
    }

    private TransactionFailureException exception( Status status, Throwable cause, String message )
    {
        kernelHealth.panic( cause );
        return new TransactionFailureException( status, cause, message );
    }
}
