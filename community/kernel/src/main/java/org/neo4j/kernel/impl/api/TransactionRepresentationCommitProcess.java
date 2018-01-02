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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotCommit;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotWriteToLog;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.ValidationFailed;

public class TransactionRepresentationCommitProcess implements TransactionCommitProcess
{
    private final TransactionAppender appender;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final IndexUpdatesValidator indexUpdatesValidator;

    public TransactionRepresentationCommitProcess( TransactionAppender appender,
            TransactionRepresentationStoreApplier storeApplier, IndexUpdatesValidator indexUpdatesValidator )
    {
        this.appender = appender;
        this.storeApplier = storeApplier;
        this.indexUpdatesValidator = indexUpdatesValidator;
    }

    @Override
    public long commit( TransactionRepresentation transaction, LockGroup locks, CommitEvent commitEvent,
            TransactionApplicationMode mode ) throws TransactionFailureException
    {
        try ( ValidatedIndexUpdates indexUpdates = validateIndexUpdates( transaction ) )
        {
            Commitment commitment = appendToLog( transaction, commitEvent );
            applyToStore( transaction, locks, commitEvent, indexUpdates, commitment, mode );
            return commitment.transactionId();
        }
    }

    private ValidatedIndexUpdates validateIndexUpdates( TransactionRepresentation transaction )
            throws TransactionFailureException
    {
        try
        {
            return indexUpdatesValidator.validate( transaction );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( ValidationFailed, e, "Validation of index updates failed" );
        }
    }

    private Commitment appendToLog(
            TransactionRepresentation transaction, CommitEvent commitEvent ) throws TransactionFailureException
    {
        Commitment commitment;
        try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
        {
            commitment = appender.append( transaction, logAppendEvent );
        }
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( CouldNotWriteToLog, cause,
                    "Could not append transaction representation to log" );
        }
        commitEvent.setTransactionId( commitment.transactionId() );
        return commitment;
    }

    private void applyToStore(
            TransactionRepresentation transaction, LockGroup locks, CommitEvent commitEvent,
            ValidatedIndexUpdates indexUpdates, Commitment commitment, TransactionApplicationMode mode )
            throws TransactionFailureException
    {
        try ( StoreApplyEvent storeApplyEvent = commitEvent.beginStoreApply() )
        {
            storeApplier.apply( transaction, indexUpdates, locks, commitment.transactionId(), mode );
        }
        // TODO catch different types of exceptions here, some which are OK
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( CouldNotCommit, cause,
                    "Could not apply the transaction to the store after written to log" );
        }
        finally
        {
            commitment.publishAsApplied();
        }
    }
}
