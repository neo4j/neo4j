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

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotCommit;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.CouldNotWriteToLog;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.ValidationFailed;
import static org.neo4j.kernel.impl.api.TransactionToApply.TRANSACTION_ID_NOT_SPECIFIED;

public class TransactionRepresentationCommitProcess implements TransactionCommitProcess
{
    private final TransactionAppender appender;
    private final StorageEngine storageEngine;
    private final IndexUpdatesValidator indexUpdatesValidator;

    public TransactionRepresentationCommitProcess( TransactionAppender appender,
            StorageEngine storageEngine, IndexUpdatesValidator indexUpdatesValidator )
    {
        this.appender = appender;
        this.storageEngine = storageEngine;
        this.indexUpdatesValidator = indexUpdatesValidator;
    }

    @Override
    public long commit( TransactionToApply batch, CommitEvent commitEvent,
            TransactionApplicationMode mode ) throws TransactionFailureException
    {
        validateIndexUpdatesBeforeCommit( batch );
        long lastTxId = appendToLog( batch, commitEvent );
        try
        {
            applyToStore( batch, commitEvent, mode );
            return lastTxId;
        }
        finally
        {
            close( batch );
        }
    }

    private void validateIndexUpdatesBeforeCommit( TransactionToApply batch ) throws TransactionFailureException
    {
        if ( batch.transactionId() == TRANSACTION_ID_NOT_SPECIFIED )
        {
            // A normal commit, i.e. new transaction data that is to be committed in db/cluster for the first time
            // For the moment this only supports a single transaction, since:
            //  - we must validate the index updates before each transaction in order to know whether or not
            //    it can be committed. This will change when busting the lucene limits.
            //  - each validation depends upon the fact that all previous transactions have been applied
            //    to the store. This will change when we write index commands to the log directly instead of inferring.
            if ( batch.next() != null )
            {
                throw new UnsupportedOperationException(
                        "For the time being we only support a single previously uncommitted transaction " +
                        "to be committed at a time. Batching is fine when replicating transactions currently. " +
                        "This problem will go away when we bust the id limits" );
            }

            while ( batch != null )
            {
                batch.validatedIndexUpdates( validateIndexUpdates( batch.transactionRepresentation() ) );
                batch = batch.next();
            }
        }
        else
        {
            // We will do it as part of applying transactions. Reason is that the batching of transactions
            // arose from HA environment where slaves applies transactions in batches. In that environment
            // those transactions had already been committed and didn't need to be validated before committing
            // on the slave (i.e. here). The second reason is that validating index updates means translating
            // physical record updates to logical index commands. In that process there's some amount of reading
            // from store and that reading might fail or read stale data if we would validate all transactions
            // in this batch before any of them would have been applied. Weird, huh?
            // This will change when ValidatedIndexUpdates goes away (busting the lucene id limits)
            // so don't worry too much about this.
        }
    }

    private ValidatedIndexUpdates validateIndexUpdates( TransactionRepresentation transactionRepresentation )
            throws TransactionFailureException
    {
        try
        {
            return indexUpdatesValidator.validate( transactionRepresentation );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( ValidationFailed, e, "Validation of index updates failed" );
        }
    }

    private long appendToLog( TransactionToApply batch, CommitEvent commitEvent ) throws TransactionFailureException
    {
        try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
        {
            return appender.append( batch, logAppendEvent );
        }
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( CouldNotWriteToLog, cause,
                    "Could not append transaction representation to log" );
        }
    }

    private void applyToStore( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
            throws TransactionFailureException
    {
        try ( StoreApplyEvent storeApplyEvent = commitEvent.beginStoreApply() )
        {
            storageEngine.apply( batch, mode );
        }
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( CouldNotCommit, cause,
                    "Could not apply the transaction to the store after written to log" );
        }
    }

    private void close( TransactionToApply batch )
    {
        while ( batch != null )
        {
            if ( batch.commitment().markedAsCommitted() )
            {
                batch.commitment().publishAsClosed();
            }
            batch = batch.next();
        }
    }
}
