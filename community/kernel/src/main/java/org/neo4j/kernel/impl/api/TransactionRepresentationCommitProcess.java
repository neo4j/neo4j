/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionLogError;

public class TransactionRepresentationCommitProcess implements TransactionCommitProcess
{
    private final TransactionAppender appender;
    private final StorageEngine storageEngine;

    public TransactionRepresentationCommitProcess( TransactionAppender appender, StorageEngine storageEngine )
    {
        this.appender = appender;
        this.storageEngine = storageEngine;
    }

    @Override
    public long commit( TransactionToApply batch, CommitEvent commitEvent,
            TransactionApplicationMode mode ) throws TransactionFailureException
    {
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

    private long appendToLog( TransactionToApply batch, CommitEvent commitEvent ) throws TransactionFailureException
    {
        try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
        {
            return appender.append( batch, logAppendEvent );
        }
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( TransactionLogError, cause,
                    "Could not append transaction representation to log" );
        }
    }

    protected void applyToStore( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
            throws TransactionFailureException
    {
        try ( StoreApplyEvent storeApplyEvent = commitEvent.beginStoreApply() )
        {
            storageEngine.apply( batch, mode );
        }
        catch ( Throwable cause )
        {
            throw new TransactionFailureException( TransactionCommitFailed, cause,
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
            batch.close();
            batch = batch.next();
        }
    }
}
