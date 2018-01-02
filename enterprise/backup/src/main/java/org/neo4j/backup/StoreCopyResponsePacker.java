/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreCopyServer.Monitor;
import org.neo4j.function.Supplier;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * In full backup we're more tolerant about missing transactions. Just as long as we fulfill this criterion:
 * We must be able to find and stream transactions that has happened since the start of the full backup.
 * Full backup will stream at most N transactions back, even if nothing happened during the backup.
 * Streaming these transaction aren't as important, since they are mostly a nice-to-have.
 */
public class StoreCopyResponsePacker extends ResponsePacker
{
    private final long mandatoryStartTransactionId;
    private final LogFileInformation logFileInformation;
    private final TransactionIdStore transactionIdStore;
    private final Monitor monitor;

    public StoreCopyResponsePacker( LogicalTransactionStore transactionStore,
            TransactionIdStore transactionIdStore, LogFileInformation logFileInformation,
            Supplier<StoreId> storeId, long mandatoryStartTransactionId, StoreCopyServer.Monitor monitor )
    {
        super( transactionStore, transactionIdStore, storeId );
        this.transactionIdStore = transactionIdStore;
        this.mandatoryStartTransactionId = mandatoryStartTransactionId;
        this.logFileInformation = logFileInformation;
        this.monitor = monitor;
    }

    @Override
    public <T> Response<T> packTransactionStreamResponse( RequestContext context, T response )
    {
        final long toStartFrom = mandatoryStartTransactionId;
        final long toEndAt = transactionIdStore.getLastCommittedTransactionId();
        TransactionStream transactions = new TransactionStream()
        {
            @Override
            public void accept( Visitor<CommittedTransactionRepresentation,IOException> visitor ) throws IOException
            {
                // Check so that it's even worth thinking about extracting any transactions at all
                if ( toStartFrom > BASE_TX_ID && toStartFrom <= toEndAt )
                {
                    monitor.startStreamingTransactions( toStartFrom );
                    extractTransactions( toStartFrom, filterVisitor( visitor, toEndAt ) );
                    monitor.finishStreamingTransactions( toEndAt );
                }
            }
        };
        return new TransactionStreamResponse<>( response, storeId.get(), transactions, ResourceReleaser.NO_OP );
    }

    @Override
    protected void extractTransactions( long startingAtTransactionId,
            Visitor<CommittedTransactionRepresentation, IOException> accumulator ) throws IOException
    {
        try
        {
            startingAtTransactionId = Math.min( mandatoryStartTransactionId, startingAtTransactionId );
            super.extractTransactions( startingAtTransactionId, accumulator );
        }
        catch ( NoSuchTransactionException e )
        {
            // We no longer have transactions that far back. Which transaction is the farthest back?
            if ( startingAtTransactionId < mandatoryStartTransactionId )
            {
                // We don't necessarily need to ask that far back. Ask which is the oldest transaction in the log(s)
                // that we can possibly serve
                long oldestExistingTransactionId = logFileInformation.getFirstExistingTxId();
                if ( oldestExistingTransactionId == -1 )
                {
                    // Seriously, there are no logs that we can serve?
                    if ( mandatoryStartTransactionId >= transactionIdStore.getLastCommittedTransactionId() )
                    {
                        // Although there are no mandatory transactions to stream, so we're good here.
                        return;
                    }

                    // We are required to serve one or more transactions, but there are none, tell that
                    throw e;
                }

                if ( oldestExistingTransactionId <= mandatoryStartTransactionId )
                {
                    super.extractTransactions( oldestExistingTransactionId, accumulator );
                }

                // We can't serve the mandatory transactions, tell that
                throw e;
            }
        }
    }
}
