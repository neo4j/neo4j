/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

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

    public StoreCopyResponsePacker( LogicalTransactionStore transactionStore,
            TransactionIdStore transactionIdStore, LogFileInformation logFileInformation,
            Provider<StoreId> storeId, long mandatoryStartTransactionId )
    {
        super( transactionStore, transactionIdStore, storeId );
        this.transactionIdStore = transactionIdStore;
        this.mandatoryStartTransactionId = mandatoryStartTransactionId;
        this.logFileInformation = logFileInformation;
    }

    @Override
    protected void extractTransactions( long startingAtTransactionId,
            Visitor<CommittedTransactionRepresentation, IOException> accumulator ) throws IOException
    {
        try
        {
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
