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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

public class PhysicalLogFileInformation implements LogFileInformation
{
    public interface LogVersionToTimestamp
    {
        long getTimestampForVersion( long version ) throws IOException;
    }

    private final PhysicalLogFiles logFiles;
    private final TransactionMetadataCache transactionMetadataCache;
    private final TransactionIdStore transactionIdStore;
    private final LogVersionToTimestamp logVersionToTimestamp;

    public PhysicalLogFileInformation( PhysicalLogFiles logFiles,
                                       TransactionMetadataCache transactionMetadataCache,
                                       TransactionIdStore transactionIdStore,
                                       LogVersionToTimestamp logVersionToTimestamp )
    {
        this.logFiles = logFiles;
        this.transactionMetadataCache = transactionMetadataCache;
        this.transactionIdStore = transactionIdStore;
        this.logVersionToTimestamp = logVersionToTimestamp;
    }

    @Override
    public long getFirstExistingTxId() throws IOException
    {
        long version = logFiles.getHighestLogVersion();
        long candidateFirstTx = -1;
        while ( logFiles.versionExists( version ) )
        {
            candidateFirstTx = getFirstCommittedTxId( version );
            version--;
        }
        version++; // the loop above goes back one version too far.

        // OK, so we now have the oldest existing log version here. Open it and see if there's any transaction
        // in there. If there is then that transaction is the first one that we have.
        return logFiles.hasAnyTransaction( version ) ? candidateFirstTx : -1;
    }

    @Override
    public long getFirstCommittedTxId( long version ) throws IOException
    {
        long logHeader = transactionMetadataCache.getLogHeader( version );
        if ( logHeader != -1 )
        {   // It existed in cache
            return logHeader + 1;
        }

        // Wasn't cached, go look for it
        if ( logFiles.versionExists( version ) )
        {
            long previousVersionLastCommittedTx = logFiles.extractHeader( version ).lastCommittedTxId;
            transactionMetadataCache.putHeader( version, previousVersionLastCommittedTx );
            return previousVersionLastCommittedTx + 1;
        }
        return -1;
    }

    @Override
    public long getLastCommittedTxId()
    {
        return transactionIdStore.getLastCommittedTransactionId();
    }

    @Override
    public long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        return logVersionToTimestamp.getTimestampForVersion( version );
    }
}
