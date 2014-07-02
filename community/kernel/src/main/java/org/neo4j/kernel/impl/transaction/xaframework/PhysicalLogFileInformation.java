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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;

public class PhysicalLogFileInformation implements LogFileInformation
{
    public interface SPI
    {
        long getTimestampForVersion( long version ) throws IOException;
    }

    private final PhysicalLogFiles logFiles;
    private final TransactionMetadataCache transactionMetadataCache;
    private final TransactionIdStore transactionIdStore;
    private final SPI spi;

    public PhysicalLogFileInformation( PhysicalLogFiles logFiles,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            SPI spi )
    {
        this.logFiles = logFiles;
        this.transactionMetadataCache = transactionMetadataCache;
        this.transactionIdStore = transactionIdStore;
        this.spi = spi;
    }

    @Override
    public long getFirstCommittedTxId() throws IOException
    {
        long version = logFiles.getHighestLogVersion();
        long candidateFirstTx = -1;
        while ( logFiles.versionExists( version ) )
        {
            candidateFirstTx = getOrExtractFirstCommittedTx( version );
            version--;
        }
        version++; // the loop above goes back one version too far.

        if ( candidateFirstTx == -1 )
        {
            return -1;
        }

        // OK, so we now have the oldest existing log version here. Open it and see if there's any transaction
        // in there. If there is then that transaction is the first one that we have.
        return logFiles.hasAnyTransaction( version ) ? candidateFirstTx : -1;
    }

    @Override
    public long getFirstCommittedTxId( long version ) throws IOException
    {
        if ( version == 0 )
        {
            return 1L;
        }

        // First committed tx for version V = last committed tx version V-1 + 1
        return getOrExtractFirstCommittedTx( version );
    }

    private long getOrExtractFirstCommittedTx( long version ) throws IOException
    {
        Long header = transactionMetadataCache.getHeader( version );
        if ( header != null )
        {   // It existed in cache
            return header+1;
        }

        // Wasn't cached, go look for it
        if ( logFiles.versionExists( version ) )
        {
            long[] headerLongs = logFiles.extractHeader( version );
            long previousVersionLastCommittedTx = headerLongs[1];
            transactionMetadataCache.putHeader( version, previousVersionLastCommittedTx );
            return previousVersionLastCommittedTx+1;
        }
        return -1;
    }

    @Override
    public long getLastCommittedTxId()
    {
        return transactionIdStore.getLastCommittingTransactionId();
    }

    @Override
    public long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        return spi.getTimestampForVersion( version );
    }
}
