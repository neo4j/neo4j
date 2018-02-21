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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CommitStateHelper
{
    private PageCache pageCache;
    private FileSystemAbstraction fs;
    private Config config;

    public CommitStateHelper( PageCache pageCache, FileSystemAbstraction fs, Config config )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.config = config;
    }

    CommitState getStoreState( File storeDir ) throws IOException
    {
        ReadOnlyTransactionIdStore metaDataStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        long metaDataStoreTxId = metaDataStore.getLastCommittedTransactionId();

        Optional<Long> latestTransactionLogIndex = getLatestTransactionLogIndex( metaDataStoreTxId, storeDir );

        //noinspection OptionalIsPresent
        if ( latestTransactionLogIndex.isPresent() )
        {
            return new CommitState( metaDataStoreTxId, latestTransactionLogIndex.get() );
        }
        else
        {
            return new CommitState( metaDataStoreTxId );
        }
    }

    private Optional<Long> getLatestTransactionLogIndex( long startTxId, File storeDir ) throws IOException
    {
        if ( !hasTxLogs( storeDir ) )
        {
            return Optional.empty();
        }

        // this is not really a read-only store, because it will create an empty transaction log if there is none
        ReadOnlyTransactionStore txStore = new ReadOnlyTransactionStore( pageCache, fs, storeDir, config, new Monitors() );

        long lastTxId = BASE_TX_ID;
        try ( Lifespan ignored = new Lifespan( txStore ); TransactionCursor cursor = txStore.getTransactions( startTxId ) )
        {
            while ( cursor.next() )
            {
                CommittedTransactionRepresentation tx = cursor.get();
                lastTxId = tx.getCommitEntry().getTxId();
            }

            return Optional.of( lastTxId );
        }
        catch ( NoSuchTransactionException e )
        {
            return Optional.empty();
        }
    }

    public boolean hasTxLogs( File storeDir )
    {
        File[] files = fs.listFiles( storeDir, TransactionLogFiles.DEFAULT_FILENAME_FILTER );
        if ( files == null )
        {
            throw new RuntimeException( "Files was null. Incorrect directory or I/O error?" );
        }
        return files.length > 0;
    }
}
