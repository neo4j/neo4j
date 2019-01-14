/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
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

    public boolean hasTxLogs( File storeDir ) throws IOException
    {
        return LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache ).withConfig( config ).build().logFiles().length > 0;
    }
}
