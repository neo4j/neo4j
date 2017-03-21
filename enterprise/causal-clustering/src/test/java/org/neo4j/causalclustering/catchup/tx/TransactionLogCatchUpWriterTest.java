/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;

public class TransactionLogCatchUpWriterTest
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory( getClass() );

    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public NeoStoreDataSourceRule dsRule = new NeoStoreDataSourceRule();

    private PageCache pageCache;
    private FileSystemAbstraction fs;
    private File storeDir;

    @Before
    public void setup() throws IOException
    {
        storeDir = dir.directory( "graph.db" );
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void shouldCreateTransactionLogWithCheckpoint() throws Exception
    {
        // given
        org.neo4j.kernel.impl.store.StoreId storeId = simulateStoreCopy();

        int fromTxId = 37;
        int endTxId = fromTxId + 5;

        TransactionLogCatchUpWriter catchUpWriter = new TransactionLogCatchUpWriter( storeDir, fs, pageCache, NullLogProvider.getInstance(), fromTxId, true );

        // when
        for ( int i = fromTxId; i <= endTxId; i++ )
        {
            catchUpWriter.onTxReceived( new TxPullResponse( toCasualStoreId( storeId ), tx( i ) ) );
        }

        catchUpWriter.close();

        // then
        verifyTransactionsInLog( fromTxId, endTxId );
        verifyCheckpointInLog(); // necessary for recovery

    }

    private void verifyCheckpointInLog() throws IOException
    {
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
        final LatestCheckPointFinder checkPointFinder =
                new LatestCheckPointFinder( logFiles, fs, logEntryReader );

        LatestCheckPointFinder.LatestCheckPoint checkPoint = checkPointFinder.find( 0 );
        assertNotNull( checkPoint.checkPoint );
        assertTrue( checkPoint.commitsAfterCheckPoint );
    }

    private void verifyTransactionsInLog( long fromTxId, long endTxId ) throws IOException
    {
        long expectedTxId = fromTxId;
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
        LogVersionedStoreChannel versionedStoreChannel =
                PhysicalLogFile.openForVersion( logFiles, fs, 0, false );
        try ( ReadableLogChannel channel =
                      new ReadAheadLogChannel( versionedStoreChannel, LogVersionBridge.NO_MORE_CHANNELS, 1024 ) )
        {
            try ( PhysicalTransactionCursor<ReadableLogChannel> txCursor =
                          new PhysicalTransactionCursor<>( channel, new VersionAwareLogEntryReader<>() ) )
            {
                while ( txCursor.next() )
                {
                    CommittedTransactionRepresentation tx = txCursor.get();
                    long txId = tx.getCommitEntry().getTxId();

                    assertThat( expectedTxId, lessThanOrEqualTo( endTxId ) );
                    assertEquals( expectedTxId, txId );
                    expectedTxId++;
                }
            }
        }
    }

    private org.neo4j.kernel.impl.store.StoreId simulateStoreCopy() throws IOException
    {
        // create an empty store
        org.neo4j.kernel.impl.store.StoreId storeId;
        NeoStoreDataSource ds = dsRule.getDataSource( storeDir, fs, pageCache, emptyMap() );
        try ( Lifespan ignored = new Lifespan( ds ) )
        {
            storeId = ds.getStoreId();
        }

        // we don't have log files after a store copy
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fsRule.get() );
        logFiles.accept( ( file, version ) -> file.delete() );

        return storeId;
    }

    private StoreId toCasualStoreId( org.neo4j.kernel.impl.store.StoreId storeId )
    {
        return new StoreId( storeId.getCreationTime(), storeId.getRandomId(), storeId.getUpgradeTime(), storeId.getUpgradeId() );
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ), new OnePhaseCommit( id, id ) );
    }
}
