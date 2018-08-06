/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.tx;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;

@RunWith( Parameterized.class )
public class TransactionLogCatchUpWriterTest
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public NeoStoreDataSourceRule dsRule = new NeoStoreDataSourceRule();

    @Parameterized.Parameter
    public boolean partOfStoreCopy;

    private PageCache pageCache;
    private FileSystemAbstraction fs;
    private DatabaseLayout databaseLayout;

    @Parameterized.Parameters
    public static List<Boolean> partOfStoreCopy()
    {
        return Arrays.asList( Boolean.TRUE, Boolean.FALSE );
    }

    @Before
    public void setup()
    {
        databaseLayout = dir.databaseLayout();
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void shouldCreateTransactionLogWithCheckpoint() throws Exception
    {
        createTransactionLogWithCheckpoint( Config.defaults(), true );
    }

    @Test
    public void createTransactionLogWithCheckpointInCustomLocation() throws IOException
    {
        createTransactionLogWithCheckpoint( Config.defaults( GraphDatabaseSettings.logical_logs_location,
                "custom-tx-logs"), false );
    }

    @Test
    public void pullRotatesWhenThresholdCrossedAndExplicitlySet() throws IOException
    {
        // given
        Config config = Config.defaults();
        config.augment( GraphDatabaseSettings.logical_log_rotation_threshold, "1m" );

        // and
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        // and
        long fromTx = 0;
        TransactionLogCatchUpWriter subject =
                new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(), fromTx, partOfStoreCopy, false, true );

        // when 1M tx received
        IntStream.range( 0, (int) ByteUnit.mebiBytes( 1 ) )
                .mapToObj( TransactionLogCatchUpWriterTest::tx )
                .map(tx -> new TxPullResponse( toCasualStoreId( storeId ), tx ))
                .forEach( subject::onTxReceived );
        subject.close();

        // then there was a rotation
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        LogFiles logFiles = logFilesBuilder.build();
        assertNotEquals( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() );
    }

    @Test
    public void pullDoesntRotateWhenThresholdCrossedAndExplicitlyOff() throws IOException
    {
        // given
        Config config = Config.defaults();
        config.augment( GraphDatabaseSettings.logical_log_rotation_threshold, "1m" );

        // and
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        // and
        long fromTx = 0;
        TransactionLogCatchUpWriter subject =
                new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(), fromTx, partOfStoreCopy, false, false );

        // when 1M tx received
        IntStream.range( 0, (int) ByteUnit.mebiBytes( 1 ) )
                .mapToObj( TransactionLogCatchUpWriterTest::tx )
                .map(tx -> new TxPullResponse( toCasualStoreId( storeId ), tx ))
                .forEach( subject::onTxReceived );
        subject.close();

        // then there was a rotation
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        LogFiles logFiles = logFilesBuilder.build();
        assertEquals( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() );
    }

    private void createTransactionLogWithCheckpoint( Config config, boolean logsInStoreDir ) throws IOException
    {
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        int fromTxId = 37;
        int endTxId = fromTxId + 5;

        TransactionLogCatchUpWriter catchUpWriter = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config,
                NullLogProvider.getInstance(), fromTxId, partOfStoreCopy, logsInStoreDir, true );

        // when
        for ( int i = fromTxId; i <= endTxId; i++ )
        {
            catchUpWriter.onTxReceived( new TxPullResponse( toCasualStoreId( storeId ), tx( i ) ) );
        }

        catchUpWriter.close();

        // then
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        if ( !logsInStoreDir )
        {
            logFilesBuilder.withConfig( config );
        }
        LogFiles logFiles = logFilesBuilder.build();

        verifyTransactionsInLog( logFiles, fromTxId, endTxId );
        if ( partOfStoreCopy )
        {
            verifyCheckpointInLog( logFiles );
        }
    }

    private void verifyCheckpointInLog( LogFiles logFiles )
    {
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );
        final LogTailScanner logTailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );

        LogTailInformation tailInformation = logTailScanner.getTailInformation();
        assertNotNull( tailInformation.lastCheckPoint );
        assertTrue( tailInformation.commitsAfterLastCheckpoint() );
    }

    private void verifyTransactionsInLog( LogFiles logFiles, long fromTxId, long endTxId ) throws
            IOException
    {
        long expectedTxId = fromTxId;
        LogVersionedStoreChannel versionedStoreChannel = logFiles.openForVersion( 0 );
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

    private org.neo4j.storageengine.api.StoreId simulateStoreCopy() throws IOException
    {
        // create an empty store
        org.neo4j.storageengine.api.StoreId storeId;
        NeoStoreDataSource ds = dsRule.getDataSource( databaseLayout, fs, pageCache );
        try ( Lifespan ignored = new Lifespan( ds ) )
        {
            storeId = ds.getStoreId();
        }

        // we don't have log files after a store copy
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fsRule.get() ).build();
        logFiles.accept( ( file, version ) -> file.delete() );

        return storeId;
    }

    private StoreId toCasualStoreId( org.neo4j.storageengine.api.StoreId storeId )
    {
        return new StoreId( storeId.getCreationTime(), storeId.getRandomId(), storeId.getUpgradeTime(), storeId.getUpgradeId() );
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ), new LogEntryCommit( id, id ) );
    }
}
