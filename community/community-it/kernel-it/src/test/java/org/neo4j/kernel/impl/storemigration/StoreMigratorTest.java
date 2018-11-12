/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.LayoutConfig;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.impl.store.MetaDataStore.setRecord;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class StoreMigratorTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp()
    {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void shouldExtractTransactionInformationFromMetaDataStore() throws Exception
    {
        // given
        // ... variables
        long txId = 42;
        long checksum = 123456789123456789L;
        long timestamp = 919191919191919191L;
        TransactionId expected = new TransactionId( txId, checksum, timestamp );

        // ... and files
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File neoStore = databaseLayout.metadataStore();
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );

        // when
        // ... data in record
        setRecord( pageCache, neoStore, LAST_TRANSACTION_ID, txId );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM, checksum );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP, timestamp );

        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, config, logService, jobScheduler );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, txId );

        // then
        assertEquals( expected, actual );
    }

    @Test
    void shouldGenerateTransactionInformationWhenLogsNotPresent() throws Exception
    {
        // given
        long txId = 42;
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File neoStore = databaseLayout.metadataStore();
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, config, logService, jobScheduler );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
    }

    @Test
    void extractTransactionInformationFromLogsInCustomAbsoluteLocation() throws Exception
    {
        File customLogLocation = testDirectory.directory( "customLogLocation" );
        extractTransactionalInformationFromLogs( customLogLocation.getAbsolutePath() );
    }

    @Test
    void shouldGenerateTransactionInformationWhenLogsAreEmpty() throws Exception
    {
        // given
        long txId = 1;
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File neoStore = databaseLayout.metadataStore();
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, config, logService, jobScheduler );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.BASE_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
    }

    @Test
    void writeAndReadLastTxInformation() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        TransactionId writtenTxId = new TransactionId( random.nextLong(), random.nextLong(), random.nextLong() );

        migrator.writeLastTxInformation( testDirectory.databaseLayout(), writtenTxId );

        TransactionId readTxId = migrator.readLastTxInformation( testDirectory.databaseLayout() );

        assertEquals( writtenTxId, readTxId );
    }

    @Test
    void writeAndReadLastTxLogPosition() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        LogPosition writtenLogPosition = new LogPosition( random.nextLong(), random.nextLong() );

        migrator.writeLastTxLogPosition( testDirectory.databaseLayout(), writtenLogPosition );

        LogPosition readLogPosition = migrator.readLastTxLogPosition( testDirectory.databaseLayout() );

        assertEquals( writtenLogPosition, readLogPosition );
    }

    @Test
    void shouldNotMigrateFilesForVersionsWithSameCapability() throws Exception
    {
        // Prepare migrator and file
        StoreMigrator migrator = newStoreMigrator();
        DatabaseLayout dbLayout = testDirectory.databaseLayout();
        File neoStore = dbLayout.metadataStore();
        neoStore.createNewFile();

        // Monitor what happens
        MyProgressReporter progressReporter = new MyProgressReporter();
        // Migrate with two storeversions that have the same FORMAT capabilities
        migrator.migrate( dbLayout, testDirectory.databaseLayout( "migrationDir" ), progressReporter,
                StandardV3_0.STORE_VERSION, StandardV3_2.STORE_VERSION );

        // Should not have started any migration
        assertFalse( progressReporter.started );
    }

    private void extractTransactionalInformationFromLogs( String customLogsLocation ) throws IOException
    {
        Config config = Config.builder().withSetting( transaction_logs_root_path, customLogsLocation ).build();
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        DatabaseLayout databaseLayout = testDirectory.databaseLayout( LayoutConfig.of( config ) );
        File neoStore = databaseLayout.metadataStore();

        GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( databaseLayout.databaseDirectory() )
                .setConfig( transaction_logs_root_path, customLogsLocation ).newGraphDatabase();
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode();
                transaction.success();
            }
        }
        database.shutdown();

        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION,
                MetaDataRecordFormat.FIELD_NOT_PRESENT );
        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, config, logService, jobScheduler );
        LogPosition logPosition = migrator.extractTransactionLogPosition( neoStore, databaseLayout, 100 );

        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystem, pageCache ).withConfig( config ).build();
        assertEquals( 0, logPosition.getLogVersion() );
        assertEquals( logFiles.getHighestLogFile().length(), logPosition.getByteOffset() );
    }

    private StoreMigrator newStoreMigrator()
    {
        return new StoreMigrator( fileSystem, pageCache, Config.defaults(), NullLogService.getInstance(), jobScheduler );
    }

    private static class MyProgressReporter implements ProgressReporter
    {
        public boolean started;

        @Override
        public void start( long max )
        {
            started = true;
        }

        @Override
        public void progress( long add )
        {

        }

        @Override
        public void completed()
        {

        }
    }
}
