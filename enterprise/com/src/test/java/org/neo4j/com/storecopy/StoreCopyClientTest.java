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
package org.neo4j.com.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.com.storecopy.StoreUtil.TEMP_COPY_DIRECTORY_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class StoreCopyClientTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final CleanupRule cleanup = new CleanupRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public TestRule rules = RuleChain.outerRule( directory ).around( fileSystemRule ).
                                      around( pageCacheRule ).around( cleanup );

    private final StoreCopyRequestFactory requestFactory = LocalStoreCopyRequester::new;
    private FileSystemAbstraction fileSystem;

    @Before
    public void setUp()
    {
        fileSystem = fileSystemRule.get();
    }

    @Test
    public void shouldCopyStoreFilesAcrossIfACancellationRequestHappensAfterTheTempStoreHasBeenRecovered()
            throws Exception
    {
        // given
        final DatabaseLayout copyLayout = directory.databaseLayout("copy" );
        final File originalDir = directory.storeDir( "original" );
        DatabaseLayout originalLayout = directory.databaseLayout( originalDir );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        StoreCopyClientMonitor storeCopyMonitor = new StoreCopyClientMonitor.Adapter()
        {
            @Override
            public void finishRecoveringStore()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        };

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreCopyClient copier =
                new StoreCopyClient( copyLayout, Config.defaults(), loadKernelExtensions(),
                        NullLogProvider.getInstance(), fileSystem, pageCache, storeCopyMonitor, false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalLayout.databaseDirectory() );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                spy( requestFactory.create( original, originalLayout.databaseDirectory(), fileSystem, false ) );

        // when
        copier.copyStore( storeCopyRequest, cancelStoreCopy::get, MoveAfterCopy.moveReplaceExisting() );

        // Then
        GraphDatabaseService copy = startDatabase( copyLayout.databaseDirectory() );

        try ( Transaction tx = copy.beginTx() )
        {
            long nodesCount = Iterators.count( copy.findNodes( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 1L ) );

            assertThat( Iterators.single( copy.findNodes( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0L ) );

            tx.success();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
        assertFalse( copyLayout.file( TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    @Test
    public void storeCopyClientUseCustomTransactionLogLocationWhenConfigured() throws Exception
    {
        final DatabaseLayout copyLayout = directory.databaseLayout("copyCustomLocation" );
        final File originalDir = new File( directory.directory(), "originalCustomLocation" );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        File copyCustomLogFilesLocation = copyLayout.file( "CopyCustomLogFilesLocation" );
        File originalCustomLogFilesLocation = new File( originalDir, "originalCustomLogFilesLocation" );

        Config config = Config.defaults( logical_logs_location, copyCustomLogFilesLocation.getName() );
        config.augment( GraphDatabaseSettings.active_database, copyLayout.getDatabaseName() );
        StoreCopyClient copier = new StoreCopyClient(
                copyLayout, config, loadKernelExtensions(), NullLogProvider.getInstance(), fileSystem, pageCache,
                new StoreCopyClientMonitor.Adapter(), false );

        GraphDatabaseAPI original = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( originalDir )
                .setConfig( logical_logs_location, originalCustomLogFilesLocation.getName() )
                .newGraphDatabase();
        generateTransactions( original );
        long logFileSize =
                original.getDependencyResolver().resolveDependency( LogFiles.class ).getLogFileForVersion( 0 ).length();

        StoreCopyClient.StoreCopyRequester storeCopyRequest = requestFactory.create( original, originalDir, fileSystem, true );

        copier.copyStore( storeCopyRequest, CancellationRequest.NEVER_CANCELLED, MoveAfterCopy.moveReplaceExisting() );
        original.shutdown();

        assertFalse( copyLayout.file( TEMP_COPY_DIRECTORY_NAME ).exists() );

        LogFiles customLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( copyCustomLogFilesLocation, fileSystem ).build();
        assertTrue( customLogFiles.versionExists( 0 ) );
        assertThat( customLogFiles.getLogFileForVersion( 0 ).length(), greaterThanOrEqualTo( logFileSize ) );

        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( copyLayout.databaseDirectory(), fileSystem ).build();
        assertFalse( logFiles.versionExists( 0 ) );

        new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( copyLayout.databaseDirectory() )
                .setConfig( logical_logs_location, copyCustomLogFilesLocation.getName() )
                .newGraphDatabase().shutdown();
    }

    @Test
    public void storeCopyClientMustWorkWithStandardRecordFormat() throws Exception
    {
        checkStoreCopyClientWithRecordFormats( Standard.LATEST_NAME );
    }

    @Test
    public void storeCopyClientMustWorkWithHighLimitRecordFormat() throws Exception
    {
        checkStoreCopyClientWithRecordFormats( HighLimit.NAME );
    }

    @Test
    public void shouldEndUpWithAnEmptyStoreIfCancellationRequestIssuedJustBeforeRecoveryTakesPlace()
            throws Exception
    {
        // given
        final DatabaseLayout copyLayout = directory.databaseLayout("copy" );
        final File originalDir = directory.storeDir( "original" );
        DatabaseLayout originalLayout = directory.databaseLayout( originalDir );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        StoreCopyClientMonitor storeCopyMonitor = new StoreCopyClientMonitor.Adapter()
        {
            @Override
            public void finishReceivingStoreFiles()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        };

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreCopyClient copier = new StoreCopyClient( copyLayout, Config.defaults(), loadKernelExtensions(),
                NullLogProvider.getInstance(), fileSystem, pageCache, storeCopyMonitor, false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalLayout.databaseDirectory() );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                spy( requestFactory.create( original, originalLayout.databaseDirectory(), fileSystem, false ) );

        // when
        copier.copyStore( storeCopyRequest, cancelStoreCopy::get, MoveAfterCopy.moveReplaceExisting() );

        // Then
        GraphDatabaseService copy = startDatabase( copyLayout.databaseDirectory() );

        try ( Transaction tx = copy.beginTx() )
        {
            long nodesCount = Iterators.count( copy.findNodes( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 0L ) );

            tx.success();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
        assertFalse( copyLayout.file( TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    @Test
    public void shouldResetNeoStoreLastTransactionOffsetForNonForensicCopy() throws Exception
    {
        // GIVEN
        DatabaseLayout initialLayout = directory.databaseLayout( "initialStore" );
        File backupStore = directory.directory( "backupStore" );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        createInitialDatabase( initialLayout.databaseDirectory() );

        long originalTransactionOffset = MetaDataStore.getRecord( pageCache, initialLayout.metadataStore(),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        GraphDatabaseService initialDatabase = startDatabase( initialLayout.databaseDirectory() );

        DatabaseLayout backupLayout = directory.databaseLayout( backupStore );
        StoreCopyClient copier =
                new StoreCopyClient( backupLayout, Config.defaults(), loadKernelExtensions(), NullLogProvider
                        .getInstance(), fileSystem, pageCache, new StoreCopyClientMonitor.Adapter(), false );
        CancellationRequest falseCancellationRequest = () -> false;
        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                requestFactory.create( (GraphDatabaseAPI) initialDatabase, initialLayout.databaseDirectory(), fileSystem, false );

        // WHEN
        copier.copyStore( storeCopyRequest, falseCancellationRequest, MoveAfterCopy.moveReplaceExisting() );

        // THEN
        long updatedTransactionOffset =
                MetaDataStore.getRecord( pageCache, backupLayout.metadataStore(),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        assertNotEquals( originalTransactionOffset, updatedTransactionOffset );
        assertEquals( LogHeader.LOG_HEADER_SIZE, updatedTransactionOffset );
        assertFalse( new File( backupStore, TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    @Test
    public void shouldDeleteTempCopyFolderOnFailures() throws Exception
    {
        // GIVEN
        File initialStore = directory.directory( "initialStore" );
        DatabaseLayout backupLayout = directory.databaseLayout( "backupStore" );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        GraphDatabaseService initialDatabase = createInitialDatabase( initialStore );
        StoreCopyClient copier =
                new StoreCopyClient( backupLayout, Config.defaults(), loadKernelExtensions(), NullLogProvider
                        .getInstance(), fileSystem, pageCache, new StoreCopyClientMonitor.Adapter(), false );
        CancellationRequest falseCancellationRequest = () -> false;

        RuntimeException exception = new RuntimeException( "Boom!" );
        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                new LocalStoreCopyRequester( (GraphDatabaseAPI) initialDatabase, initialStore, fileSystem, false )
                {
                    @Override
                    public void done()
                    {
                        throw exception;
                    }
                };

        // WHEN
        try
        {
            copier.copyStore( storeCopyRequest, falseCancellationRequest, MoveAfterCopy.moveReplaceExisting() );
            fail( "should have thrown " );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( exception, ex );
        }

        // THEN
        assertFalse( backupLayout.file( TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    private void checkStoreCopyClientWithRecordFormats( String recordFormatsName ) throws Exception
    {
        final DatabaseLayout copyLayout = directory.databaseLayout("copy" );
        final File originalDir = directory.storeDir( "original" );
        DatabaseLayout originalLayout = directory.databaseLayout( originalDir );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.defaults( record_format, recordFormatsName );
        StoreCopyClient copier = new StoreCopyClient( copyLayout,
                config, loadKernelExtensions(), NullLogProvider.getInstance(), fileSystem, pageCache,
                new StoreCopyClientMonitor.Adapter(), false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalLayout.databaseDirectory(), recordFormatsName );
        StoreCopyClient.StoreCopyRequester storeCopyRequest = requestFactory.create( original, originalLayout.databaseDirectory(), fileSystem, false );

        copier.copyStore( storeCopyRequest, CancellationRequest.NEVER_CANCELLED, MoveAfterCopy.moveReplaceExisting() );

        assertFalse( copyLayout.file( TEMP_COPY_DIRECTORY_NAME ).exists() );

        // Must not throw
        startDatabase( copyLayout.databaseDirectory(), recordFormatsName ).shutdown();
    }

    private GraphDatabaseService createInitialDatabase( File initialStore )
    {
        GraphDatabaseService initialDatabase = startDatabase( initialStore );
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = initialDatabase.beginTx() )
            {
                initialDatabase.createNode( label( "Neo" + i ) );
                tx.success();
            }
        }
        initialDatabase.shutdown();
        return initialDatabase;
    }

    private GraphDatabaseService startDatabase( File storeDir )
    {
        return startDatabase( storeDir, Standard.LATEST_NAME );
    }

    private GraphDatabaseService startDatabase( File storeDir, String recordFormatName )
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, recordFormatName )
                .newGraphDatabase();
        return cleanup.add( database );
    }

    private static List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }

    private static void generateTransactions( GraphDatabaseAPI original )
    {
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = original.beginTx() )
            {
                original.createNode();
                transaction.success();
            }
        }
    }

    private interface StoreCopyRequestFactory
    {
        StoreCopyClient.StoreCopyRequester create( GraphDatabaseAPI original, File originalDir, FileSystemAbstraction fs,
                boolean includeLogs );
    }

    private static class LocalStoreCopyRequester implements StoreCopyClient.StoreCopyRequester
    {
        private final GraphDatabaseAPI original;
        private final File originalDir;
        private final FileSystemAbstraction fs;

        private Response<?> response;
        private boolean includeLogs;

        LocalStoreCopyRequester( GraphDatabaseAPI original, File originalDir, FileSystemAbstraction fs,
                boolean includeLogs )
        {
            this.original = original;
            this.originalDir = originalDir;
            this.fs = fs;
            this.includeLogs = includeLogs;
        }

        protected PageCache getPageCache()
        {
            return original.getDependencyResolver().resolveDependency( PageCache.class );
        }

        @Override
        public Response<?> copyStore( StoreWriter writer )
        {
            NeoStoreDataSource neoStoreDataSource =
                    original.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );

            TransactionIdStore transactionIdStore =
                    original.getDependencyResolver().resolveDependency( TransactionIdStore.class );

            LogicalTransactionStore logicalTransactionStore  =
                    original.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );

            CheckPointer checkPointer =
                    original.getDependencyResolver().resolveDependency( CheckPointer.class );

            RequestContext requestContext = new StoreCopyServer( neoStoreDataSource, checkPointer, fs, originalDir,
                    new Monitors().newMonitor( StoreCopyServer.Monitor.class ) ).flushStoresAndStreamStoreFiles( "test", writer, includeLogs );

            final StoreId storeId =
                    original.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).getStoreId();

            ResponsePacker responsePacker = new ResponsePacker( logicalTransactionStore,
                    transactionIdStore, () -> storeId );

            response = spy( responsePacker.packTransactionStreamResponse( requestContext, null ) );
            return response;
        }

        @Override
        public void done()
        {
            // Ensure response is closed before this method is called
            assertNotNull( response );
            verify( response, times( 1 ) ).close();
        }
    }
}
