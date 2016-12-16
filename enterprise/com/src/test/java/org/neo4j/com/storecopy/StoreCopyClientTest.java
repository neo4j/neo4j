/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.com.storecopy.StoreUtil.TEMP_COPY_DIRECTORY_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class StoreCopyClientTest
{
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final CleanupRule cleanup = new CleanupRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public TestRule rules = RuleChain.outerRule( testDir ).around( fileSystemRule ).
                                      around( pageCacheRule ).around( cleanup );

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
        final File copyDir = new File( testDir.directory(), "copy" );
        final File originalDir = new File( testDir.directory(), "original" );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        StoreCopyClient.Monitor storeCopyMonitor = new StoreCopyClient.Monitor.Adapter()
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
                new StoreCopyClient( copyDir, Config.empty(), loadKernelExtensions(), NullLogProvider.getInstance(),
                        fileSystem,
                        pageCache, storeCopyMonitor, false );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) startDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                spy( new LocalStoreCopyRequester( original, originalDir, fileSystem ) );

        // when
        copier.copyStore( storeCopyRequest, cancelStoreCopy::get, MoveAfterCopy.moveReplaceExisting() );

        // Then
        GraphDatabaseService copy = startDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            long nodesCount = Iterators.count( copy.findNodes( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 1L ) );

            assertThat( Iterators.single( copy.findNodes( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0L ) );

            tx.success();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
        assertFalse( new File( copyDir, TEMP_COPY_DIRECTORY_NAME ).exists() );
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

    private void checkStoreCopyClientWithRecordFormats( String recordFormatsName ) throws Exception
    {
        final File copyDir = new File( testDir.directory(), "copy" );
        final File originalDir = new File( testDir.directory(), "original" );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.empty().augment( stringMap( record_format.name(), recordFormatsName ) );
        StoreCopyClient copier = new StoreCopyClient(
                copyDir, config, loadKernelExtensions(), NullLogProvider.getInstance(), fileSystem, pageCache,
                new StoreCopyClient.Monitor.Adapter(), false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalDir, recordFormatsName );
        StoreCopyClient.StoreCopyRequester storeCopyRequest = new LocalStoreCopyRequester( original, originalDir,
                fileSystem );

        copier.copyStore( storeCopyRequest, CancellationRequest.NEVER_CANCELLED, MoveAfterCopy.moveReplaceExisting() );

        assertFalse( new File( copyDir, TEMP_COPY_DIRECTORY_NAME ).exists() );

        // Must not throw
        startDatabase( copyDir, recordFormatsName ).shutdown();
    }

    @Test
    public void shouldEndUpWithAnEmptyStoreIfCancellationRequestIssuedJustBeforeRecoveryTakesPlace()
            throws Exception
    {
        // given
        final File copyDir = new File( testDir.directory(), "copy" );
        final File originalDir = new File( testDir.directory(), "original" );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        StoreCopyClient.Monitor storeCopyMonitor = new StoreCopyClient.Monitor.Adapter()
        {
            @Override
            public void finishReceivingStoreFiles()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        };

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreCopyClient copier = new StoreCopyClient(
                copyDir, Config.empty(), loadKernelExtensions(), NullLogProvider.getInstance(), fileSystem, pageCache,
                storeCopyMonitor, false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                spy( new LocalStoreCopyRequester( original, originalDir, fileSystem ) );

        // when
        copier.copyStore( storeCopyRequest, cancelStoreCopy::get, MoveAfterCopy.moveReplaceExisting() );

        // Then
        GraphDatabaseService copy = startDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            long nodesCount = Iterators.count( copy.findNodes( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 0L ) );

            tx.success();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
        assertFalse( new File( copyDir, TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    @Test
    public void shouldResetNeoStoreLastTransactionOffsetForNonForensicCopy() throws Exception
    {
        // GIVEN
        File initialStore = testDir.directory( "initialStore" );
        File backupStore = testDir.directory( "backupStore" );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        createInitialDatabase( initialStore );

        long originalTransactionOffset =
                MetaDataStore.getRecord( pageCache, new File( initialStore, MetaDataStore.DEFAULT_NAME ),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        GraphDatabaseService initialDatabase = startDatabase( initialStore );

        StoreCopyClient copier =
                new StoreCopyClient( backupStore, Config.empty(), loadKernelExtensions(), NullLogProvider
                        .getInstance(), fileSystem, pageCache, new StoreCopyClient.Monitor.Adapter(), false );
        CancellationRequest falseCancellationRequest = () -> false;
        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                new LocalStoreCopyRequester( (GraphDatabaseAPI) initialDatabase, initialStore, fileSystem );

        // WHEN
        copier.copyStore( storeCopyRequest, falseCancellationRequest, MoveAfterCopy.moveReplaceExisting() );

        // THEN
        long updatedTransactionOffset =
                MetaDataStore.getRecord( pageCache, new File( backupStore, MetaDataStore.DEFAULT_NAME ),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        assertNotEquals( originalTransactionOffset, updatedTransactionOffset );
        assertEquals( LogHeader.LOG_HEADER_SIZE, updatedTransactionOffset );
        assertFalse( new File( backupStore, TEMP_COPY_DIRECTORY_NAME ).exists() );
    }

    @Test
    public void shouldDeleteTempCopyFolderOnFailures() throws Exception
    {
        // GIVEN
        File initialStore = testDir.directory( "initialStore" );
        File backupStore = testDir.directory( "backupStore" );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        GraphDatabaseService initialDatabase = createInitialDatabase( initialStore );
        StoreCopyClient copier =
                new StoreCopyClient( backupStore, Config.empty(), loadKernelExtensions(), NullLogProvider
                        .getInstance(), fileSystem, pageCache, new StoreCopyClient.Monitor.Adapter(), false );
        CancellationRequest falseCancellationRequest = () -> false;

        RuntimeException exception = new RuntimeException( "Boom!" );
        StoreCopyClient.StoreCopyRequester storeCopyRequest =
                new LocalStoreCopyRequester( (GraphDatabaseAPI) initialDatabase, initialStore, fileSystem )
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
        assertFalse( new File( backupStore, TEMP_COPY_DIRECTORY_NAME ).exists() );
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

    private static class LocalStoreCopyRequester implements StoreCopyClient.StoreCopyRequester
    {
        private final GraphDatabaseAPI original;
        private final File originalDir;
        private final FileSystemAbstraction fs;

        private Response<?> response;

        LocalStoreCopyRequester( GraphDatabaseAPI original, File originalDir, FileSystemAbstraction fs )
        {
            this.original = original;
            this.originalDir = originalDir;
            this.fs = fs;
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

            PageCache pageCache =
                    original.getDependencyResolver().resolveDependency( PageCache.class );

            RequestContext requestContext = new StoreCopyServer( neoStoreDataSource, checkPointer, fs,
                    originalDir, new Monitors().newMonitor( StoreCopyServer.Monitor.class ), pageCache )
                    .flushStoresAndStreamStoreFiles( "test", writer, false );

            final StoreId storeId =
                    original.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                            .testAccessNeoStores().getMetaDataStore().getStoreId();

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
