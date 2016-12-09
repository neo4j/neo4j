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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class StoreCopyClientTest
{
    private TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private CleanupRule cleanup = new CleanupRule();

    @Rule
    public TestRule rules = RuleChain.outerRule( testDir ).around( pageCacheRule ).around( cleanup );

    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldCopyStoreFilesAcrossIfACancellationRequestHappensAfterTheTempStoreHasBeenRecovered()
            throws Exception
    {
        // given
        final File copyDir = new File( testDir.directory(), "copy" );
        final File originalDir = new File( testDir.directory(), "original" );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        CancellationRequest cancellationRequest = () -> cancelStoreCopy.get();

        StoreCopyClient.Monitor storeCopyMonitor = new StoreCopyClient.Monitor.Adapter()
        {
            @Override
            public void finishRecoveringStore()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        };

        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreCopyClient copier =
                new StoreCopyClient( copyDir, Config.empty(), loadKernelExtensions(), NullLogProvider.getInstance(), fs, pageCache, storeCopyMonitor,
                        false );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) startDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest = storeCopyRequest( originalDir, original );

        // when
        File copyOfStore = copier.copyStore( storeCopyRequest, cancellationRequest );
        new MoveToDir().move( copyOfStore, copyDir );

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
    }

    @Test
    public void storeCopyClientMustWorkWithStandardRecordFormat() throws Exception
    {
        checkStoreCopyClientWithRecordFormats( StandardV3_0_7.NAME );
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
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        Config config = Config.empty().augment( stringMap( record_format.name(), recordFormatsName ) );
        StoreCopyClient copier = new StoreCopyClient(
                copyDir, config, loadKernelExtensions(), NullLogProvider.getInstance(), fs, pageCache,
                new StoreCopyClient.Monitor.Adapter(), false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalDir, recordFormatsName );
        StoreCopyClient.StoreCopyRequester storeCopyRequest = storeCopyRequest( originalDir, original );

        copier.copyStore( storeCopyRequest, CancellationRequest.NEVER_CANCELLED );

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
        CancellationRequest cancellationRequest = () -> cancelStoreCopy.get();

        StoreCopyClient.Monitor storeCopyMonitor = new StoreCopyClient.Monitor.Adapter()
        {
            @Override
            public void finishReceivingStoreFiles()
            {
                // simulate a cancellation request
                cancelStoreCopy.set( true );
            }
        };

        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreCopyClient copier = new StoreCopyClient(
                copyDir, Config.empty(), loadKernelExtensions(), NullLogProvider.getInstance(), fs, pageCache,
                storeCopyMonitor, false );

        final GraphDatabaseAPI original = (GraphDatabaseAPI) startDatabase( originalDir );

        try ( Transaction tx = original.beginTx() )
        {
            original.createNode( label( "BeforeCopyBegins" ) );
            tx.success();
        }

        StoreCopyClient.StoreCopyRequester storeCopyRequest = storeCopyRequest( originalDir, original );

        // when
        copier.copyStore( storeCopyRequest, cancellationRequest );

        // Then
        GraphDatabaseService copy = startDatabase( copyDir );

        try ( Transaction tx = copy.beginTx() )
        {
            long nodesCount = Iterators.count( copy.findNodes( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 0l ) );

            tx.success();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
    }

    @Test
    public void shouldResetNeoStoreLastTransactionOffsetForNonForensicCopy() throws Exception
    {
        // GIVEN
        File initialStore = testDir.directory( "initialStore" );
        File backupStore = testDir.directory( "backupStore" );

        PageCache pageCache = pageCacheRule.getPageCache( fs );
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

        long originalTransactionOffset =
                MetaDataStore.getRecord( pageCache, new File( initialStore, MetaDataStore.DEFAULT_NAME ),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        initialDatabase = startDatabase( initialStore );

        StoreCopyClient copier =
                new StoreCopyClient( backupStore, Config.empty(), loadKernelExtensions(), NullLogProvider
                        .getInstance(), fs, pageCache, new StoreCopyClient.Monitor.Adapter(), false );
        CancellationRequest falseCancellationRequest = () -> false;
        StoreCopyClient.StoreCopyRequester storeCopyRequest = storeCopyRequest( initialStore, (GraphDatabaseAPI)
                initialDatabase );

        // WHEN
        File copyOfStore = copier.copyStore( storeCopyRequest, falseCancellationRequest );
        new MoveToDir().move( copyOfStore, backupStore );

        // THEN
        long updatedTransactionOffset =
                MetaDataStore.getRecord( pageCache, new File( backupStore, MetaDataStore.DEFAULT_NAME ),
                        MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        assertNotEquals( originalTransactionOffset, updatedTransactionOffset );
        assertEquals( LogHeader.LOG_HEADER_SIZE, updatedTransactionOffset );
    }

    private GraphDatabaseService startDatabase( File storeDir )
    {
        return startDatabase( storeDir, StandardV3_0_7.NAME );
    }

    private GraphDatabaseService startDatabase( File storeDir, String recordFormatName )
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, recordFormatName )
                .newGraphDatabase();
        return cleanup.add( database );
    }

    private StoreCopyClient.StoreCopyRequester storeCopyRequest( final File originalDir,
            final GraphDatabaseAPI original )
    {
        return spy( new StoreCopyClient.StoreCopyRequester()
            {
                public Response<?> response;

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

                    RequestContext requestContext = new StoreCopyServer( neoStoreDataSource,
                            checkPointer, fs, originalDir, new Monitors().newMonitor( StoreCopyServer.Monitor.class ) )
                            .flushStoresAndStreamStoreFiles( "test", writer, false );

                    final StoreId storeId = original.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores().getMetaDataStore().getStoreId();

                    ResponsePacker responsePacker = new ResponsePacker( logicalTransactionStore,
                            transactionIdStore, () -> storeId );


                    response = spy(responsePacker.packTransactionStreamResponse( requestContext, null ));
                    return response;

                }

                @Override
                public void done()
                {
                    // Ensure response is closed before this method is called
                    assertNotNull( response );
                    verify( response, times( 1 ) ).close();
                }
            } );
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
}
