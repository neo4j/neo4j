/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;

public class StoreCopyClientTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldCopyStoreFilesAcrossIfACancellationRequestHappensAfterTheTempStoreHasBeenRecovered()
            throws IOException
    {
        // given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();

        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null, null );
        ConsoleLogger console = new ConsoleLogger( StringLogger.SYSTEM );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        CancellationRequest cancellationRequest = new CancellationRequest()
        {
            @Override
            public boolean cancellationRequested()
            {
                return cancelStoreCopy.get();
            }
        };

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
                new StoreCopyClient( config, loadKernelExtensions(), console, logging, fs, pageCache, storeCopyMonitor );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) startDatabase( originalDir );

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
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );

            long nodesCount = Iterables.count( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 1l ) );

            assertThat( Iterables.single( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0l ) );

            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
    }

    private GraphDatabaseService startDatabase( String storeDir )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
    }

    @Test
    public void shouldEndUpWithAnEmptyStoreIfCancellationRequestIssuedJustBeforeRecoveryTakesPlace()
            throws IOException
    {
        // given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();

        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        Logging logging = LogbackWeakDependency.tryLoadLogbackService( config, null, null );
        ConsoleLogger console = new ConsoleLogger( StringLogger.SYSTEM );

        final AtomicBoolean cancelStoreCopy = new AtomicBoolean( false );
        CancellationRequest cancellationRequest = new CancellationRequest()
        {
            @Override
            public boolean cancellationRequested()
            {
                return cancelStoreCopy.get();
            }
        };

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
        StoreCopyClient copier =
                new StoreCopyClient( config, loadKernelExtensions(), console, logging, fs, pageCache, storeCopyMonitor );

        final GraphDatabaseAPI original =
                (GraphDatabaseAPI) startDatabase( originalDir );

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
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );

            long nodesCount = Iterables.count( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) );
            assertThat( nodesCount, equalTo( 0l ) );

            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( storeCopyRequest, times( 1 ) ).done();
    }

    private StoreCopyClient.StoreCopyRequester storeCopyRequest( final String originalDir,
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

                    TransactionIdStore transactionIdStore = original.getDependencyResolver().resolveDependency(
                            TransactionIdStore.class );

                    LogicalTransactionStore logicalTransactionStore  = original.getDependencyResolver().resolveDependency(
                            LogicalTransactionStore.class );

                    LogRotationControl logRotationControl = original.getDependencyResolver().resolveDependency(
                            LogRotationControl.class );

                    RequestContext requestContext = new StoreCopyServer(transactionIdStore, neoStoreDataSource,
                            logRotationControl, fs, new File(originalDir), new Monitors().newMonitor( StoreCopyServer.Monitor.class ) )
                            .flushStoresAndStreamStoreFiles( writer, false );

                    final StoreId storeId = original.getDependencyResolver().resolveDependency( StoreId.class );

                    ResponsePacker responsePacker = new ResponsePacker( logicalTransactionStore,
                            transactionIdStore, new Provider<StoreId>()
                    {
                        @Override
                        public StoreId instance()
                        {
                            return storeId;
                        }
                    } );


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
