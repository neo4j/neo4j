/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator.AWAIT_TIMEOUT_MINUTES_NAME;
import static org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator.BATCH_SIZE_NAME;
import static org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator.QUEUE_THRESHOLD_NAME;
import static org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator.TASK_QUEUE_SIZE_NAME;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

public class BatchingMultipleIndexPopulatorTest
{
    @After
    public void tearDown() throws Exception
    {
        clearProperty( QUEUE_THRESHOLD_NAME );
        clearProperty( TASK_QUEUE_SIZE_NAME );
        clearProperty( AWAIT_TIMEOUT_MINUTES_NAME );
        clearProperty( BATCH_SIZE_NAME );
    }

    @Test
    public void populateFromQueueDoesNothingIfThresholdNotReached() throws Exception
    {
        setProperty( QUEUE_THRESHOLD_NAME, 5 );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator(
                mock( IndexStoreView.class ), mock( ExecutorService.class ), NullLogProvider.getInstance() );

        IndexPopulator populator = addPopulator( batchingPopulator, 1 );
        IndexUpdater updater = mock( IndexUpdater.class );
        when( populator.newPopulatingUpdater( any() ) ).thenReturn( updater );

        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 2, 1, "bar", new long[]{1} );
        batchingPopulator.queue( update1 );
        batchingPopulator.queue( update2 );

        batchingPopulator.populateFromQueueBatched( 42 );

        verify( updater, never() ).process( any() );
        verify( populator, never() ).newPopulatingUpdater( any() );
    }

    @Test
    public void populateFromQueuePopulatesWhenThresholdReached() throws Exception
    {
        setProperty( QUEUE_THRESHOLD_NAME, 2 );

        NeoStores neoStores = mock( NeoStores.class );
        NodeStore nodeStore = mock( NodeStore.class );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );

        NeoStoreIndexStoreView storeView =
                new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, neoStores );
        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator(
                storeView, mock( ExecutorService.class ), NullLogProvider.getInstance() );

        IndexPopulator populator1 = addPopulator( batchingPopulator, 1 );
        IndexUpdater updater1 = mock( IndexUpdater.class );
        when( populator1.newPopulatingUpdater( any() ) ).thenReturn( updater1 );

        IndexPopulator populator2 = addPopulator( batchingPopulator, 2 );
        IndexUpdater updater2 = mock( IndexUpdater.class );
        when( populator2.newPopulatingUpdater( any() ) ).thenReturn( updater2 );

        batchingPopulator.indexAllNodes();
        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 2, 2, "bar", new long[]{2} );
        NodePropertyUpdate update3 = NodePropertyUpdate.add( 3, 1, "baz", new long[]{1} );
        batchingPopulator.queue( update1 );
        batchingPopulator.queue( update2 );
        batchingPopulator.queue( update3 );

        batchingPopulator.populateFromQueue( 42 );

        verify( updater1 ).process( update1 );
        verify( updater1 ).process( update3 );
        verify( updater2 ).process( update2 );
    }

    @Test
    public void executorShutdownAfterStoreScanCompletes() throws Exception
    {
        NodePropertyUpdate update = NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} );
        IndexStoreView storeView = newStoreView( update );

        ExecutorService executor = mock( ExecutorService.class );
        when( executor.awaitTermination( anyLong(), any() ) ).thenReturn( true );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView,
                executor, NullLogProvider.getInstance() );

        StoreScan<IndexPopulationFailedKernelException> storeScan = batchingPopulator.indexAllNodes();
        verify( executor, never() ).shutdown();

        storeScan.run();
        verify( executor ).shutdown();
        verify( executor ).awaitTermination( anyLong(), any() );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void executorForcefullyShutdownIfStoreScanFails() throws Exception
    {
        IndexStoreView storeView = mock( IndexStoreView.class );
        StoreScan<Exception> failingStoreScan = mock( StoreScan.class );
        RuntimeException scanError = new RuntimeException();
        doThrow( scanError ).when( failingStoreScan ).run();
        when( storeView.visitNodes( any(), any(), any(), any() ) ).thenReturn( failingStoreScan );

        ExecutorService executor = mock( ExecutorService.class );
        when( executor.awaitTermination( anyLong(), any() ) ).thenReturn( true );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView,
                executor, NullLogProvider.getInstance() );

        StoreScan<IndexPopulationFailedKernelException> storeScan = batchingPopulator.indexAllNodes();
        verify( executor, never() ).shutdown();

        try
        {
            storeScan.run();
            fail( "Exception expected" );
        }
        catch ( Throwable t )
        {
            assertSame( scanError, t );
        }

        verify( executor ).shutdownNow();
        verify( executor ).awaitTermination( anyLong(), any() );
    }

    @Test
    public void pendingBatchesFlushedAfterStoreScan() throws Exception
    {
        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 2, 1, "bar", new long[]{1} );
        NodePropertyUpdate update3 = NodePropertyUpdate.add( 3, 1, "baz", new long[]{1} );
        NodePropertyUpdate update42 = NodePropertyUpdate.add( 4, 42, "42", new long[]{42} );
        IndexStoreView storeView = newStoreView( update1, update2, update3, update42 );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView,
                sameThreadExecutor(), NullLogProvider.getInstance() );

        IndexPopulator populator1 = addPopulator( batchingPopulator, 1 );
        IndexPopulator populator42 = addPopulator( batchingPopulator, 42 );

        batchingPopulator.indexAllNodes().run();

        verify( populator1 ).add( Arrays.asList( update1, update2, update3 ) );
        verify( populator42 ).add( singletonList( update42 ) );
    }

    @Test
    public void batchIsFlushedWhenThresholdReached() throws Exception
    {
        setProperty( BATCH_SIZE_NAME, 2 );

        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "foo", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 2, 1, "bar", new long[]{1} );
        NodePropertyUpdate update3 = NodePropertyUpdate.add( 3, 1, "baz", new long[]{1} );
        IndexStoreView storeView = newStoreView( update1, update2, update3 );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView,
                sameThreadExecutor(), NullLogProvider.getInstance() );

        IndexPopulator populator = addPopulator( batchingPopulator, 1 );

        batchingPopulator.indexAllNodes().run();

        verify( populator ).add( Arrays.asList( update1, update2 ) );
        verify( populator ).add( singletonList( update3 ) );
    }

    @Test
    public void populatorMarkedAsFailed() throws Exception
    {
        setProperty( BATCH_SIZE_NAME, 2 );

        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "aaa", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 1, 1, "bbb", new long[]{1} );
        IndexStoreView storeView = newStoreView( update1, update2 );

        RuntimeException batchFlushError = new RuntimeException( "Batch failed" );

        IndexPopulator populator;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView, executor,
                    NullLogProvider.getInstance() );

            populator = addPopulator( batchingPopulator, 1 );
            doThrow( batchFlushError ).when( populator ).add( Arrays.asList( update1, update2 ) );

            batchingPopulator.indexAllNodes().run();
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }

        verify( populator ).markAsFailed( failure( batchFlushError ).asString() );
    }

    @Test
    public void populatorMarkedAsFailedAndUpdatesNotAdded() throws Exception
    {
        setProperty( BATCH_SIZE_NAME, 2 );

        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 1, "aaa", new long[]{1} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 1, 1, "bbb", new long[]{1} );
        NodePropertyUpdate update3 = NodePropertyUpdate.add( 1, 1, "ccc", new long[]{1} );
        NodePropertyUpdate update4 = NodePropertyUpdate.add( 1, 1, "ddd", new long[]{1} );
        NodePropertyUpdate update5 = NodePropertyUpdate.add( 1, 1, "eee", new long[]{1} );
        IndexStoreView storeView = newStoreView( update1, update2, update3, update4, update5 );

        RuntimeException batchFlushError = new RuntimeException( "Batch failed" );

        BatchingMultipleIndexPopulator batchingPopulator = new BatchingMultipleIndexPopulator( storeView,
                sameThreadExecutor(), NullLogProvider.getInstance() );

        IndexPopulator populator = addPopulator( batchingPopulator, 1 );
        doThrow( batchFlushError ).when( populator ).add( Arrays.asList( update3, update4 ) );

        batchingPopulator.indexAllNodes().run();

        verify( populator ).add( Arrays.asList( update1, update2 ) );
        verify( populator ).add( Arrays.asList( update3, update4 ) );
        verify( populator ).markAsFailed( failure( batchFlushError ).asString() );
        verify( populator, never() ).add( singletonList( update5 ) );
    }

    private static IndexPopulator addPopulator( BatchingMultipleIndexPopulator batchingPopulator, int id )
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        IndexDescriptor descriptor = new IndexDescriptor( id, id );

        IndexProxyFactory indexProxyFactory = mock( IndexProxyFactory.class );
        FailedIndexProxyFactory failedIndexProxyFactory = mock( FailedIndexProxyFactory.class );
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget( indexProxyFactory );

        batchingPopulator.addPopulator( populator, descriptor, new SchemaIndexProvider.Descriptor( "foo", "1" ),
                IndexConfiguration.NON_UNIQUE, flipper, failedIndexProxyFactory, "testIndex" );

        return populator;
    }

    @SuppressWarnings( "unchecked" )
    private static IndexStoreView newStoreView( NodePropertyUpdate... updates )
    {
        IndexStoreView storeView = mock( IndexStoreView.class );
        when( storeView.visitNodes( any(), any(), any(), any() ) ).thenAnswer( invocation -> {
            Object visitorArg = invocation.getArguments()[2];
            Visitor<NodePropertyUpdates,IndexPopulationFailedKernelException> visitor =
                    (Visitor<NodePropertyUpdates,IndexPopulationFailedKernelException>) visitorArg;
            return new NodePropertyUpdatesScan( updates, visitor );
        } );
        return storeView;
    }

    private static ExecutorService sameThreadExecutor() throws InterruptedException
    {
        ExecutorService executor = mock( ExecutorService.class );
        when( executor.awaitTermination( anyLong(), any() ) ).thenReturn( true );
        doAnswer( invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        } ).when( executor ).execute( any() );
        return executor;
    }

    private static void setProperty( String name, int value )
    {
        FeatureToggles.set( BatchingMultipleIndexPopulator.class, name, value );
    }

    private static void clearProperty( String name )
    {
        FeatureToggles.clear( BatchingMultipleIndexPopulator.class, name );
    }

    private static class NodePropertyUpdatesScan implements StoreScan<IndexPopulationFailedKernelException>
    {
        final NodePropertyUpdate[] updates;
        final Visitor<NodePropertyUpdates,IndexPopulationFailedKernelException> visitor;

        boolean stop;

        NodePropertyUpdatesScan( NodePropertyUpdate[] updates,
                Visitor<NodePropertyUpdates,IndexPopulationFailedKernelException> visitor )
        {
            this.updates = updates;
            this.visitor = visitor;
        }

        @Override
        public void run() throws IndexPopulationFailedKernelException
        {

            NodePropertyUpdates nodePropertyUpdates = new NodePropertyUpdates();
            for ( NodePropertyUpdate update : updates )
            {
                if ( stop )
                {
                    return;
                }
                nodePropertyUpdates.initForNodeId( update.getNodeId() );
                nodePropertyUpdates.add( update );
                visitor.visit( nodePropertyUpdates );
                nodePropertyUpdates.reset();
            }
        }

        @Override
        public void stop()
        {
            stop = true;
        }

        @Override
        public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, NodePropertyUpdate update,
                long currentlyIndexedNodeId )
        {

        }

        @Override
        public PopulationProgress getProgress()
        {
            return PopulationProgress.NONE;
        }

        @Override
        public void configure( List<MultipleIndexPopulator.IndexPopulation> populations )
        {

        }
    }
}
