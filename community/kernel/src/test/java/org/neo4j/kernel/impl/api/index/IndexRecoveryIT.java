/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.getIndexes;
import static org.neo4j.graphdb.Neo4jMatchers.hasSize;
import static org.neo4j.graphdb.Neo4jMatchers.haveState;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

public class IndexRecoveryIT
{
    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndex() throws Exception
    {
        // Given
        startDb();

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );

        // And Given
        Future<Void> killFuture = killDbInSeparateThread();
        latch.countDown();
        killFuture.get();

        // When
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.POPULATING ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) );
        verify( mockedIndexProvider, times( 0 ) ).getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        latch.countDown();
    }

    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndexWhereLogHasRotated() throws Exception
    {
        // Given
        startDb();

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );
        rotateLogs();

        // And Given
        Future<Void> killFuture = killDbInSeparateThread();
        latch.countDown();
        killFuture.get();
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.POPULATING ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) );
        verify( mockedIndexProvider, times( 0 ) ).getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        latch.countDown();
    }

    @Test
    public void shouldBeAbleToRecoverAndUpdateOnlineIndex() throws Exception
    {
        // Given
        startDb();

        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( mock( IndexPopulator.class ) );
        IndexAccessor mockedAccessor = mock( IndexAccessor.class );
        when( mockedAccessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( SwallowingIndexUpdater.INSTANCE );
        when( mockedIndexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) )
                .thenReturn( mockedAccessor );
        createIndexAndAwaitPopulation( myLabel );
        Set<NodePropertyUpdate> expectedUpdates = createSomeBananas( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.ONLINE );
        GatheringIndexWriter writer = new GatheringIndexWriter();
        when( mockedIndexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) )
                .thenReturn( writer );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.ONLINE ) ) );
        verify( mockedIndexProvider, times( 1 ) ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) );
        int onlineAccessorInvocationCount = 2; // once when we create the index, and once when we restart the db
        verify( mockedIndexProvider, times( onlineAccessorInvocationCount ) )
                .getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        assertEquals( expectedUpdates, writer.recoveredUpdates );
        for ( NodePropertyUpdate update : writer.recoveredUpdates )
        {
            assertTrue( writer.recoveredNodes.contains( update.getNodeId() ) );
        }
    }

    @Test
    public void shouldKeepFailedIndexesAsFailedAfterRestart() throws Exception
    {
        // Given
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) ) )
                .thenReturn( mock( IndexPopulator.class ) );
        when( mockedIndexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        startDb();
        createIndex( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.FAILED );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.FAILED ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ) );
    }

    @SuppressWarnings("deprecation") private GraphDatabaseAPI db;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final SchemaIndexProvider mockedIndexProvider = mock( SchemaIndexProvider.class );
    private final KernelExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceSchemaIndexProviderFactory( TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR.getKey(),
                    mockedIndexProvider );
    private final String key = "number_of_bananas_owned";
    private final Label myLabel = label( "MyLabel" );

    @Before
    public void setUp()
    {
        when( mockedIndexProvider.getProviderDescriptor() )
                .thenReturn( TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR );
        when( mockedIndexProvider.compareTo( any( SchemaIndexProvider.class ) ) )
                .thenReturn( 1 ); // always pretend to have highest priority
    }

    @SuppressWarnings("deprecation")
    private void startDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.addKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( mockedIndexProviderFactory ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void killDb()
    {
        if ( db != null )
        {
            fs.snapshot( new Runnable()
            {
                @Override
                public void run()
                {
                    db.shutdown();
                    db = null;
                }
            } );
        }
    }

    private Future<Void> killDbInSeparateThread()
    {
        ExecutorService executor = newSingleThreadExecutor();
        Future<Void> result = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                killDb();
                return null;
            }
        } );
        executor.shutdown();
        return result;
    }

    @After
    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    private void rotateLogs()
    {
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).rotateLogicalLogs();
    }

    private void createIndexAndAwaitPopulation( Label label )
    {
        IndexDefinition index = createIndex( label );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, SECONDS );
            tx.success();
        }
    }

    private IndexDefinition createIndex( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( label ).on( key ).create();
            tx.success();
            return index;
        }
    }

    private Set<NodePropertyUpdate> createSomeBananas( Label label )
    {
        Set<NodePropertyUpdate> updates = new HashSet<>();
        try ( Transaction tx = db.beginTx() )
        {
            ThreadToStatementContextBridge ctxProvider = db.getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class );
            try ( Statement statement = ctxProvider.instance() )
            {
                for ( int number : new int[] {4, 10} )
                {
                    Node node = db.createNode( label );
                    node.setProperty( key, number );
                    updates.add( NodePropertyUpdate.add( node.getId(),
                            statement.readOperations().propertyKeyGetForName( key ), number,
                            new long[]{statement.readOperations().labelGetForName( label.name() )} ) );
                }
            }
            tx.success();
            return updates;
        }
    }

    public static class GatheringIndexWriter extends IndexAccessor.Adapter
    {
        private final Set<NodePropertyUpdate> regularUpdates = new HashSet<>();
        private final Set<NodePropertyUpdate> recoveredUpdates = new HashSet<>();
        private final Set<Long> recoveredNodes = new HashSet<>();

        @Override
        public IndexUpdater newUpdater( final IndexUpdateMode mode )
        {
            return new CollectingIndexUpdater()
            {
                @Override
                public void close() throws IOException, IndexEntryConflictException
                {
                    switch (mode)
                    {
                        case ONLINE:
                            regularUpdates.addAll( updates );
                            break;

                        case RECOVERY:
                            recoveredUpdates.addAll( updates );
                            break;

                        default:
                            throw new UnsupportedOperationException(  );
                    }
                }

                @Override
                public void remove( Iterable<Long> nodeIds ) throws IOException
                {
                    for ( Long nodeId : nodeIds )
                    {
                        recoveredNodes.add( nodeId );
                    }
                }
            };
        }
    }

    private IndexPopulator indexPopulatorWithControlledCompletionTiming( final CountDownLatch latch )
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public void create()
            {
                try
                {
                    latch.await();
                }
                catch ( InterruptedException e )
                {
                    // fall through and return early
                }
                throw new RuntimeException( "this is expected" );
            }
        };
    }
}
