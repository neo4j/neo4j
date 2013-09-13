/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

public class IndexPopulationJobTest
{
    @Test
    public void shouldPopulateIndexWithOneNode() throws Exception
    {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode( map( name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy() );

        // WHEN
        job.run();

        // THEN
        verify( populator ).create();
        verify( populator ).add( nodeId, value );
        verify( populator ).close( true );

        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldFlushSchemaStateAfterPopulation() throws Exception
    {
        // GIVEN
        String value = "Taylor";
        createNode( map( name, value ), FIRST );
        stateHolder.apply( MapUtil.stringMap( "key", "original_value" ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy() );

        // WHEN
        job.run();

        // THEN
        String result = stateHolder.get( "key" );
        assertEquals( null, result );
    }

    @Test
    public void shouldPopulateIndexWithASmallDataset() throws Exception
    {
        // GIVEN
        String value = "Mattias";
        long node1 = createNode( map( name, value ), FIRST );
        createNode( map( name, value ), SECOND );
        createNode( map( age, 31 ), FIRST );
        long node4 = createNode( map( age, 35, name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy() );

        // WHEN
        job.run();

        // THEN
        verify( populator ).create();
        verify( populator ).add( node1, value );
        verify( populator ).add( node4, value );
        verify( populator ).close( true );

        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldIndexUpdatesWhenDoingThePopulation() throws Exception
    {
        // GIVEN
        Object value1 = "Mattias", value2 = "Jacob", value3 = "Stefan", changedValue = "changed";
        long node1 = createNode( map( name, value1 ), FIRST );
        long node2 = createNode( map( name, value2 ), FIRST );
        long node3 = createNode( map( name, value3 ), FIRST );
        @SuppressWarnings("UnnecessaryLocalVariable")
        long changeNode = node1;
        int propertyKeyId = getPropertyKeyForName( name );
        NodeChangingWriter populator = new NodeChangingWriter( changeNode, propertyKeyId, value1, changedValue,
                labelId );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy() );
        populator.setJob( job );

        // WHEN
        job.run();

        // THEN
        Set<Pair<Long, Object>> expected = asSet(
                Pair.of( node1, value1 ),
                Pair.of( node2, value2 ),
                Pair.of( node3, value3 ),
                Pair.of( node1, changedValue ) );
        assertEquals( expected, populator.added );
    }

    @Test
    public void shouldRemoveViaIndexUpdatesWhenDoingThePopulation() throws Exception
    {
        // GIVEN
        String value1 = "Mattias", value2 = "Jacob", value3 = "Stefan";
        long node1 = createNode( map( name, value1 ), FIRST );
        long node2 = createNode( map( name, value2 ), FIRST );
        long node3 = createNode( map( name, value3 ), FIRST );
        int propertyKeyId = getPropertyKeyForName( name );
        NodeDeletingWriter populator = new NodeDeletingWriter( node2, propertyKeyId, value2, labelId );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy() );
        populator.setJob( job );

        // WHEN
        job.run();

        // THEN
        Map<Long, Object> expectedAdded = genericMap( node1, value1, node2, value2, node3, value3 );
        assertEquals( expectedAdded, populator.added );
        Map<Long, Object> expectedRemoved = genericMap( node2, value2 );
        assertEquals( expectedRemoved, populator.removed );
    }

    @Test
    public void shouldTransitionToFailedStateIfPopulationJobCrashes() throws Exception
    {
        // GIVEN
        IndexPopulator failingPopulator = mock( IndexPopulator.class );
        doThrow( new RuntimeException( "BORK BORK" ) ).when( failingPopulator ).add( anyLong(), any() );

        FlippableIndexProxy index = new FlippableIndexProxy();

        createNode( map( name, "Taylor" ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, failingPopulator, index );

        // WHEN
        job.run();

        // THEN
        assertThat(index.getState(), equalTo(InternalIndexState.FAILED));
    }

    @Test
    public void shouldBeAbleToCancelPopulationJob() throws Exception
    {
        // GIVEN
        createNode( map( name, "Mattias" ), FIRST );
        IndexPopulator populator = mock( IndexPopulator.class );
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        IndexStoreView storeView = mock( IndexStoreView.class );
        ControlledStoreScan storeScan = new ControlledStoreScan();
        when( storeView.visitNodesWithPropertyAndLabel( any( IndexDescriptor.class ),
                Matchers.<Visitor<NodePropertyUpdate, RuntimeException>>any() ) ).thenReturn( storeScan );


        final IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, storeView,
                StringLogger.DEV_NULL );

        OtherThreadExecutor<Void> populationJobRunner = new OtherThreadExecutor<>(
                "Population job test runner", null );
        Future<Void> runFuture = populationJobRunner.executeDontWait( new WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state )
            {
                job.run();
                return null;
            }
        } );

        storeScan.latch.awaitStart();
        job.cancel().get();
        storeScan.latch.awaitFinish();

        // WHEN
        runFuture.get();

        // THEN
        verify( populator, times( 1 ) ).close( false );
        verify( index, times( 0 ) ).flip( Matchers.<Callable<Void>>any(), Matchers.<FailedIndexProxyFactory>any() );
    }

    @Test
    public void shouldLogJobProgress() throws Exception
    {
        // Given
        createNode( map( name, "irrelephant" ), FIRST );
        TestLogger logger = new TestLogger();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        NeoStoreIndexStoreView store = newStoreView();

        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, store, logger );

        // When
        job.run();

        // Then
        logger.assertExactly(
                info( "Index population started: [:FIRST(name)]" ),
                info( "Index population completed. Index is now online: [:FIRST(name)]" )
        );
    }

    @Test
    public void shouldLogJobFailure() throws Exception
    {
        // Given
        createNode( map( name, "irrelephant" ), FIRST );
        TestLogger logger = new TestLogger();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        NeoStoreIndexStoreView store = newStoreView();

        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, store, logger );

        IllegalStateException failure = new IllegalStateException( "not successful" );
        doThrow( failure ).when( populator ).create();

        // When
        job.run();

        // Then
        logger.assertAtLeastOnce( error( "Failed to populate index: [:FIRST(name)]", failure ) );
    }

    @Test
    public void shouldFlipToFailedUsingFailedIndexProxyFactory() throws Exception
    {
        // Given
        FailedIndexProxyFactory failureDelegateFactory = mock( FailedIndexProxyFactory.class );
        IndexPopulationJob job =
            newIndexPopulationJob( FIRST, name, failureDelegateFactory, populator,
                    new FlippableIndexProxy(), newStoreView(), new TestLogger() );


        IllegalStateException failure = new IllegalStateException( "not successful" );
        doThrow( failure ).when( populator ).close( true );

        // When
        job.run();

        // Then
        verify( failureDelegateFactory ).create( any( Throwable.class ) );
    }

    @Test
    public void shouldCloseAndFailOnFailure() throws Exception
    {
        createNode( map( name, "irrelephant" ), FIRST );
        TestLogger logger = new TestLogger();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        NeoStoreIndexStoreView store = newStoreView();

        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, store, logger );

        String failureMessage = "not successful";
        IllegalStateException failure = new IllegalStateException( failureMessage );
        doThrow( failure ).when( populator ).create();

        // When
        job.run();

        // Then
        verify( populator ).markAsFailed( Matchers.contains( failureMessage ) );
    }

    private static class ControlledStoreScan implements StoreScan<RuntimeException>
    {
        private final DoubleLatch latch = new DoubleLatch();

        @Override
        public void run()
        {
            latch.startAndAwaitFinish();
        }

        @Override
        public void stop()
        {
            latch.finish();
        }
    }

    private class NodeChangingWriter extends IndexPopulator.Adapter
    {
        private final Set<Pair<Long, Object>> added = new HashSet<>();
        private IndexPopulationJob job;
        private final long nodeToChange;
        private final Object newValue;
        private final Object previousValue;
        private final int label, propertyKeyId;

        public NodeChangingWriter( long nodeToChange, int propertyKeyId, Object previousValue, Object newValue,
                                   int label )
        {
            this.nodeToChange = nodeToChange;
            this.propertyKeyId = propertyKeyId;
            this.previousValue = previousValue;
            this.newValue = newValue;
            this.label = label;
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            if ( nodeId == 2 )
            {
                long[] labels = new long[]{label};
                job.update( asList( NodePropertyUpdate.change( nodeToChange, propertyKeyId, previousValue, labels,
                        newValue, labels ) ) );
            }
            added.add( Pair.of( nodeId, propertyValue ) );
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
            for ( NodePropertyUpdate update : updates )
            {
                switch ( update.getUpdateMode() )
                {
                case ADDED: case CHANGED:
                    added.add( Pair.of( update.getNodeId(), update.getValueAfter() ) );
                    break;
                default:
                    throw new IllegalArgumentException( update.getUpdateMode().name() );
                }
            }

        }

        public void setJob( IndexPopulationJob job )
        {
            this.job = job;
        }
    }

    private class NodeDeletingWriter extends IndexPopulator.Adapter
    {
        private final Map<Long, Object> added = new HashMap<>();
        private final Map<Long, Object> removed = new HashMap<>();
        private final long nodeToDelete;
        private IndexPopulationJob job;
        private final int propertyKeyId;
        private final Object valueToDelete;
        private final int label;

        public NodeDeletingWriter( long nodeToDelete, int propertyKeyId, Object valueToDelete, int label )
        {
            this.nodeToDelete = nodeToDelete;
            this.propertyKeyId = propertyKeyId;
            this.valueToDelete = valueToDelete;
            this.label = label;
        }

        public void setJob( IndexPopulationJob job )
        {
            this.job = job;
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            if ( nodeId == 3 )
            {
                job.update( asList( NodePropertyUpdate.remove( nodeToDelete, propertyKeyId, valueToDelete,
                        new long[]{label} ) ) );
            }
            added.put( nodeId, propertyValue );
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
            for ( NodePropertyUpdate update : updates )
            {
                switch ( update.getUpdateMode() )
                {
                case ADDED: case CHANGED:
                    added.put( update.getNodeId(), update.getValueAfter() );
                    break;
                case REMOVED:
                    removed.put( update.getNodeId(), update.getValueBefore() );
                    break;
                default:
                    throw new IllegalArgumentException( update.getUpdateMode().name() );
                }
            }

        }
    }

    private ImpermanentGraphDatabase db;

    private final Label FIRST = DynamicLabel.label( "FIRST" );
    private final Label SECOND = DynamicLabel.label( "SECOND" );

    private final String name = "name", age = "age";
    private ThreadToStatementContextBridge ctxProvider;
    private IndexPopulator populator;
    private KernelSchemaStateStore stateHolder;

    private int labelId;

    @Before
    public void before() throws Exception
    {
        db = (ImpermanentGraphDatabase) new TestGraphDatabaseFactory().newImpermanentDatabase();
        ctxProvider = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        populator = mock( IndexPopulator.class );
        stateHolder = new KernelSchemaStateStore();

        Transaction tx = db.beginTx();
        Statement statement = ctxProvider.statement();
        labelId = statement.schemaWriteOperations().labelGetOrCreateForName( FIRST.name() );

        statement.schemaWriteOperations().labelGetOrCreateForName( SECOND.name() );
        statement.close();
        tx.success();
        tx.finish();
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey, IndexPopulator populator,
                                                      FlippableIndexProxy flipper )
    {
        return newIndexPopulationJob( label, propertyKey, populator, flipper, newStoreView(),
                StringLogger.DEV_NULL );
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey,
                                                      IndexPopulator populator,
                                                      FlippableIndexProxy flipper, IndexStoreView storeView,
                                                      StringLogger logger )
    {
        return newIndexPopulationJob( label, propertyKey,
                mock( FailedIndexProxyFactory.class ), populator, flipper, storeView, logger );
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey,
                                                      FailedIndexProxyFactory failureDelegateFactory,
                                                      IndexPopulator populator,
                                                      FlippableIndexProxy flipper, IndexStoreView storeView,
                                                      StringLogger logger )
    {
        IndexDescriptor descriptor;
        try ( Transaction tx = db.beginTx() )
        {
            ReadOperations statement = ctxProvider.statement().readOperations();
            descriptor = new IndexDescriptor( statement.labelGetForName( label.name() ),
                    statement.propertyKeyGetForName( propertyKey ) );
            tx.success();
        }

        flipper.setFlipTarget( mock( IndexProxyFactory.class ) );
        return new IndexPopulationJob(
                descriptor, PROVIDER_DESCRIPTOR,
                format( ":%s(%s)", label.name(), propertyKey ),
                failureDelegateFactory,
                populator, flipper, storeView,
                stateHolder, new SingleLoggingService( logger ) );
    }

    private long createNode( Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node.getId();
        }
    }

    private int getPropertyKeyForName( String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int result = ctxProvider.statement().readOperations().propertyKeyGetForName( name );
            tx.success();
            return result;
        }
    }

    @SuppressWarnings( "deprecation" )
    private NeoStoreIndexStoreView newStoreView()
    {
        return new NeoStoreIndexStoreView(
                db.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore() );
    }
}
