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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.impl.api.index.IndexingService.NO_MONITOR;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class IndexPopulationJobTest
{
    @Test
    public void shouldPopulateIndexWithOneNode() throws Exception
    {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode( map( name, value ), FIRST );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy(), false );

        // WHEN
        job.run();

        // THEN
        NodePropertyUpdate update = add( nodeId, 0, value, new long[]{0} );

        verify( populator ).create();
        verify( populator ).includeSample( update );
        verify( populator ).add( anyListOf(NodePropertyUpdate.class) );
        verify( populator ).verifyDeferredConstraints( indexStoreView );
        verify( populator ).sampleResult();
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
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy(), false );

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
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy(), false );

        // WHEN
        job.run();

        // THEN
        NodePropertyUpdate update1 = add( node1, 0, value, new long[]{0} );
        NodePropertyUpdate update2 = add( node4, 0, value, new long[]{0} );

        verify( populator ).create();
        verify( populator ).includeSample( update1 );
        verify( populator ).includeSample( update2 );
        verify( populator, times( 2 ) ).add( anyListOf(NodePropertyUpdate.class ) );
        verify( populator ).verifyDeferredConstraints( indexStoreView );
        verify( populator ).sampleResult();
        verify( populator ).close( true );

        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldIndexConcurrentUpdatesWhilePopulating() throws Exception
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
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy(), false );
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
    public void shouldRemoveViaConcurrentIndexUpdatesWhilePopulating() throws Exception
    {
        // GIVEN
        String value1 = "Mattias", value2 = "Jacob", value3 = "Stefan";
        long node1 = createNode( map( name, value1 ), FIRST );
        long node2 = createNode( map( name, value2 ), FIRST );
        long node3 = createNode( map( name, value3 ), FIRST );
        int propertyKeyId = getPropertyKeyForName( name );
        NodeDeletingWriter populator = new NodeDeletingWriter( node2, propertyKeyId, value2, labelId );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexProxy(), false );
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
        doThrow( new RuntimeException( "BORK BORK" ) ).when( failingPopulator ).add( any() );

        FlippableIndexProxy index = new FlippableIndexProxy();

        createNode( map( name, "Taylor" ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, failingPopulator, index, false );

        // WHEN
        job.run();

        // THEN
        assertThat( index.getState(), equalTo( InternalIndexState.FAILED ) );
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
        when( storeView.visitNodes( any(int[].class), any( IntPredicate.class ),
                Matchers.<Visitor<NodePropertyUpdates,RuntimeException>>any(),
                Matchers.<Visitor<NodeLabelUpdate,RuntimeException>>any()) )
                .thenReturn(storeScan );

        final IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, storeView,
                NullLogProvider.getInstance(), false );

        OtherThreadExecutor<Void> populationJobRunner = cleanup.add( new OtherThreadExecutor<>(
                "Population job test runner", null ) );
        Future<Void> runFuture = populationJobRunner
                .executeDontWait( state -> {
                    job.run();
                    return null;
                } );

        storeScan.latch.awaitStart();
        job.cancel().get();
        storeScan.latch.awaitFinish();

        // WHEN
        runFuture.get();

        // THEN
        verify( populator, times( 1 ) ).close( false );
        verify( index, times( 0 ) ).flip( Matchers.<Callable<Void>>any(), Matchers.<FailedIndexProxyFactory>any() );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexUpdatesAndSize( FIRST, name ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexSample( FIRST, name ) );
    }

    @Test
    public void shouldLogJobProgress() throws Exception
    {
        // Given
        createNode( map( name, "irrelephant" ), FIRST );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, indexStoreView, logProvider, false );

        // When
        job.run();

        // Then
        LogMatcherBuilder match = inLog( IndexPopulationJob.class );
        logProvider.assertExactly(
                match.info( "Index population started: [%s]", ":FIRST(name)" ),
                match.info( "Index population completed. Index is now online: [%s]", ":FIRST(name)" )
        );
    }

    @Test
    public void shouldLogJobFailure() throws Exception
    {
        // Given
        createNode( map( name, "irrelephant" ), FIRST );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, indexStoreView, logProvider, false );

        Throwable failure = new IllegalStateException( "not successful" );
        doThrow( failure ).when( populator ).create();

        // When
        job.run();

        // Then
        LogMatcherBuilder match = inLog( IndexPopulationJob.class );
        logProvider.assertAtLeastOnce(
                match.error( is( "Failed to populate index: [:FIRST(name)]" ), sameInstance( failure ) )
        );
    }

    @Test
    public void shouldFlipToFailedUsingFailedIndexProxyFactory() throws Exception
    {
        // Given
        FailedIndexProxyFactory failureDelegateFactory = mock( FailedIndexProxyFactory.class );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job =
                newIndexPopulationJob( FIRST, name, failureDelegateFactory, populator,
                        new FlippableIndexProxy(), indexStoreView, NullLogProvider.getInstance(), false );


        IllegalStateException failure = new IllegalStateException( "not successful" );
        doThrow( failure ).when( populator ).close( true );

        // When
        job.run();

        // Then
        verify( failureDelegateFactory ).create( any( Throwable.class ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexUpdatesAndSize( FIRST, name ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexSample( FIRST, name ) );
    }

    @Test
    public void shouldCloseAndFailOnFailure() throws Exception
    {
        createNode( map( name, "irrelephant" ), FIRST );
        LogProvider logProvider = NullLogProvider.getInstance();
        FlippableIndexProxy index = mock( FlippableIndexProxy.class );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, indexStoreView, logProvider, false );

        String failureMessage = "not successful";
        IllegalStateException failure = new IllegalStateException( failureMessage );
        doThrow( failure ).when( populator ).create();

        // When
        job.run();

        // Then
        verify( populator ).markAsFailed( Matchers.contains( failureMessage ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexUpdatesAndSize( FIRST, name ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexSample( FIRST, name ) );
    }

    @Test
    public void shouldFailIfDeferredConstraintViolated() throws Exception
    {
        createNode( map( name, "irrelephant" ), FIRST );
        LogProvider logProvider = NullLogProvider.getInstance();
        FlippableIndexProxy index = new FlippableIndexProxy( mock( IndexProxy.class ) );
        IndexPopulator populator = spy( inMemoryPopulator( false ) );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, index, indexStoreView, logProvider, true );

        IndexEntryConflictException failure = new PreexistingIndexEntryConflictException( "duplicate value", 0, 1 );
        doThrow( failure ).when( populator ).verifyDeferredConstraints( indexStoreView );

        // When
        job.run();

        // Then
        verify( populator ).markAsFailed( Matchers.contains( "duplicate value" ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexUpdatesAndSize( FIRST, name ) );

        // AND ALSO
        assertDoubleLongEquals( 0, 0, indexSample( FIRST, name ) );
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

        @Override
        public PopulationProgress getProgress()
        {
            return new PopulationProgress( 42, 100 );
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
        public void add( Collection<NodePropertyUpdate> updates )
        {
            for ( NodePropertyUpdate update : updates )
            {
                if ( update.getNodeId() == 2 )
                {
                    long[] labels = new long[]{label};
                    job.update( change( nodeToChange, propertyKeyId, previousValue, labels, newValue, labels ) );
                }
                added.add( Pair.of( update.getNodeId(), update.getValueAfter() ) );
            }
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor )
        {
            return new IndexUpdater()
            {
                @Override
                public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
                {
                    switch ( update.getUpdateMode() )
                    {
                        case ADDED:
                        case CHANGED:
                            added.add( Pair.of( update.getNodeId(), update.getValueAfter() ) );
                            break;
                        default:
                            throw new IllegalArgumentException( update.getUpdateMode().name() );
                    }
                }


                @Override
                public void close() throws IOException, IndexEntryConflictException
                {
                }

                @Override
                public void remove( PrimitiveLongSet nodeIds )
                {
                    throw new UnsupportedOperationException( "not expected" );
                }
            };
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
        public void add( Collection<NodePropertyUpdate> updates )
        {
            for ( NodePropertyUpdate update : updates )
            {
                if ( update.getNodeId() == 2 )
                {
                    job.update( remove( nodeToDelete, propertyKeyId, valueToDelete, new long[]{label} ) );
                }
                added.put( update.getNodeId(), update.getValueAfter() );
            }
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor )
        {
            return new IndexUpdater()
            {
                @Override
                public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
                {
                    switch ( update.getUpdateMode() )
                    {
                        case ADDED:
                        case CHANGED:
                            added.put( update.getNodeId(), update.getValueAfter() );
                            break;
                        case REMOVED:
                            removed.put( update.getNodeId(), update.getValueBefore() );
                            break;
                        default:
                            throw new IllegalArgumentException( update.getUpdateMode().name() );
                    }
                }

                @Override
                public void close() throws IOException, IndexEntryConflictException
                {
                }

                @Override
                public void remove( PrimitiveLongSet nodeIds )
                {
                    throw new UnsupportedOperationException( "not expected" );
                }
            };
        }
    }

    private IndexPopulator inMemoryPopulator( boolean constraint ) throws TransactionFailureException
    {
        IndexConfiguration indexConfig = IndexConfiguration.of( constraint );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.empty() );
        IndexDescriptor descriptor = indexDescriptor( FIRST, name );
        return new InMemoryIndexProvider().getPopulator( 21, descriptor, indexConfig, samplingConfig );
    }

    private GraphDatabaseAPI db;

    private final Label FIRST = Label.label( "FIRST" );
    private final Label SECOND = Label.label( "SECOND" );
    private final String name = "name";
    private final String age = "age";

    private KernelAPI kernel;
    private IndexStoreView indexStoreView;
    private KernelSchemaStateStore stateHolder;

    private int labelId;
    public final @Rule CleanupRule cleanup = new CleanupRule();

    @Before
    public void before() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        stateHolder = new KernelSchemaStateStore( NullLogProvider.getInstance() );
        indexStoreView = indexStoreView();

        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
              Statement statement = tx.acquireStatement() )
        {
            labelId = statement.schemaWriteOperations().labelGetOrCreateForName( FIRST.name() );
            statement.schemaWriteOperations().labelGetOrCreateForName( SECOND.name() );
            tx.success();
        }
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey, IndexPopulator populator,
                                                      FlippableIndexProxy flipper, boolean constraint )
                                                              throws TransactionFailureException
    {
        return newIndexPopulationJob( label, propertyKey, populator, flipper, indexStoreView,
                NullLogProvider.getInstance(), constraint );
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey,
                                                      IndexPopulator populator,
                                                      FlippableIndexProxy flipper, IndexStoreView storeView,
                                                      LogProvider logProvider,
                                                      boolean constraint ) throws TransactionFailureException
    {
        return newIndexPopulationJob( label, propertyKey,
                mock( FailedIndexProxyFactory.class ), populator, flipper, storeView, logProvider, constraint );
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey,
                                                      FailedIndexProxyFactory failureDelegateFactory,
                                                      IndexPopulator populator,
                                                      FlippableIndexProxy flipper, IndexStoreView storeView,
                                                      LogProvider logProvider, boolean constraint )
                                                              throws TransactionFailureException
    {
        IndexDescriptor descriptor = indexDescriptor( label, propertyKey );
        flipper.setFlipTarget( mock( IndexProxyFactory.class ) );

        MultipleIndexPopulator multiPopulator = new MultipleIndexPopulator( storeView, logProvider );
        IndexPopulationJob job = new IndexPopulationJob( storeView, multiPopulator, NO_MONITOR, stateHolder::clear );
        job.addPopulator( populator, descriptor, IndexConfiguration.of( constraint ), PROVIDER_DESCRIPTOR,
                format( ":%s(%s)", label.name(), propertyKey ), flipper, failureDelegateFactory );
        return job;
    }

    private IndexDescriptor indexDescriptor( Label label, String propertyKey ) throws TransactionFailureException
    {
        IndexDescriptor descriptor;
        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AccessMode.Static.READ );
              Statement statement = tx.acquireStatement() )
        {
            descriptor = new IndexDescriptor( statement.readOperations().labelGetForName( label.name() ),
                    statement.readOperations().propertyKeyGetForName( propertyKey ) );
            tx.success();
        }
        return descriptor;
    }

    private DoubleLongRegister indexUpdatesAndSize( Label label, String propertyKey ) throws KernelException
    {
        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AccessMode.Static.READ );
              Statement statement = tx.acquireStatement() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
            DoubleLongRegister result =
                    statement.readOperations().indexUpdatesAndSize( new IndexDescriptor( labelId, propertyKeyId ),
                            Registers.newDoubleLongRegister() );
            tx.success();
            return result;
        }
    }

    private DoubleLongRegister indexSample( Label label, String propertyKey ) throws KernelException
    {
        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AccessMode.Static.READ );
              Statement statement = tx.acquireStatement() )
        {
            DoubleLongRegister result = Registers.newDoubleLongRegister();
            int labelId = statement.readOperations().labelGetForName( label.name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
            statement.readOperations().indexSample( new IndexDescriptor( labelId, propertyKeyId ), result );
            tx.success();
            return result;
        }
    }

    private void assertDoubleLongEquals( long expectedUniqueValue, long expectedSampledSize,
                                         DoubleLongRegister register )
    {
        assertEquals( expectedUniqueValue, register.readFirst() );
        assertEquals( expectedSampledSize, register.readSecond() );
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

    private int getPropertyKeyForName( String name ) throws TransactionFailureException
    {
        try ( KernelTransaction tx = kernel.newTransaction( KernelTransaction.Type.implicit, AccessMode.Static.READ );
              Statement statement = tx.acquireStatement() )
        {
            int result = statement.readOperations().propertyKeyGetForName( name );
            tx.success();
            return result;
        }
    }

    private IndexStoreView indexStoreView()
    {
        return db.getDependencyResolver().resolveDependency( IndexStoreView.class );
    }
}
