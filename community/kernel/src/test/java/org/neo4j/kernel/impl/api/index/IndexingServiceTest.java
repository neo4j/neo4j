/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.index;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer;
import org.neo4j.kernel.impl.scheduler.GroupedDaemonThreadFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.backgroundRebuildAll;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.RECOVERY;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class IndexingServiceTest
{
    private final LifeSupport life = new LifeSupport();

    private static final IndexProviderDescriptor native30Descriptor = new IndexProviderDescriptor( NATIVE30.providerKey(), NATIVE30.providerVersion() );
    private static final IndexProviderDescriptor nativeBtree10Descriptor =
            new IndexProviderDescriptor( NATIVE_BTREE10.providerKey(), NATIVE_BTREE10.providerVersion() );
    private static final IndexProviderDescriptor fulltextDescriptor = new IndexProviderDescriptor( "fulltext", "1.0" );
    private static final IndexProviderDescriptor tokenDescriptor = new IndexProviderDescriptor( "token-lookup", "1.0" );
    private static final IndexProviderDescriptor textDescriptor = new IndexProviderDescriptor( "text", "1.0" );
    private static final IndexProviderDescriptor[] indexProviderDescriptors =
            new IndexProviderDescriptor[]{native30Descriptor, nativeBtree10Descriptor, fulltextDescriptor};
    private final SchemaState schemaState = mock( SchemaState.class );
    private final int labelId = 7;
    private final int propertyKeyId = 15;
    private final int uniquePropertyKeyId = 15;
    private final IndexPrototype prototype =
            forSchema( forLabel( labelId, propertyKeyId ) ).withIndexProvider( PROVIDER_DESCRIPTOR ).withName( "index" );
    private final IndexDescriptor index = prototype.materialise( 0 );
    private final IndexPrototype uniqueIndex =
            uniqueForSchema( forLabel( labelId, uniquePropertyKeyId ) ).withIndexProvider( PROVIDER_DESCRIPTOR ).withName( "constraint" );
    private final IndexPopulator populator = mock( IndexPopulator.class );
    private final IndexUpdater updater = mock( IndexUpdater.class );
    private final IndexProvider indexProvider = mock( IndexProvider.class );
    private final IndexAccessor accessor = mock( IndexAccessor.class, RETURNS_MOCKS );
    private final IndexStoreView storeView  = mock( IndexStoreView.class );
    private final IndexStoreViewFactory storeViewFactory  = mock( IndexStoreViewFactory.class );
    private final NodePropertyAccessor propertyAccessor = mock( NodePropertyAccessor.class );
    private final InMemoryTokens nameLookup = new InMemoryTokens();
    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );
    private final JobScheduler scheduler = JobSchedulerFactory.createScheduler();

    @BeforeEach
    void setUp() throws IndexNotFoundKernelException
    {
        when( populator.sample( any( CursorContext.class ) ) ).thenReturn( new IndexSample() );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample() );
        when( storeViewFactory.createTokenIndexStoreView( any() )).thenReturn( storeView );
        when( storeView.newPropertyAccessor( any( CursorContext.class ), any() ) ).thenReturn( propertyAccessor );
        ValueIndexReader indexReader = mock( ValueIndexReader.class );
        IndexSampler indexSampler = mock( IndexSampler.class );
        when( indexSampler.sampleIndex( any(), any() ) ).thenReturn( new IndexSample() );
        when( indexReader.createSampler() ).thenReturn( indexSampler );
        when( accessor.newValueReader() ).thenReturn( indexReader );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    @Test
    void noMessagesWhenThereIsNoIndexes() throws Throwable
    {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexingService indexingService = createIndexServiceWithCustomIndexMap( indexMapReference );
        indexingService.start();

        assertThat( internalLogProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldBringIndexOnlineAndFlipOverToIndexAccessor() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( AUTH_DISABLED, index );
        IndexProxy proxy = indexingService.getIndexProxy( index );

        waitForIndexesToComeOnline( indexingService, index );
        verify( populator, timeout( 10000 ) ).close( eq( true ), any() );

        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            updater.process( add( 10, "foo" ) );
        }

        // then
        assertEquals( ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).close( eq( true ), any() );
        order.verify( accessor ).newUpdater( eq( IndexUpdateMode.ONLINE_IDEMPOTENT ), any() );
        order.verify( updater ).process( add( 10, "foo" ) );
        order.verify( updater ).close();
    }

    @Test
    void indexCreationShouldBeIdempotent() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( AUTH_DISABLED, index );
        indexingService.createIndexes( AUTH_DISABLED, index );

        // We are asserting that the second call to createIndex does not throw an exception.
        waitForIndexesToComeOnline( indexingService, index );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldDeliverUpdatesThatOccurDuringPopulationToPopulator() throws Exception
    {
        // given
        when( populator.newPopulatingUpdater( eq( propertyAccessor ), any() ) ).thenReturn( updater );

        CountDownLatch populationLatch = new CountDownLatch( 1 );

        Barrier.Control populationStartBarrier = new Barrier.Control();
        IndexMonitor monitor = new IndexMonitor.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanStarting( IndexDescriptor[] indexDescriptors )
            {
                populationStartBarrier.reached();
            }

            @Override
            public void indexPopulationScanComplete()
            {
                try
                {
                    populationLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException( "Index population monitor was interrupted", e );
                }
            }
        };
        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies( populator, accessor, withData( addNodeUpdate( 1, "value1" ) ), monitor );

        life.start();

        // when

        indexingService.createIndexes( AUTH_DISABLED, index );
        IndexProxy proxy = indexingService.getIndexProxy( index );
        assertEquals( POPULATING, proxy.getState() );
        populationStartBarrier.await();
        populationStartBarrier.release();

        IndexEntryUpdate<?> value2 = add( 2, "value2" );
        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            updater.process( value2 );
        }

        populationLatch.countDown();

        waitForIndexesToComeOnline( indexingService, index );
        verify( populator ).close( eq( true ), any() );

        // then
        assertEquals( ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).includeSample( add( 1, "value1" ) );
        order.verify( populator, times( 1 ) ).add( any( Collection.class ), any( CursorContext.class ) );
        order.verify( populator ).scanCompleted( any( PhaseTracker.class ), any( IndexPopulator.PopulationWorkScheduler.class ),
                any( CursorContext.class ) );
        order.verify( populator ).newPopulatingUpdater( eq( propertyAccessor ), any() );
        order.verify( populator ).includeSample( any() );
        order.verify( updater ).process( any() );
        order.verify( updater ).close();
        order.verify( populator ).sample( any() );
        order.verify( populator ).close( eq( true ), any() );
        verifyNoMoreInteractions( updater );
        verifyNoMoreInteractions( populator );

        verifyNoInteractions( accessor );
    }

    @Test
    void shouldStillReportInternalIndexStateAsPopulatingWhenConstraintIndexIsDonePopulating() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater );
        ValueIndexReader indexReader = mock( ValueIndexReader.class );
        when( accessor.newValueReader() ).thenReturn( indexReader );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( index ) ).when( indexReader ).query( any(), any(), any(), any(), any() );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        IndexDescriptor index = constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        indexingService.createIndexes( AUTH_DISABLED, index );
        IndexProxy proxy = indexingService.getIndexProxy( index );

        // don't wait for index to come ONLINE here since we're testing that it doesn't
        verify( populator, timeout( 20000 ) ).close( eq( true ), any() );

        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            updater.process( add( 10, "foo" ) );
        }

        // then
        assertEquals( POPULATING, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater );
        order.verify( populator ).create();
        order.verify( populator ).close( eq( true ), any() );
        order.verify( accessor ).newUpdater( eq( IndexUpdateMode.ONLINE ), any() );
        order.verify( updater ).process( add( 10, "foo" ) );
        order.verify( updater ).close();
    }

    @Test
    void shouldBringConstraintIndexOnlineWhenExplicitlyToldTo() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        IndexDescriptor index = constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        indexingService.createIndexes( AUTH_DISABLED, index );
        IndexProxy proxy = indexingService.getIndexProxy( index );

        indexingService.activateIndex( index );

        // then
        assertEquals( ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).close( eq( true ), any( CursorContext.class ) );
    }

    @Test
    void shouldLogIndexStateOnInit() throws Exception
    {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor( PROVIDER_DESCRIPTOR );
        Config config = Config.defaults( default_schema_provider, PROVIDER_DESCRIPTOR.name() );
        IndexProviderMap providerMap = life.add( new MockIndexProviderMap( provider ) );

        IndexDescriptor onlineIndex     = storeIndex( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexDescriptor populatingIndex = storeIndex( 2, 1, 2, PROVIDER_DESCRIPTOR );
        IndexDescriptor failedIndex     = storeIndex( 3, 2, 2, PROVIDER_DESCRIPTOR );

        life.add( IndexingServiceFactory.createIndexingService( config, mock( JobScheduler.class ), providerMap,
                mock( IndexStoreViewFactory.class ), nameLookup, asList( onlineIndex, populatingIndex, failedIndex ),
                internalLogProvider, userLogProvider, IndexMonitor.NO_MONITOR, schemaState, indexStatisticsStore, PageCacheTracer.NULL, INSTANCE, "",
                writable() ) );

        when( provider.getInitialState( eq( onlineIndex ), any() ) ).thenReturn( ONLINE );
        when( provider.getInitialState( eq( populatingIndex ), any() ) ).thenReturn( POPULATING );
        when( provider.getInitialState( eq( failedIndex ), any() ) ).thenReturn( FAILED );

        nameLookup.label( 1, "LabelOne" );
        nameLookup.label( 2, "LabelTwo" );
        nameLookup.propertyKey( 1, "propertyOne" );
        nameLookup.propertyKey( 2, "propertyTwo" );

        // when
        life.init();

        // then
        assertThat( internalLogProvider ).forLevel( DEBUG ).containsMessages(
                "IndexingService.init: index 1 on (:LabelOne {propertyOne}) is ONLINE",
                "IndexingService.init: index 2 on (:LabelOne {propertyTwo}) is POPULATING",
                "IndexingService.init: index 3 on (:LabelTwo {propertyTwo}) is FAILED" );
    }

    @Test
    void shouldLogIndexStateOnStart() throws Throwable
    {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor( PROVIDER_DESCRIPTOR );
        Config config = Config.defaults( default_schema_provider, PROVIDER_DESCRIPTOR.name() );
        MockIndexProviderMap providerMap = new MockIndexProviderMap( provider );
        providerMap.init();

        IndexDescriptor onlineIndex     = storeIndex( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexDescriptor populatingIndex = storeIndex( 2, 1, 2, PROVIDER_DESCRIPTOR );
        IndexDescriptor failedIndex     = storeIndex( 3, 2, 2, PROVIDER_DESCRIPTOR );

        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config,
                mock( JobScheduler.class ), providerMap, storeViewFactory, nameLookup,
                asList( onlineIndex, populatingIndex, failedIndex ), internalLogProvider, userLogProvider, IndexMonitor.NO_MONITOR,
                schemaState, indexStatisticsStore, PageCacheTracer.NULL, INSTANCE, "", writable() );

        when( provider.getInitialState( eq( onlineIndex ), any() ) ).thenReturn( ONLINE );
        when( provider.getInitialState( eq( populatingIndex ), any() ) ).thenReturn( POPULATING );
        when( provider.getInitialState( eq( failedIndex ), any() ) ).thenReturn( FAILED );

        indexingService.init();

        nameLookup.label( 1, "LabelOne" );
        nameLookup.label( 2, "LabelTwo" );
        nameLookup.propertyKey( 1, "propertyOne" );
        nameLookup.propertyKey( 2, "propertyTwo" );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100L, 32L, 32L ) );

        internalLogProvider.clear();

        // when
        indexingService.start();

        // then
        verify( provider ).getPopulationFailure( eq( failedIndex ), any() );
        assertThat( internalLogProvider ).forLevel( DEBUG ).containsMessages(
                "IndexingService.start: index 1 on (:LabelOne {propertyOne}) is ONLINE",
                "IndexingService.start: index 2 on (:LabelOne {propertyTwo}) is POPULATING",
                "IndexingService.start: index 3 on (:LabelTwo {propertyTwo}) is FAILED" );
    }

    @Test
    void shouldFailToStartIfMissingIndexProvider() throws Exception
    {
        // GIVEN an indexing service that has a schema index provider X
        String otherProviderKey = "something-completely-different";
        IndexProviderDescriptor otherDescriptor = new IndexProviderDescriptor( otherProviderKey, "no-version" );
        IndexDescriptor rule = storeIndex( 1, 2, 3, otherDescriptor );
        newIndexingServiceWithMockedDependencies(
                mock( IndexPopulator.class ), mock( IndexAccessor.class ),
                new DataUpdates(), rule );

        // WHEN trying to start up and initialize it with an index from provider Y
        assertThatThrownBy( life::init ).isInstanceOf( LifecycleException.class )
                                        .hasMessageContaining( "lookup by descriptor failed" );
    }

    @Test
    void shouldSnapshotOnlineIndexes() throws Exception
    {
        // GIVEN
        int indexId = 1;
        int indexId2 = 2;
        IndexDescriptor rule1 = storeIndex( indexId, 2, 3, PROVIDER_DESCRIPTOR );
        IndexDescriptor rule2 = storeIndex( indexId2, 4, 5, PROVIDER_DESCRIPTOR );

        IndexAccessor indexAccessor = mock( IndexAccessor.class );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                mock( IndexPopulator.class ), indexAccessor,
                new DataUpdates( ), rule1, rule2 );
        Path theFile = Path.of( "Blah" );

        when( indexAccessor.snapshotFiles()).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( rule1, NULL ) ).thenReturn( ONLINE );
        when( indexProvider.getInitialState( rule2, NULL ) ).thenReturn( ONLINE );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100L, 32L, 32L ) );

        life.start();

        // WHEN
        ResourceIterator<Path> files = indexing.snapshotIndexFiles();

        // THEN
        // We get a snapshot per online index
        assertThat( asCollection( files ) ).isEqualTo( asCollection( iterator( theFile, theFile ) ) );
    }

    @Test
    void shouldNotSnapshotPopulatingIndexes() throws Exception
    {
        // GIVEN
        CountDownLatch populatorLatch = new CountDownLatch( 1 );
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        int indexId = 1;
        int indexId2 = 2;
        IndexDescriptor index1 = storeIndex( indexId, 2, 3, PROVIDER_DESCRIPTOR );
        IndexDescriptor index2 = storeIndex( indexId2, 4, 5, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, indexAccessor,
                new DataUpdates(), index1, index2 );
        Path theFile = Path.of( "Blah" );

        doAnswer( waitForLatch( populatorLatch ) ).when( populator ).create();
        when( indexAccessor.snapshotFiles() ).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( eq( index1 ), any() ) ).thenReturn( POPULATING );
        when( indexProvider.getInitialState( eq( index2 ), any() ) ).thenReturn( ONLINE );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100, 32, 32 ) );
        life.start();

        // WHEN
        ResourceIterator<Path> files = indexing.snapshotIndexFiles();
        populatorLatch.countDown(); // only now, after the snapshot, is the population job allowed to finish
        waitForIndexesToComeOnline( indexing, index1, index2 );

        // THEN
        // We get a snapshot from the online index, but no snapshot from the populating one
        assertThat( asCollection( files ) ).isEqualTo( asCollection( iterator( theFile ) ) );
    }

    @Test
    void shouldIgnoreActivateCallDuringRecovery() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        IndexDescriptor index = forSchema( forLabel( 0, 0 ) ).withIndexProvider( PROVIDER_DESCRIPTOR ).withName( "index" ).materialise( 0 );

        // when
        indexingService.activateIndex( index );

        // then no exception should be thrown.
    }

    @Test
    void shouldLogTriggerSamplingOnAllIndexes() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        IndexSamplingMode mode = backgroundRebuildAll();

        // when
        indexingService.triggerIndexSampling( mode );

        // then
        assertThat( internalLogProvider ).forLevel( INFO )
                .containsMessages( "Manual trigger for sampling all indexes [" + mode + "]" );
    }

    @Test
    void shouldLogTriggerSamplingOnAnIndexes() throws Exception
    {
        // given
        long indexId = 0;
        IndexSamplingMode mode = backgroundRebuildAll();
        IndexPrototype prototype = forSchema( forLabel( 0, 1 ) ).withIndexProvider( PROVIDER_DESCRIPTOR ).withName( "index" );
        IndexDescriptor index = prototype.materialise( indexId );
        when( accessor.newValueReader() ).thenReturn( ValueIndexReader.EMPTY );
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), index );
        life.init();
        life.start();

        // when
        indexingService.triggerIndexSampling( index , mode );

        // then
        String userDescription = index.userDescription( nameLookup );
        assertThat( internalLogProvider ).forLevel( INFO )
                .containsMessages( "Manual trigger for sampling index " + userDescription + " [" + mode + "]" );
    }

    @Test
    void applicationOfIndexUpdatesShouldThrowIfServiceIsShutdown() throws IOException
    {
        // Given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();
        life.shutdown();

        var e = assertThrows( IllegalStateException.class, () -> indexingService.applyUpdates( asSet( add( 1, "foo" ) ) ,NULL ) );
        assertThat( e.getMessage() ).startsWith( "Can't apply index updates" );
    }

    @Test
    void applicationOfUpdatesShouldFlush() throws Exception
    {
        // Given
        when( accessor.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater );
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();

        indexing.createIndexes( AUTH_DISABLED, index );
        waitForIndexesToComeOnline( indexing, index );
        verify( populator, timeout( 10000 ) ).close( eq( true ), any() );

        // When
        indexing.applyUpdates( asList( add( 1, "foo" ), add( 2, "bar" ) ), NULL );

        // Then
        InOrder inOrder = inOrder( updater );
        inOrder.verify( updater ).process( add( 1, "foo" ) );
        inOrder.verify( updater ).process( add( 2, "bar" ) );
        inOrder.verify( updater ).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closingOfValidatedUpdatesShouldCloseUpdaters() throws Exception
    {
        // Given
        long indexId1 = 1;
        long indexId2 = 2;

        int labelId1 = 24;
        int labelId2 = 42;

        IndexDescriptor index1 = storeIndex( indexId1, labelId1, propertyKeyId, PROVIDER_DESCRIPTOR );
        IndexDescriptor index2 = storeIndex( indexId2, labelId2, propertyKeyId, PROVIDER_DESCRIPTOR );

        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        IndexAccessor accessor1 = mock( IndexAccessor.class );
        IndexUpdater updater1 = mock( IndexUpdater.class );
        when( accessor1.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater1 );

        IndexAccessor accessor2 = mock( IndexAccessor.class );
        IndexUpdater updater2 = mock( IndexUpdater.class );
        when( accessor2.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater2 );

        when( indexProvider.getOnlineAccessor( eq( index1 ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) ).thenReturn( accessor1 );
        when( indexProvider.getOnlineAccessor( eq( index2 ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) ).thenReturn( accessor2 );

        life.start();

        indexing.createIndexes( AUTH_DISABLED, index1 );
        indexing.createIndexes( AUTH_DISABLED, index2 );

        waitForIndexesToComeOnline( indexing, index1, index2 );

        verify( populator, timeout( 10000 ).times( 2 ) ).close( eq( true ), any() );

        // When
        indexing.applyUpdates( asList(
                add( 1, "foo", index1 ),
                add( 2, "bar", index2 ) ), NULL );

        // Then
        verify( updater1 ).close();
        verify( updater2 ).close();
    }

    private static void waitForIndexesToComeOnline( IndexingService indexing, IndexDescriptor... index )
            throws IndexNotFoundKernelException
    {
        waitForIndexesToGetIntoState( indexing, ONLINE, index );
    }

    private static void waitForIndexesToGetIntoState( IndexingService indexing, InternalIndexState state,
            IndexDescriptor... indexes )
            throws IndexNotFoundKernelException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 30 );
        while ( !allInState( indexing, state, indexes ) )
        {
            if ( currentTimeMillis() > end )
            {
                fail( "Indexes couldn't come online" );
            }
        }
    }

    private static boolean allInState( IndexingService indexing, InternalIndexState state,
            IndexDescriptor[] indexes ) throws IndexNotFoundKernelException
    {
        for ( IndexDescriptor index : indexes )
        {
            if ( indexing.getIndexProxy( index ).getState() != state )
            {
                return false;
            }
        }
        return true;
    }

    private Iterable<IndexEntryUpdate<IndexDescriptor>> nodeIdsAsIndexUpdates( long... nodeIds )
    {
        return () ->
        {
            List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
            for ( long nodeId : nodeIds )
            {
                updates.add( IndexEntryUpdate.add( nodeId, index, Values.of( 1 ) ) );
            }
            return updates.iterator();
        };
    }

    /*
     * See comments in IndexingService#createIndex
     */
    @Test
    void shouldNotLoseIndexDescriptorDueToOtherSimilarIndexDuringRecovery() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        long otherIndexId = 2;
        Update update = addNodeUpdate( nodeId, "value" );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100, 42, 42 ) );
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy( new TrackingIndexAccessor() );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData( update ), index
        );
        when( indexProvider.getInitialState( index, NULL ) ).thenReturn( ONLINE );
        life.init();

        // WHEN dropping another index, which happens to have the same label/property... while recovering
        IndexDescriptor otherIndex = prototype.withName( "index_" + otherIndexId ).materialise( otherIndexId );
        indexing.createIndexes( AUTH_DISABLED, otherIndex );
        indexing.dropIndex( otherIndex );
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndexes( AUTH_DISABLED, index );
        reset( accessor );
        indexing.applyUpdates( nodeIdsAsIndexUpdates( nodeId ), NULL );
        // and WHEN starting, i.e. completing recovery
        life.start();

        verify( accessor ).newUpdater( eq( RECOVERY ), any( CursorContext.class ) );
    }

    @Test
    void shouldNotLoseIndexDescriptorDueToOtherVerySimilarIndexDuringRecovery() throws Exception
    {
        // GIVEN
        AtomicReference<BinaryLatch> populationStartLatch = latchedIndexPopulation();
        long nodeId = 0;
        Update update = addNodeUpdate( nodeId, "value" );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100, 42, 42 ) );
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy( new TrackingIndexAccessor() );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData( update ), index
        );
        when( indexProvider.getInitialState( index, NULL ) ).thenReturn( ONLINE );
        life.init();
        populationStartLatch.getAndSet( new BinaryLatch() ).release();

        // WHEN dropping another index, which happens to be identical to the existing one except for different index config... while recovering
        IndexConfig indexConfig = index.getIndexConfig().withIfAbsent( "a", Values.booleanValue( true ) );
        IndexDescriptor otherIndex = index.withIndexConfig( indexConfig );
        indexing.createIndexes( AUTH_DISABLED, otherIndex );
        indexing.dropIndex( otherIndex );
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndexes( AUTH_DISABLED, index );
        reset( accessor );
        indexing.applyUpdates( nodeIdsAsIndexUpdates( nodeId ), NULL );
        // and WHEN starting, i.e. completing recovery
        life.start();

        IndexProxy indexProxy = indexing.getIndexProxy( index );
        try
        {
            assertNull( indexProxy.getDescriptor().getIndexConfig().get( "a" ) );
            assertThat( indexProxy.getState() ).isEqualTo( POPULATING ); // The existing online index got nuked during recovery.
        }
        finally
        {
            populationStartLatch.get().release();
        }
    }

    @Test
    void shouldWaitForRecoveredUniquenessConstraintIndexesToBeFullyPopulated() throws Exception
    {
        // I.e. when a uniqueness constraint is created, but database crashes before that schema record
        // ends up in the store, so that next start have no choice but to rebuild it.

        // GIVEN
        DoubleLatch latch = new DoubleLatch();
        ControlledIndexPopulator populator = new ControlledIndexPopulator( latch );
        AtomicReference<IndexDescriptor> indexRef = new AtomicReference<>();
        IndexMonitor monitor = new IndexMonitor.MonitorAdapter()
        {
            @Override
            public void awaitingPopulationOfRecoveredIndex( IndexDescriptor descriptor )
            {
                // When we see that we start to await the index to populate, notify the slow-as-heck
                // populator that it can actually go and complete its job.
                indexRef.set( descriptor );
                latch.startAndWaitForAllToStart();
            }
        };
        // leaving out the IndexRule here will have the index being populated from scratch
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor,
                withData( addNodeUpdate( 0, "value", 1 ) ), monitor );

        // WHEN initializing, i.e. preparing for recovery
        life.init();
        // simulating an index being created as part of applying recovered transactions
        long fakeOwningConstraintRuleId = 1;
        indexing.createIndexes( AUTH_DISABLED, constraintIndexRule( 2, labelId, propertyKeyId, PROVIDER_DESCRIPTOR,
                fakeOwningConstraintRuleId ) );
        // and then starting, i.e. considering recovery completed
        life.start();

        // THEN afterwards the index should be ONLINE
        assertEquals( 2, indexRef.get().getId() );
        assertEquals( InternalIndexState.ONLINE, indexing.getIndexProxy( indexRef.get() ).getState() );
    }

    @Test
    void shouldCreateMultipleIndexesInOneCall() throws Exception
    {
        // GIVEN
        IndexMonitor monitor = IndexMonitor.NO_MONITOR;
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor,
                withData( addNodeUpdate( 0, "value", 1 ) ), monitor );
        life.start();

        // WHEN
        IndexDescriptor index1 = storeIndex( 0, 0, 0, PROVIDER_DESCRIPTOR );
        IndexDescriptor index2 = storeIndex( 1, 0, 1, PROVIDER_DESCRIPTOR );
        IndexDescriptor index3 = storeIndex( 2, 1, 0, PROVIDER_DESCRIPTOR );
        indexing.createIndexes( AUTH_DISABLED, index1, index2, index3 );

        // THEN
        IndexPrototype prototype = forSchema( forLabel( 0, 0 ) ).withIndexProvider( PROVIDER_DESCRIPTOR );
        verify( indexProvider ).getPopulator( eq( prototype.withName( "index_0" ).materialise( 0 ) ),
                any( IndexSamplingConfig.class ), any(), any(), any( TokenNameLookup.class ) );
        verify( indexProvider ).getPopulator( eq( prototype.withSchemaDescriptor( forLabel( 0, 1 ) ).withName( "index_1" ).materialise( 1 ) ),
                any( IndexSamplingConfig.class ), any(), any(), any( TokenNameLookup.class ) );
        verify( indexProvider ).getPopulator( eq( prototype.withSchemaDescriptor( forLabel( 1, 0 ) ).withName( "index_2" ).materialise( 2 ) ),
                any( IndexSamplingConfig.class ), any(), any(), any( TokenNameLookup.class ) );

        waitForIndexesToComeOnline( indexing, index1, index2, index3 );
    }

    @Test
    void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterPopulating() throws Exception
    {
        // given
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        IOException exception = new IOException( "Expected failure" );
        nameLookup.label( labelId, "TheLabel" );
        nameLookup.propertyKey( propertyKeyId, "propertyKey" );

        when( indexProvider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        indexing.createIndexes( AUTH_DISABLED , index );
        waitForIndexesToGetIntoState( indexing, FAILED, index );
        verify( populator, timeout( 10000 ).times( 2 ) ).close( closeArgs.capture(), any() );

        // then
        assertEquals( FAILED, indexing.getIndexProxy( index ).getState() );
        assertEquals( asList( true, false ), closeArgs.getAllValues() );
        assertThat( storedFailure() ).contains( format( "java.io.IOException: Expected failure%n\tat " ) );
        assertThat( internalLogProvider ).forClass( IndexPopulationJob.class ).forLevel( ERROR ).assertExceptionForLogMessage(
                "Failed to populate index: [Index( id=0, name='index', type='GENERAL BTREE', schema=(:TheLabel {propertyKey}), " +
                        "indexProvider='quantum-dex-25.0' )]" )
                .hasRootCause( exception );
        assertThat( internalLogProvider ).forClass( IndexPopulationJob.class ).forLevel( INFO )
                .doesNotContainMessageWithArguments( "Index population completed. Index is now online: [%s]",
                "Index( id=0, name='index', type='GENERAL BTREE', schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )" );
    }

    @Test
    void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterRecoveringPopulatingIndex() throws Exception
    {
        // given
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), index );

        IOException exception = new IOException( "Expected failure" );
        nameLookup.label( labelId, "TheLabel" );
        nameLookup.propertyKey( propertyKeyId, "propertyKey" );

        when( indexProvider.getInitialState( index, NULL ) ).thenReturn( POPULATING );
        when( indexProvider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        waitForIndexesToGetIntoState( indexing, FAILED, index );
        verify( populator, timeout( 10000 ).times( 2 ) ).close( closeArgs.capture(), any() );

        // then
        assertEquals( FAILED, indexing.getIndexProxy( index ).getState() );
        assertEquals( asList( true, false ), closeArgs.getAllValues() );
        assertThat( storedFailure() ).contains( format( "java.io.IOException: Expected failure%n\tat " ) );
        assertThat( internalLogProvider ).forClass( IndexPopulationJob.class ).forLevel( ERROR )
                .assertExceptionForLogMessage( "Failed to populate index: [Index( id=0, name='index', type='GENERAL BTREE', " +
                                "schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )]" )
                .hasRootCause( exception );
        assertThat( internalLogProvider ).forClass( IndexPopulationJob.class ).forLevel( INFO )
                .doesNotContainMessageWithArguments( "Index population completed. Index is now online: [%s]",
                "Index( id=0, name='index', type='GENERAL BTREE', schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )" );
    }

    @Test
    void constraintIndexesWithoutConstraintsMustGetPopulatingProxies() throws Exception
    {
        // given
        AtomicReference<BinaryLatch> populationStartLatch = latchedIndexPopulation();
        try
        {
            long indexId = 1;
            IndexDescriptor index = uniqueIndex.materialise( indexId ); // Note the lack of an "owned constraint id".
            IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), index );
            when( indexProvider.getInitialState( eq( index ), any() ) ).thenReturn( POPULATING );

            // when
            life.start();

            // then
            assertEquals( POPULATING, indexing.getIndexProxy( index ).getState() );
        }
        finally
        {
            populationStartLatch.get().release();
        }
    }

    @Test
    void shouldReportCauseOfPopulationFailureIfPopulationFailsDuringRecovery() throws Exception
    {
        // given
        long indexId = 1;
        long constraintId = 2;
        IndexDescriptor indexRule = uniqueIndex.materialise( indexId ).withOwningConstraintId( constraintId );
        Barrier.Control barrier = new Barrier.Control();
        CountDownLatch exceptionBarrier = new CountDownLatch( 1 );
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), new IndexMonitor.MonitorAdapter()
        {
            @Override
            public void awaitingPopulationOfRecoveredIndex( IndexDescriptor descriptor )
            {
                barrier.reached();
            }
        }, indexRule );
        when( indexProvider.getInitialState( eq( indexRule ), any() ) ).thenReturn( POPULATING );

        life.init();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            executor.submit( () ->
            {
                try
                {
                    life.start();
                }
                finally
                {
                    exceptionBarrier.countDown();
                }
            } );

            // Thread is just about to start checking index status. We flip to failed proxy to indicate population failure during recovery.
            barrier.await();
            // Wait for the index to come online, otherwise we'll race the failed flip below with its flip and sometimes the POPULATING -> ONLINE
            // flip will win and make the index NOT fail and therefor hanging this test awaiting on the exceptionBarrier below
            waitForIndexesToComeOnline( indexing, indexRule );
            IndexProxy indexProxy = indexing.getIndexProxy( indexRule );
            assertThat( indexProxy ).isInstanceOf( ContractCheckingIndexProxy.class );
            ContractCheckingIndexProxy contractCheckingIndexProxy = (ContractCheckingIndexProxy) indexProxy;
            IndexProxy delegate = contractCheckingIndexProxy.getDelegate();
            assertThat( delegate ).isInstanceOf( FlippableIndexProxy.class );
            FlippableIndexProxy flippableIndexProxy = (FlippableIndexProxy) delegate;
            Exception expectedCause = new Exception( "index was failed on purpose" );
            IndexPopulationFailure indexFailure = IndexPopulationFailure.failure( expectedCause );

            flippableIndexProxy.flipTo(
                    new FailedIndexProxy( new ValueIndexProxyStrategy( indexRule, mock( IndexStatisticsStore.class ), nameLookup ),
                            mock( IndexPopulator.class ),
                            indexFailure,
                            internalLogProvider )
            );
            barrier.release();
            exceptionBarrier.await();

            assertThat( internalLogProvider ).containsMessages( expectedCause.getMessage() )
                    .containsMessages( format( "Index %s entered %s state ", indexRule, FAILED ) );
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    void shouldLogIndexStateOutliersOnInit() throws Exception
    {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor( PROVIDER_DESCRIPTOR );
        Config config = Config.defaults( default_schema_provider, PROVIDER_DESCRIPTOR.name() );
        IndexProviderMap providerMap = life.add( new MockIndexProviderMap( provider ) );

        List<IndexDescriptor> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexDescriptor populatingIndex = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( eq( populatingIndex ), any() ) ).thenReturn( POPULATING );
        indexes.add( populatingIndex );
        IndexDescriptor failedIndex = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( eq( failedIndex ), any() ) ).thenReturn( FAILED );
        indexes.add( failedIndex );
        for ( int i = 0; i < 10; i++ )
        {
            IndexDescriptor indexRule = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
            when( provider.getInitialState( eq( indexRule ), any() ) ).thenReturn( ONLINE );
            indexes.add( indexRule );
        }
        for ( int i = 0; i < nextIndexId; i++ )
        {
            nameLookup.label( i, "Label" + i );
        }

        life.add( IndexingServiceFactory.createIndexingService( config, mock( JobScheduler.class ), providerMap,
                mock( IndexStoreViewFactory.class ), nameLookup, indexes, internalLogProvider, userLogProvider, IndexMonitor.NO_MONITOR,
                schemaState, indexStatisticsStore, PageCacheTracer.NULL, INSTANCE, "", writable() ) );

        nameLookup.propertyKey( 1, "prop" );

        // when
        life.init();

        // then
        assertThat( internalLogProvider ).forLevel( INFO )
                .containsMessages(
                        "IndexingService.init: index 1 on (:Label1 {prop}) is POPULATING",
                        "IndexingService.init: index 2 on (:Label2 {prop}) is FAILED",
                        "IndexingService.init: indexes not specifically mentioned above are ONLINE" )
                .doesNotContainMessage( "IndexingService.init: index 3 on :Label3(prop) is ONLINE" );
    }

    @Test
    void shouldLogIndexStateOutliersOnStart() throws Throwable
    {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor( PROVIDER_DESCRIPTOR );
        Config config = Config.defaults( default_schema_provider, PROVIDER_DESCRIPTOR.name() );
        MockIndexProviderMap providerMap = new MockIndexProviderMap( provider );
        providerMap.init();

        List<IndexDescriptor> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexDescriptor populatingIndex = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( eq( populatingIndex ), any() ) ).thenReturn( POPULATING );
        indexes.add( populatingIndex );
        IndexDescriptor failedIndex = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( eq( failedIndex ), any() ) ).thenReturn( FAILED );
        indexes.add( failedIndex );
        for ( int i = 0; i < 10; i++ )
        {
            IndexDescriptor indexRule = storeIndex( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
            when( provider.getInitialState( eq( indexRule ), any() ) ).thenReturn( ONLINE );
            indexes.add( indexRule );
        }
        for ( int i = 0; i < nextIndexId; i++ )
        {
            nameLookup.label( i, "Label" + i );
        }

        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config,
                mock( JobScheduler.class ), providerMap, storeViewFactory, nameLookup, indexes,
                internalLogProvider, userLogProvider, IndexMonitor.NO_MONITOR, schemaState, indexStatisticsStore, PageCacheTracer.NULL, INSTANCE, "",
                writable() );
        when( indexStatisticsStore.indexSample( anyLong() ) ).thenReturn( new IndexSample( 100, 32, 32 ) );
        nameLookup.propertyKey( 1, "prop" );

        // when
        indexingService.init();
        internalLogProvider.clear();
        indexingService.start();

        // then
        assertThat( internalLogProvider ).forLevel( INFO )
                .containsMessages(
                        "IndexingService.start: index 1 on (:Label1 {prop}) is POPULATING",
                        "IndexingService.start: index 2 on (:Label2 {prop}) is FAILED",
                        "IndexingService.start: indexes not specifically mentioned above are ONLINE" )
                .doesNotContainMessage( "IndexingService.start: index 3 on :Label3(prop) is ONLINE" );
    }

    @Test
    void flushAllIndexesWhileSomeOfThemDropped() throws IOException
    {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxy validIndex1 = createIndexProxyMock(1);
        IndexProxy validIndex2 = createIndexProxyMock(2);
        IndexProxy deletedIndexProxy = createIndexProxyMock(3);
        IndexProxy validIndex3 = createIndexProxyMock(4);
        IndexProxy validIndex4 = createIndexProxyMock(5);
        indexMapReference.modify( indexMap ->
        {
            indexMap.putIndexProxy( validIndex1 );
            indexMap.putIndexProxy( validIndex2 );
            indexMap.putIndexProxy( deletedIndexProxy );
            indexMap.putIndexProxy( validIndex3 );
            indexMap.putIndexProxy( validIndex4 );
            return indexMap;
        } );

        doAnswer( invocation ->
        {
            indexMapReference.modify( indexMap ->
            {
                indexMap.removeIndexProxy( 3 );
                return indexMap;
            } );
            throw new RuntimeException( "Index deleted." );
        } ).when( deletedIndexProxy ).force( any( CursorContext.class ) );

        IndexingService indexingService = createIndexServiceWithCustomIndexMap( indexMapReference );

        indexingService.forceAll( NULL );
        verify( validIndex1 ).force( NULL );
        verify( validIndex2 ).force( NULL );
        verify( validIndex3 ).force( NULL );
        verify( validIndex4 ).force( NULL );
    }

    @Test
    void failForceAllWhenOneOfTheIndexesFailToForce() throws IOException
    {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxy strangeIndexProxy = createIndexProxyMock( 1 );
        doThrow( new UncheckedIOException( new IOException( "Can't force" ) ) ).when( strangeIndexProxy )
                .force( any( CursorContext.class ) );
        indexMapReference.modify( indexMap ->
        {
            IndexProxy validIndex = createIndexProxyMock( 0 );
            indexMap.putIndexProxy( validIndex );
            indexMap.putIndexProxy( validIndex );
            indexMap.putIndexProxy( strangeIndexProxy );
            indexMap.putIndexProxy( validIndex );
            indexMap.putIndexProxy( validIndex );
            return indexMap;
        } );

        IndexingService indexingService = createIndexServiceWithCustomIndexMap( indexMapReference );

        var e = assertThrows( UnderlyingStorageException.class,
                () -> indexingService.forceAll( NULL ) );
        assertThat( e.getMessage() ).startsWith( "Unable to force" );
    }

    @Test
    void shouldRefreshIndexesOnStart() throws Exception
    {
        // given
        newIndexingServiceWithMockedDependencies( populator, accessor, withData(), index );

        IndexAccessor accessor = mock( IndexAccessor.class );
        IndexUpdater updater = mock( IndexUpdater.class );
        when( accessor.newValueReader() ).thenReturn( ValueIndexReader.EMPTY );
        when( accessor.newUpdater( any( IndexUpdateMode.class ), any( CursorContext.class ) ) ).thenReturn( updater );
        when( indexProvider.getOnlineAccessor( any( IndexDescriptor.class ),
                any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) ).thenReturn( accessor );

        life.init();

        verify( accessor, never() ).refresh();

        life.start();

        // Then
        verify( accessor ).refresh();
    }

    @Test
    void shouldNotHaveToWaitForOrphanedUniquenessIndexInRecovery() throws Exception
    {
        // given that we have a uniqueness index that needs to be recovered and that doesn't have a constraint attached to it
        IndexDescriptor descriptor = uniqueIndex.materialise( 10 );
        Iterable<IndexDescriptor> schemaRules = Collections.singletonList( descriptor );
        IndexProvider indexProvider = mock( IndexProvider.class );
        when( indexProvider.getInitialState( any(), any() ) ).thenReturn( POPULATING );
        IndexProviderMap indexProviderMap = mock( IndexProviderMap.class );
        when( indexProviderMap.lookup( anyString() ) ).thenReturn( indexProvider );
        when( indexProviderMap.lookup( any( IndexProviderDescriptor.class ) ) ).thenReturn( indexProvider );
        when( indexProviderMap.getDefaultProvider() ).thenReturn( indexProvider );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxyCreator indexProxyCreator = mock( IndexProxyCreator.class );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( descriptor );
        // Eventually return ONLINE so that this test won't hang if the product code changes in this regard.
        // This test should still fail below when verifying interactions with the proxy and monitor tho.
        when( indexProxy.getState() ).thenReturn( POPULATING, POPULATING, POPULATING, POPULATING, ONLINE );
        when( indexProxyCreator.createRecoveringIndexProxy( any() ) ).thenReturn( indexProxy );
        when( indexProxyCreator.createFailedIndexProxy( any(), any() ) ).thenReturn( indexProxy );
        when( indexProxyCreator.createPopulatingIndexProxy( any(), any(), any() ) ).thenReturn( indexProxy );
        JobScheduler scheduler = mock( JobScheduler.class );
        IndexSamplingController samplingController = mock( IndexSamplingController.class );
        IndexMonitor monitor = mock( IndexMonitor.class );
        IndexStoreViewFactory storeViewFactory = mock( IndexStoreViewFactory.class );
        when( storeViewFactory.createTokenIndexStoreView( any() ) ).thenReturn( storeView );
        when( storeView.newPropertyAccessor( any(), any() ) ).thenReturn( propertyAccessor );
        IndexingService indexingService = new IndexingService(
                indexProxyCreator, indexProviderMap, indexMapReference, storeViewFactory, schemaRules,
                samplingController, nameLookup, scheduler, null, logProvider, logProvider, monitor, mock( IndexStatisticsStore.class ),
                PageCacheTracer.NULL, INSTANCE, "", writable(), Config.defaults() );
        // and where index population starts
        indexingService.init();

        // when starting the indexing service
        indexingService.start();

        // then it should be able to start without awaiting the completion of the population of the index
        verify( indexProxy, never() ).awaitStoreScanCompleted( anyLong(), any() );
        verify( monitor, never() ).awaitingPopulationOfRecoveredIndex( any() );
    }

    @Test
    public void shouldIncrementIndexUpdatesAfterStartingExistingOnlineIndexProxy() throws Exception
    {
        // given
        long indexId = 10;
        IndexDescriptor indexDescriptor = uniqueIndex.materialise( indexId );
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), indexDescriptor );
        life.start();

        // when
        IndexProxy proxy = indexingService.getIndexProxy( indexDescriptor );
        proxy.awaitStoreScanCompleted( 1, HOURS );
        proxy.activate();
        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            updater.process( IndexEntryUpdate.add( 123, indexDescriptor, stringValue( "some value" ) ) );
        }

        // then
        verify( indexStatisticsStore ).incrementIndexUpdates( indexId, 1L );
    }

    @Test
    public void shouldDropAndCreateIndexWithSameIdDuringRecovery() throws IOException
    {
        // given
        long indexId = 10;
        IndexDescriptor indexDescriptor = uniqueIndex.materialise( indexId );
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), indexDescriptor );
        life.init();

        // when
        indexingService.dropIndex( indexDescriptor );
        indexingService.createIndexes( AUTH_DISABLED, indexDescriptor );
        life.start();

        // then
        verify( accessor ).drop();
    }

    @Test
    void shouldStopBackgroundSampling()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 30 ), () ->
        {
            var localLife = new LifeSupport();
            var indexingService = newIndexingServiceWithMockedDependencies(
                    populator, accessor, withData(), IndexMonitor.NO_MONITOR, localLife );
            localLife.start();
            indexingService.createIndexes( AUTH_DISABLED, index );
            waitForIndexesToComeOnline( indexingService, index );

            IndexSampler neverEndingSampler = ( cursorContext, stopped ) ->
            {
                while ( !stopped.get() )
                {
                    Thread.yield();
                }
                return new IndexSample();
            };
            ValueIndexReader indexReader = mock( ValueIndexReader.class );
            when( indexReader.createSampler() ).thenReturn( neverEndingSampler );
            when( accessor.newValueReader() ).thenReturn( indexReader );

            clearInvocations( indexStatisticsStore );
            indexingService.triggerIndexSampling( backgroundRebuildAll() );
            // shouldn't hang
            localLife.stop();
            // and the index statistics store should not have been updated
            verify( indexStatisticsStore, never() ).replaceStats( anyLong(), any() );
        });
    }

    /*
     * This scenario is a semi-hypothetical scenario and yet believed to have been observed at least once in the wild:
     *
     * - an index (with internal ID X) is created.
     * - another member in the cluster starts a store copy and copies the index but not yet the schema store.
     * - index is deleted and another index (which happens to get the same internal ID X) is created as a
     *   composite index.
     * - the store copy copies the schema store, the rest and completes.
     * - now on that member the schema store says that index X is a composite index whereas the file on disk is a
     *   single-property index.
     * - replaying the transactions that happened during copy would have dropped that index and then creating the new
     *   one, so semantically it would have been fine in the end.
     *
     * The problem was that index X had status ONLINE in the schema store and so IndexingService#init went ahead
     * and just opened it, not knowing that it would open an index not matching the layout that schema store said.
     * There was already a catch for IOException, but not for other unchecked exceptions. The fix was to also
     * catch RuntimeException there.
     */
    @Test
    void shouldHandleOpeningMismatchingIndex() throws Exception
    {
        // given
        when( indexProvider.getInitialState( any( IndexDescriptor.class ), any( CursorContext.class ) ) )
                .thenReturn( ONLINE );
        when( indexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( indexProvider.getPopulator(
                any( IndexDescriptor.class ),
                any( IndexSamplingConfig.class ),
                any(),
                any(),
                any( TokenNameLookup.class ) ) )
                .thenReturn( populator );
        withData().getsProcessedByStoreScanFrom( storeView );
        when( indexProvider.getOnlineAccessor(
                eq( index ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenThrow( new IllegalStateException( "Something unexpectedly wrong with the index here" ) );
        when( indexProvider.storeMigrationParticipant(
                any( FileSystemAbstraction.class ), any( PageCache.class ), any() ) )
                .thenReturn( StoreMigrationParticipant.NOT_PARTICIPATING );
        var providerMap = life.add( new MockIndexProviderMap( indexProvider ) );
        var populationStarted = new AtomicBoolean();
        var populationCompleted = new AtomicBoolean();
        var initialState = new MutableObject<InternalIndexState>();
        var monitor = new IndexMonitor.MonitorAdapter()
        {
            @Override
            public void initialState( String databaseName, IndexDescriptor descriptor, InternalIndexState state )
            {
                if ( descriptor.equals( index ) )
                {
                    initialState.setValue( state );
                }
            }

            @Override
            public void indexPopulationScanStarting( IndexDescriptor[] indexDescriptors )
            {
                populationStarted.set( true );
            }

            @Override
            public void populationCompleteOn( IndexDescriptor descriptor )
            {
                assertThat( descriptor ).isEqualTo( index );
                populationCompleted.set( true );
            }
        };
        var indexingService = life.add( IndexingServiceFactory.createIndexingService(
                Config.defaults(),
                life.add( scheduler ),
                providerMap,
                storeViewFactory,
                nameLookup,
                loop( iterator( index ) ),
                internalLogProvider,
                userLogProvider,
                monitor,
                schemaState,
                indexStatisticsStore,
                PageCacheTracer.NULL,
                INSTANCE,
                "",
                writable() ) );

        // when
        life.init();
        assertThat( populationStarted.get() ).isFalse();
        assertThat( populationCompleted.get() ).isFalse();
        assertThat( initialState.getValue() ).isEqualTo( ONLINE );

        // and when
        when( indexProvider.getOnlineAccessor(
                eq( index ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenReturn( accessor );
        life.start();
        indexingService.getIndexProxy( index ).awaitStoreScanCompleted( 1, MINUTES );

        // then
        assertThat( populationStarted.get() ).isTrue();
        assertThat( populationCompleted.get() ).isTrue();
    }

    private AtomicReference<BinaryLatch> latchedIndexPopulation()
    {
        AtomicReference<BinaryLatch> populationStartLatch = new AtomicReference<>( new BinaryLatch() );
        scheduler.setThreadFactory( Group.INDEX_POPULATION, ( group, parent ) -> new GroupedDaemonThreadFactory( group, parent )
        {
            @Override
            public Thread newThread( Runnable job )
            {
                return super.newThread( () ->
                {
                    populationStartLatch.get().await();
                    job.run();
                } );
            }
        } );
        return populationStartLatch;
    }

    private static IndexProxy createIndexProxyMock( long indexId )
    {
        IndexProxy proxy = mock( IndexProxy.class );
        IndexDescriptor descriptor = storeIndex( indexId, 1, 2, PROVIDER_DESCRIPTOR );
        when( proxy.getDescriptor() ).thenReturn( descriptor );
        return proxy;
    }

    private String storedFailure()
    {
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass( String.class );
        verify( populator ).markAsFailed( reason.capture() );
        return reason.getValue();
    }

    private static class ControlledIndexPopulator extends IndexPopulator.Adapter
    {
        private final DoubleLatch latch;

        ControlledIndexPopulator( DoubleLatch latch )
        {
            this.latch = latch;
        }

        @Override
        public void scanCompleted( PhaseTracker phaseTracker, PopulationWorkScheduler jobScheduler, CursorContext cursorContext )
        {
            latch.waitForAllToStart();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully, CursorContext cursorContext )
        {
            latch.finish();
        }
    }

    private static Answer<Void> waitForLatch( final CountDownLatch latch )
    {
        return invocationOnMock ->
        {
            latch.await();
            return null;
        };
    }

    private static Answer<ResourceIterator<Path>> newResourceIterator( final Path theFile )
    {
        return invocationOnMock -> asResourceIterator( iterator( theFile ) );
    }

    private Update addNodeUpdate( long nodeId, Object propertyValue )
    {
        return addNodeUpdate( nodeId, propertyValue, labelId );
    }

    private Update addNodeUpdate( long nodeId, Object propertyValue, int labelId )
    {
        return new Update( nodeId, new long[]{labelId}, prototype.schema().getPropertyId(), Values.of( propertyValue ) );
    }

    private IndexEntryUpdate<IndexDescriptor> add( long nodeId, Object propertyValue )
    {
        return IndexEntryUpdate.add( nodeId, index, Values.of( propertyValue ) );
    }

    private static IndexEntryUpdate<IndexDescriptor> add( long nodeId, Object propertyValue, IndexDescriptor index )
    {
        return IndexEntryUpdate.add( nodeId, index, Values.of( propertyValue ) );
    }

    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator, IndexAccessor accessor, DataUpdates data, IndexDescriptor... rules ) throws IOException
    {
        return newIndexingServiceWithMockedDependencies( populator, accessor, data, IndexMonitor.NO_MONITOR, rules );
    }

    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator, IndexAccessor accessor, DataUpdates data, IndexMonitor monitor, IndexDescriptor... rules ) throws IOException
    {
        return newIndexingServiceWithMockedDependencies(populator, accessor, data, monitor, life, rules);
    }
    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator, IndexAccessor accessor, DataUpdates data, IndexMonitor monitor,
            LifeSupport providedLife, IndexDescriptor... rules ) throws IOException
    {
        when( indexProvider.getInitialState( any( IndexDescriptor.class ), any( CursorContext.class ) ) ).thenReturn( ONLINE );
        when( indexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( indexProvider.getPopulator( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any(), any(), any( TokenNameLookup.class ) ) )
                .thenReturn( populator );
        data.getsProcessedByStoreScanFrom( storeView );
        when( indexProvider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenReturn( accessor );
        when( indexProvider.storeMigrationParticipant( any( FileSystemAbstraction.class ), any( PageCache.class ), any() ) )
                .thenReturn( StoreMigrationParticipant.NOT_PARTICIPATING );

        Config config = Config.newBuilder()
                .set( default_schema_provider, PROVIDER_DESCRIPTOR.name() ).build();

        MockIndexProviderMap providerMap = providedLife.add( new MockIndexProviderMap( indexProvider ) );
        return providedLife.add( IndexingServiceFactory.createIndexingService( config,
                        providedLife.add( scheduler ), providerMap,
                        storeViewFactory,
                        nameLookup,
                        loop( iterator( rules ) ),
                        internalLogProvider,
                        userLogProvider,
                        monitor,
                        schemaState,
                        indexStatisticsStore,
                        PageCacheTracer.NULL,
                        INSTANCE,
                        "",
                        writable() )
        );
    }

    private static DataUpdates withData( Update... updates )
    {
        return new DataUpdates( Arrays.asList( updates ) );
    }

    private static class DataUpdates implements Answer<StoreScan>
    {
        private final List<Update> updates;

        DataUpdates()
        {
            this.updates = List.of();
        }

        DataUpdates( List<Update> updates )
        {
            this.updates = updates;
        }

        void getsProcessedByStoreScanFrom( IndexStoreView mock )
        {
            when( mock.visitNodes( any(int[].class), any( IntPredicate.class ),
                    any( PropertyScanConsumer.class ), isNull(), anyBoolean(), anyBoolean(), any( PageCacheTracer.class ), any() ) ).thenAnswer( this );
        }

        @Override
        public StoreScan answer( InvocationOnMock invocation )
        {
            final PropertyScanConsumer consumer = invocation.getArgument( 2 );
            return new StoreScan()
            {
                private volatile boolean stop;

                @Override
                public void run( ExternalUpdatesCheck externalUpdatesCheck )
                {
                    if ( stop || updates.isEmpty() )
                    {
                        return;
                    }

                    var batch = consumer.newBatch();
                    updates.forEach( update -> batch.addRecord( update.id, update.labels, Map.of( update.propertyId, update.propertyValue ) ) );
                    batch.process();
                }

                @Override
                public void stop()
                {
                    stop = true;
                }

                @Override
                public PopulationProgress getProgress()
                {
                    return PopulationProgress.single( 42, 100 );
                }
            };
        }

        @Override
        public String toString()
        {
            return updates.toString();
        }
    }

    private static class TrackingIndexAccessor extends IndexAccessor.Adapter
    {
        private final IndexUpdater updater = mock( IndexUpdater.class );

        @Override
        public void drop()
        {
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode, CursorContext cursorContext )
        {
            return updater;
        }

        @Override
        public ValueIndexReader newValueReader()
        {
            throw new UnsupportedOperationException( "Not required" );
        }

        @Override
        public BoundedIterable<Long> newAllEntriesValueReader( long fromIdInclusive, long toIdExclusive, CursorContext cursorContext )
        {
            throw new UnsupportedOperationException( "Not required" );
        }

        @Override
        public ResourceIterator<Path> snapshotFiles()
        {
            throw new UnsupportedOperationException( "Not required" );
        }
    }

    private static IndexDescriptor storeIndex( long ruleId, int labelId, int propertyKeyId, IndexProviderDescriptor providerDescriptor )
    {
        return forSchema( forLabel( labelId, propertyKeyId ) ).withIndexProvider( providerDescriptor ).withName( "index_" + ruleId ).materialise( ruleId );
    }

    private static IndexDescriptor constraintIndexRule( long ruleId, int labelId, int propertyKeyId, IndexProviderDescriptor providerDescriptor )
    {
        return uniqueForSchema( forLabel( labelId, propertyKeyId ) ).withIndexProvider( providerDescriptor )
                .withName( "constraint_" + ruleId ).materialise( ruleId );
    }

    private static IndexDescriptor constraintIndexRule( long ruleId, int labelId, int propertyKeyId, IndexProviderDescriptor providerDescriptor,
            long constraintId )
    {
        return uniqueForSchema( forLabel( labelId, propertyKeyId ) )
                .withIndexProvider( providerDescriptor )
                .withName( "constraint_" + ruleId )
                .materialise( ruleId )
                .withOwningConstraintId( constraintId );
    }

    private IndexingService createIndexServiceWithCustomIndexMap( IndexMapReference indexMapReference )
    {
        return new IndexingService( mock( IndexProxyCreator.class ), mock( IndexProviderMap.class ),
                indexMapReference, mock( IndexStoreViewFactory.class ), Collections.emptyList(),
                mock( IndexSamplingController.class ), nameLookup,
                mock( JobScheduler.class ), mock( SchemaState.class ),
                internalLogProvider, userLogProvider, IndexMonitor.NO_MONITOR, mock( IndexStatisticsStore.class ), PageCacheTracer.NULL, INSTANCE, "",
                writable(), Config.defaults() );
    }

    private static DependencyResolver buildIndexDependencies( IndexProvider... providers )
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( (Object[]) providers );
        dependencies.satisfyDependency( tokenProvider() );
        dependencies.satisfyDependency( textProvider() );
        return dependencies;
    }

    private static IndexProvider mockIndexProviderWithAccessor( IndexProviderDescriptor descriptor ) throws IOException
    {
        IndexProvider provider = mockIndexProvider( descriptor );
        IndexAccessor indexAccessor = mock( IndexAccessor.class );
        when( provider.getOnlineAccessor( any( IndexDescriptor.class ), any( IndexSamplingConfig.class ), any( TokenNameLookup.class ) ) )
                .thenReturn( indexAccessor );
        return provider;
    }

    private static IndexProvider mockIndexProvider( IndexProviderDescriptor descriptor )
    {
        IndexProvider provider = mock( IndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( descriptor );
        return provider;
    }

    private static IndexProvider fulltextProvider()
    {
        return mockIndexProvider( fulltextDescriptor );
    }

    private static IndexProvider tokenProvider()
    {
        return mockIndexProvider( tokenDescriptor );
    }

    private static IndexProvider textProvider()
    {
        return mockIndexProvider( textDescriptor );
    }

    private void onBothLogProviders( Consumer<AssertableLogProvider> logProviderAction )
    {
        logProviderAction.accept( internalLogProvider );
        logProviderAction.accept( userLogProvider );
    }

    private static class Update
    {
        private final long id;
        private final long[] labels;
        private final int propertyId;
        private final Value propertyValue;

        private Update( long id, long[] labels, int propertyId, Value propertyValue )
        {
            this.id = id;
            this.labels = labels;
            this.propertyId = propertyId;
            this.propertyValue = propertyValue;
        }
    }
}
