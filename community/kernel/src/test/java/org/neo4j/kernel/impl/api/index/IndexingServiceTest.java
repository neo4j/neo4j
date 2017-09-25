/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.DirectIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.mockito.answer.AwaitAnswer;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.RECOVERY;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_ALL;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.test.mockito.answer.AwaitAnswer.afterAwaiting;

public class IndexingServiceTest
{
    @Rule
    public final LifeRule life = new LifeRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final LogMatcherBuilder logMatch = inLog( IndexingService.class );
    private final SchemaState schemaState = mock( SchemaState.class );
    private final int labelId = 7;
    private final int propertyKeyId = 15;
    private final IndexDescriptor index = IndexDescriptorFactory.forLabel( labelId, propertyKeyId );
    private final IndexPopulator populator = mock( IndexPopulator.class );
    private final IndexUpdater updater = mock( IndexUpdater.class );
    private final SchemaIndexProvider indexProvider = mock( SchemaIndexProvider.class );
    private final IndexAccessor accessor = mock( IndexAccessor.class, RETURNS_MOCKS );
    private final IndexStoreView storeView  = mock( IndexStoreView.class );
    private final TokenNameLookup nameLookup = mock( TokenNameLookup.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Before
    public void setUp()
    {
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) )
                .thenAnswer( invocation -> invocation.getArguments()[1] );
    }

    @Test
    public void shouldBringIndexOnlineAndFlipOverToIndexAccessor() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn(updater);

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( IndexRule.indexRule( 0, index, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );

        waitForIndexesToComeOnline( indexingService, 0 );
        verify( populator, timeout( 10000 ) ).close( true );

        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( add( 10, "foo" ) );
        }

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).close( true );
        order.verify( accessor ).newUpdater( IndexUpdateMode.RECOVERY );
        order.verify( updater ).process( add( 10, "foo" ) );
        order.verify( updater ).close();
    }

    @Test
    public void indexCreationShouldBeIdempotent() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater );

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( IndexRule.indexRule( 0, index, PROVIDER_DESCRIPTOR ) );
        indexingService.createIndexes( IndexRule.indexRule( 0, index, PROVIDER_DESCRIPTOR ) );

        // We are asserting that the second call to createIndex does not throw an exception.
        waitForIndexesToComeOnline( indexingService, 0 );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldDeliverUpdatesThatOccurDuringPopulationToPopulator() throws Exception
    {
        // given
        when( populator.newPopulatingUpdater( storeView ) ).thenReturn( updater );

        CountDownLatch latch = new CountDownLatch( 1 );
        AwaitAnswer<Void> awaitAnswer = afterAwaiting( latch );
        doAnswer( awaitAnswer ).when( populator ).add( any( Collection.class ) );

        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies( populator, accessor, withData( addNodeUpdate( 1, "value1" ) ) );

        life.start();

        // when
        indexingService.createIndexes( IndexRule.indexRule( 0, index, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );
        assertEquals( InternalIndexState.POPULATING, proxy.getState() );

        IndexEntryUpdate<?> value2 = add( 2, "value2" );
        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( value2 );
        }

        latch.countDown();

        waitForIndexesToComeOnline( indexingService, 0 );
        verify( populator, timeout( 10000 ) ).close( true );

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).includeSample( add( 1, "value1" ) );
        order.verify( populator, times( 2 ) ).add( any( Collection.class ) );

        // invoked from indexAllNodes(), empty because the id we added (2) is bigger than the one we indexed (1)
        //
        // (We don't get an update for value2 here because we mock a fake store that doesn't contain it
        //  just for the purpose of testing this behavior)
        order.verify( populator ).newPopulatingUpdater( storeView );
        order.verify( updater ).close();
        order.verify( populator ).sampleResult();
        order.verify( populator ).close( true );
        verifyNoMoreInteractions( updater );
        verifyNoMoreInteractions( populator );

        verifyZeroInteractions( accessor );
    }

    @Test
    public void shouldStillReportInternalIndexStateAsPopulatingWhenConstraintIndexIsDonePopulating() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn(updater);

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );

        // don't wait for index to come ONLINE here since we're testing that it doesn't
        verify( populator, timeout( 20000 ) ).close( true );

        try ( IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( add( 10, "foo" ) );
        }

        // then
        assertEquals( InternalIndexState.POPULATING, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater );
        order.verify( populator ).create();
        order.verify( populator ).close( true );
        order.verify( accessor ).newUpdater( IndexUpdateMode.ONLINE );
        order.verify(updater).process( add( 10, "foo" ) );
        order.verify(updater).close();
    }

    @Test
    public void shouldBringConstraintIndexOnlineWhenExplicitlyToldTo() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndexes( constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );

        indexingService.activateIndex( 0 );

        // then
        assertEquals( ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor );
        order.verify( populator ).create();
        order.verify( populator ).close( true );
    }

    @Test
    public void shouldLogIndexStateOnInit() throws Exception
    {
        // given
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( provider.getOnlineAccessor( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        TokenNameLookup mockLookup = mock( TokenNameLookup.class );

        IndexRule onlineIndex     = indexRule( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexRule populatingIndex = indexRule( 2, 1, 2, PROVIDER_DESCRIPTOR );
        IndexRule failedIndex     = indexRule( 3, 2, 2, PROVIDER_DESCRIPTOR );

        life.add( IndexingServiceFactory.createIndexingService( Config.defaults(), mock( JobScheduler.class ), providerMap,
                mock( IndexStoreView.class ), mockLookup, asList( onlineIndex, populatingIndex, failedIndex ),
                logProvider, IndexingService.NO_MONITOR, schemaState ) );

        when( provider.getInitialState( onlineIndex.getId(), onlineIndex.getIndexDescriptor() ) )
                .thenReturn( ONLINE );
        when( provider.getInitialState( populatingIndex.getId(), populatingIndex.getIndexDescriptor() ) )
                .thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId(), failedIndex.getIndexDescriptor() ) )
                .thenReturn( InternalIndexState.FAILED );

        when(mockLookup.labelGetName( 1 )).thenReturn( "LabelOne" );
        when( mockLookup.labelGetName( 2 ) ).thenReturn( "LabelTwo" );
        when( mockLookup.propertyKeyGetName( 1 ) ).thenReturn( "propertyOne" );
        when( mockLookup.propertyKeyGetName( 2 ) ).thenReturn( "propertyTwo" );

        // when
        life.init();

        // then
        logProvider.assertAtLeastOnce(
                logMatch.debug( "IndexingService.init: index 1 on :LabelOne(propertyOne) is ONLINE" ),
                logMatch.debug( "IndexingService.init: index 2 on :LabelOne(propertyTwo) is POPULATING" ),
                logMatch.debug( "IndexingService.init: index 3 on :LabelTwo(propertyTwo) is FAILED" )
        );
    }

    @Test
    public void shouldLogIndexStateOnStart() throws Exception
    {
        // given
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        TokenNameLookup mockLookup = mock( TokenNameLookup.class );

        IndexRule onlineIndex     = indexRule( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexRule populatingIndex = indexRule( 2, 1, 2, PROVIDER_DESCRIPTOR );
        IndexRule failedIndex     = indexRule( 3, 2, 2, PROVIDER_DESCRIPTOR );

        IndexingService indexingService = IndexingServiceFactory.createIndexingService( Config.defaults(),
                mock( JobScheduler.class ), providerMap, storeView, mockLookup,
                asList( onlineIndex, populatingIndex, failedIndex ), logProvider, IndexingService.NO_MONITOR,
                schemaState );

        when( provider.getInitialState( onlineIndex.getId(), onlineIndex.getIndexDescriptor() ) )
                .thenReturn( ONLINE );
        when( provider.getInitialState( populatingIndex.getId(), populatingIndex.getIndexDescriptor() ) )
                .thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId(), failedIndex.getIndexDescriptor() ) )
                .thenReturn( InternalIndexState.FAILED );

        indexingService.init();

        when(mockLookup.labelGetName( 1 )).thenReturn( "LabelOne" );
        when(mockLookup.labelGetName( 2 )).thenReturn( "LabelTwo" );
        when(mockLookup.propertyKeyGetName( 1 )).thenReturn( "propertyOne" );
        when(mockLookup.propertyKeyGetName( 2 )).thenReturn( "propertyTwo" );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) ).thenReturn( newDoubleLongRegister( 32L, 32L ) );

        logProvider.clear();

        // when
        indexingService.start();

        // then
        verify( provider ).getPopulationFailure( 3 );
        logProvider.assertAtLeastOnce(
                logMatch.debug( "IndexingService.start: index 1 on :LabelOne(propertyOne) is ONLINE" ),
                logMatch.debug( "IndexingService.start: index 2 on :LabelOne(propertyTwo) is POPULATING" ),
                logMatch.debug( "IndexingService.start: index 3 on :LabelTwo(propertyTwo) is FAILED" )
        );
    }

    @Test
    public void shouldFailToStartIfMissingIndexProvider() throws Exception
    {
        // GIVEN an indexing service that has a schema index provider X
        String otherProviderKey = "something-completely-different";
        SchemaIndexProvider.Descriptor otherDescriptor = new SchemaIndexProvider.Descriptor(
                otherProviderKey, "no-version" );
        IndexRule rule = indexRule( 1, 2, 3, otherDescriptor );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                mock( IndexPopulator.class ), mock( IndexAccessor.class ),
                new DataUpdates(), rule );

        // WHEN trying to start up and initialize it with an index from provider Y
        try
        {
            life.init();
            fail( "initIndexes with mismatching index provider should fail" );
        }
        catch ( LifecycleException e )
        {   // THEN starting up should fail
            assertThat( e.getCause().getMessage(), containsString( "existing index" ) );
            assertThat( e.getCause().getMessage(), containsString( otherProviderKey ) );
        }
    }

    @Test
    public void shouldSnapshotOnlineIndexes() throws Exception
    {
        // GIVEN
        int indexId = 1;
        int indexId2 = 2;
        IndexRule rule1 = indexRule( indexId, 2, 3, PROVIDER_DESCRIPTOR );
        IndexRule rule2 = indexRule( indexId2, 4, 5, PROVIDER_DESCRIPTOR );

        IndexAccessor indexAccessor = mock( IndexAccessor.class );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                mock( IndexPopulator.class ), indexAccessor,
                new DataUpdates( ), rule1, rule2 );
        File theFile = new File( "Blah" );

        when( indexAccessor.snapshotFiles()).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( indexId, rule1.getIndexDescriptor() ) ).thenReturn( ONLINE );
        when( indexProvider.getInitialState( indexId2, rule2.getIndexDescriptor() ) ).thenReturn( ONLINE );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) )
                .thenReturn( newDoubleLongRegister( 32L, 32L ) );

        life.start();

        // WHEN
        ResourceIterator<File> files = indexing.snapshotStoreFiles();

        // THEN
        // We get a snapshot per online index
        assertThat( asCollection( files ), equalTo( asCollection( iterator( theFile, theFile ) ) ) );
    }

    @Test
    public void shouldNotSnapshotPopulatingIndexes() throws Exception
    {
        // GIVEN
        CountDownLatch populatorLatch = new CountDownLatch( 1 );
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        int indexId = 1;
        int indexId2 = 2;
        IndexRule rule1 = indexRule( indexId, 2, 3, PROVIDER_DESCRIPTOR );
        IndexRule rule2 = indexRule( indexId2, 4, 5, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, indexAccessor,
                new DataUpdates(), rule1, rule2 );
        File theFile = new File( "Blah" );

        doAnswer( waitForLatch( populatorLatch ) ).when( populator ).create();
        when( indexAccessor.snapshotFiles() ).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( indexId, rule1.getIndexDescriptor() ) ).thenReturn( POPULATING );
        when( indexProvider.getInitialState( indexId2, rule2.getIndexDescriptor() ) ).thenReturn( ONLINE );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) ).thenReturn( newDoubleLongRegister( 32L, 32L ) );
        life.start();

        // WHEN
        ResourceIterator<File> files = indexing.snapshotStoreFiles();
        populatorLatch.countDown(); // only now, after the snapshot, is the population job allowed to finish
        waitForIndexesToComeOnline( indexing, indexId, indexId2 );

        // THEN
        // We get a snapshot from the online index, but no snapshot from the populating one
        assertThat( asCollection( files ), equalTo( asCollection( iterator( theFile ) ) ) );
    }

    @Test
    public void shouldIgnoreActivateCallDuringRecovery() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        // when
        indexingService.activateIndex( 0 );

        // then no exception should be thrown.
    }

    @Test
    public void shouldLogTriggerSamplingOnAllIndexes() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        IndexSamplingMode mode = TRIGGER_REBUILD_ALL;

        // when
        indexingService.triggerIndexSampling( mode );

        // then
        logProvider.assertAtLeastOnce(
                logMatch.info( "Manual trigger for sampling all indexes [" + mode + "]" )
        );
    }

    @Test
    public void shouldLogTriggerSamplingOnAnIndexes() throws Exception
    {
        // given
        long indexId = 0;
        IndexSamplingMode mode = TRIGGER_REBUILD_ALL;
        IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 0, 1 );
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData(),
                IndexRule.indexRule( indexId, descriptor, PROVIDER_DESCRIPTOR ) );
        life.init();
        life.start();

        // when
        indexingService.triggerIndexSampling( descriptor.schema() , mode );

        // then
        String userDescription = descriptor.schema().userDescription( nameLookup );
        logProvider.assertAtLeastOnce(
                logMatch.info( "Manual trigger for sampling index " + userDescription + " [" + mode + "]" )
        );
    }

    @Test
    public void applicationOfIndexUpdatesShouldThrowIfServiceIsShutdown()
            throws IOException, IndexEntryConflictException
    {
        // Given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();
        life.shutdown();

        try
        {
            // When
            indexingService.apply( updates( asSet( add( 1, "foo" ) ) ) );
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Can't apply index updates" ) );
        }
    }

    private IndexUpdates updates( Iterable<IndexEntryUpdate<LabelSchemaDescriptor>> updates )
    {
        return new DirectIndexUpdates( updates );
    }

    @Test
    public void applicationOfUpdatesShouldFlush() throws Exception
    {
        // Given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater );
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();

        indexing.createIndexes( IndexRule.indexRule( 0, index, PROVIDER_DESCRIPTOR ) );
        waitForIndexesToComeOnline( indexing, 0 );
        verify( populator, timeout( 10000 ) ).close( true );

        // When
        indexing.apply( updates( asList( add( 1, "foo" ), add( 2, "bar" ) ) ) );

        // Then
        InOrder inOrder = inOrder( updater );
        inOrder.verify( updater ).process( add( 1, "foo" ) );
        inOrder.verify( updater ).process( add( 2, "bar" ) );
        inOrder.verify( updater ).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void closingOfValidatedUpdatesShouldCloseUpdaters() throws Exception
    {
        // Given
        long indexId1 = 1;
        long indexId2 = 2;

        int labelId1 = 24;
        int labelId2 = 42;

        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        IndexAccessor accessor1 = mock( IndexAccessor.class );
        IndexUpdater updater1 = mock( IndexUpdater.class );
        when( accessor1.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater1 );

        IndexAccessor accessor2 = mock( IndexAccessor.class );
        IndexUpdater updater2 = mock( IndexUpdater.class );
        when( accessor2.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater2 );

        when( indexProvider.getOnlineAccessor( eq( 1L ), any( IndexDescriptor.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( accessor1 );
        when( indexProvider.getOnlineAccessor( eq( 2L ), any( IndexDescriptor.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( accessor2 );

        life.start();

        indexing.createIndexes( indexRule( indexId1, labelId1, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        indexing.createIndexes( indexRule( indexId2, labelId2, propertyKeyId, PROVIDER_DESCRIPTOR ) );

        waitForIndexesToComeOnline( indexing, indexId1, indexId2 );

        verify( populator, timeout( 10000 ).times( 2 ) ).close( true );

        // When
        indexing.apply( updates( asList(
                add( 1, "foo", labelId1 ),
                add( 2, "bar", labelId2 ) ) ) );

        // Then
        verify( updater1 ).close();
        verify( updater2 ).close();
    }

    private void waitForIndexesToComeOnline( IndexingService indexing, long... indexRuleIds )
            throws IndexNotFoundKernelException
    {
        waitForIndexesToGetIntoState( indexing, ONLINE, indexRuleIds );
    }

    private void waitForIndexesToGetIntoState( IndexingService indexing, InternalIndexState state,
            long... indexRuleIds )
            throws IndexNotFoundKernelException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 30 );
        while ( !allInState( indexing, state, indexRuleIds ) )
        {
            if ( currentTimeMillis() > end )
            {
                fail( "Indexes couldn't come online" );
            }
        }
    }

    private boolean allInState( IndexingService indexing, InternalIndexState state,
            long[] indexRuleIds ) throws IndexNotFoundKernelException
    {
        for ( long indexRuleId : indexRuleIds )
        {
            if ( indexing.getIndexProxy( indexRuleId ).getState() != state )
            {
                return false;
            }
        }
        return true;
    }

    private IndexUpdates nodeIdsAsIndexUpdates( long... nodeIds )
    {
        return new IndexUpdates()
        {
            @Override
            public Iterator<IndexEntryUpdate<LabelSchemaDescriptor>> iterator()
            {
                List<IndexEntryUpdate<LabelSchemaDescriptor>> updates = new ArrayList<>();
                for ( long nodeId : nodeIds )
                {
                    updates.add( IndexEntryUpdate.add( nodeId, index.schema(), Values.of( 1 ) ) );
                }
                return updates.iterator();
            }

            @Override
            public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
                    PrimitiveLongObjectMap<NodeCommand> nodeCommands )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasUpdates()
            {
                return nodeIds.length > 0;
            }
        };
    }

    /*
     * See comments in IndexingService#createIndex
     */
    @Test
    public void shouldNotLoseIndexDescriptorDueToOtherSimilarIndexDuringRecovery() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        long indexId = 1;
        long otherIndexId = 2;
        NodeUpdates update = addNodeUpdate( nodeId, "value" );
        when( storeView.nodeAsUpdates( eq( nodeId ) ) ).thenReturn( update );
        DoubleLongRegister register = mock( DoubleLongRegister.class );
        when( register.readSecond() ).thenReturn( 42L );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) )
                .thenReturn( register );
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy( new TrackingIndexAccessor() );
        IndexRule index = IndexRule.indexRule( 1, this.index, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData( update ), index
        );
        when( indexProvider.getInitialState( indexId, index.getIndexDescriptor() ) ).thenReturn( ONLINE );
        life.init();

        // WHEN dropping another index, which happens to have the same label/property... while recovering
        IndexRule otherIndex = IndexRule.indexRule( otherIndexId, index.getIndexDescriptor(), PROVIDER_DESCRIPTOR );
        indexing.createIndexes( otherIndex );
        indexing.dropIndex( otherIndex );
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndexes( index );
        reset( accessor );
        indexing.apply( nodeIdsAsIndexUpdates( nodeId ) );
        // and WHEN starting, i.e. completing recovery
        life.start();

        verify( accessor ).newUpdater( RECOVERY );
    }

    @Test
    public void shouldWaitForRecoveredUniquenessConstraintIndexesToBeFullyPopulated() throws Exception
    {
        // I.e. when a uniqueness constraint is created, but database crashes before that schema record
        // ends up in the store, so that next start have no choice but to rebuild it.

        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        ControlledIndexPopulator populator = new ControlledIndexPopulator( latch );
        final AtomicLong indexId = new AtomicLong( -1 );
        IndexingService.Monitor monitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void awaitingPopulationOfRecoveredIndex( long index, IndexDescriptor descriptor )
            {
                // When we see that we start to await the index to populate, notify the slow-as-heck
                // populator that it can actually go and complete its job.
                indexId.set( index );
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
        indexing.createIndexes( constraintIndexRule( 2, labelId, propertyKeyId, PROVIDER_DESCRIPTOR,
                fakeOwningConstraintRuleId ) );
        // and then starting, i.e. considering recovery completed
        life.start();

        // THEN afterwards the index should be ONLINE
        assertEquals( 2, indexId.get() );
        assertEquals( ONLINE, indexing.getIndexProxy( indexId.get() ).getState() );
    }

    @Test
    public void shouldCreateMultipleIndexesInOneCall() throws Exception
    {
        // GIVEN
        IndexingService.Monitor monitor = IndexingService.NO_MONITOR;
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor,
                withData( addNodeUpdate( 0, "value", 1 ) ), monitor );
        life.start();

        // WHEN
        IndexRule indexRule1 = indexRule( 0, 0, 0, PROVIDER_DESCRIPTOR );
        IndexRule indexRule2 = indexRule( 1, 0, 1, PROVIDER_DESCRIPTOR );
        IndexRule indexRule3 = indexRule( 2, 1, 0, PROVIDER_DESCRIPTOR );
        indexing.createIndexes( indexRule1, indexRule2, indexRule3 );

        // THEN
        verify( indexProvider ).getPopulator( eq( 0L ), eq( IndexDescriptorFactory.forLabel( 0, 0 ) ),
                any( IndexSamplingConfig.class ) );
        verify( indexProvider ).getPopulator( eq( 1L ), eq( IndexDescriptorFactory.forLabel( 0, 1 ) ),
                any( IndexSamplingConfig.class ) );
        verify( indexProvider ).getPopulator( eq( 2L ), eq( IndexDescriptorFactory.forLabel( 1, 0 ) ),
                any( IndexSamplingConfig.class ) );

        waitForIndexesToComeOnline( indexing, 0, 1, 2 );
    }

    @Test
    public void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterPopulating() throws Exception
    {
        // given
        long indexId = 1;
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        IOException exception = new IOException( "Expected failure" );
        when( nameLookup.labelGetName( labelId ) ).thenReturn( "TheLabel" );
        when( nameLookup.propertyKeyGetName( propertyKeyId ) ).thenReturn( "propertyKey" );

        when( indexProvider.getOnlineAccessor(
                eq( indexId ), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        indexing.createIndexes( IndexRule.indexRule( indexId, index, PROVIDER_DESCRIPTOR ) );
        waitForIndexesToGetIntoState( indexing, InternalIndexState.FAILED, indexId );
        verify( populator, timeout( 10000 ).times( 2 ) ).close( closeArgs.capture() );

        // then
        assertEquals( FAILED, indexing.getIndexProxy( 1 ).getState() );
        assertEquals( asList( true, false ), closeArgs.getAllValues() );
        assertThat( storedFailure(), containsString( format( "java.io.IOException: Expected failure%n\tat " ) ) );
        logProvider.assertAtLeastOnce( inLog( IndexPopulationJob.class ).error( equalTo(
                "Failed to populate index: [:TheLabel(propertyKey) [provider: {key=quantum-dex, version=25.0}]]" ),
                causedBy( exception ) ) );
        logProvider.assertNone( inLog( IndexPopulationJob.class ).info(
                "Index population completed. Index is now online: [%s]",
                ":TheLabel(propertyKey) [provider: {key=quantum-dex, version=25.0}]" ) );
    }

    @Test
    public void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterRecoveringPopulatingIndex() throws Exception
    {
        // given
        long indexId = 1;
        IndexRule indexRule = IndexRule.indexRule( indexId, index, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), indexRule );

        IOException exception = new IOException( "Expected failure" );
        when( nameLookup.labelGetName( labelId ) ).thenReturn( "TheLabel" );
        when( nameLookup.propertyKeyGetName( propertyKeyId ) ).thenReturn( "propertyKey" );

        when( indexProvider.getInitialState( indexId, indexRule.getIndexDescriptor() ) ).thenReturn( POPULATING );
        when( indexProvider.getOnlineAccessor(
                eq( indexId ), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        waitForIndexesToGetIntoState( indexing, InternalIndexState.FAILED, indexId );
        verify( populator, timeout( 10000 ).times( 2 ) ).close( closeArgs.capture() );

        // then
        assertEquals( FAILED, indexing.getIndexProxy( 1 ).getState() );
        assertEquals( asList( true, false ), closeArgs.getAllValues() );
        assertThat( storedFailure(), containsString( format( "java.io.IOException: Expected failure%n\tat " ) ) );
        logProvider.assertAtLeastOnce( inLog( IndexPopulationJob.class ).error( equalTo(
                "Failed to populate index: [:TheLabel(propertyKey) [provider: {key=quantum-dex, version=25.0}]]" ),
                causedBy( exception ) ) );
        logProvider.assertNone( inLog( IndexPopulationJob.class ).info(
                "Index population completed. Index is now online: [%s]",
                ":TheLabel(propertyKey) [provider: {key=quantum-dex, version=25.0}]" ) );
    }

    @Test
    public void shouldLogIndexStateOutliersOnInit() throws Exception
    {
        // given
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( provider.getOnlineAccessor( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        TokenNameLookup mockLookup = mock( TokenNameLookup.class );

        List<IndexRule> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexRule populatingIndex = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( populatingIndex.getId(), populatingIndex.getIndexDescriptor() ) ).thenReturn( POPULATING );
        indexes.add( populatingIndex );
        IndexRule failedIndex = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( failedIndex.getId(), failedIndex.getIndexDescriptor() ) ).thenReturn( FAILED );
        indexes.add( failedIndex );
        for ( int i = 0; i < 10; i++ )
        {
            IndexRule indexRule = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
            when( provider.getInitialState( indexRule.getId(), indexRule.getIndexDescriptor() ) ).thenReturn( ONLINE );
            indexes.add( indexRule );
        }
        for ( int i = 0; i < nextIndexId; i++ )
        {
            when( mockLookup.labelGetName( i ) ).thenReturn( "Label" + i );
        }

        life.add( IndexingServiceFactory.createIndexingService( Config.defaults(), mock( JobScheduler.class ), providerMap,
                mock( IndexStoreView.class ), mockLookup, indexes, logProvider, IndexingService.NO_MONITOR,
                schemaState ) );

        when( mockLookup.propertyKeyGetName( 1 ) ).thenReturn( "prop" );

        // when
        life.init();

        // then
        logProvider.assertAtLeastOnce(
                logMatch.info( "IndexingService.init: index 1 on :Label1(prop) is POPULATING" ),
                logMatch.info( "IndexingService.init: index 2 on :Label2(prop) is FAILED" ),
                logMatch.info( "IndexingService.init: indexes not specifically mentioned above are ONLINE" )
        );
        logProvider.assertNone( logMatch.info( "IndexingService.init: index 3 on :Label3(prop) is ONLINE" ) );
    }

    @Test
    public void shouldLogIndexStateOutliersOnStart() throws Exception
    {
        // given
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( provider.getOnlineAccessor( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( mock( IndexAccessor.class ) );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        TokenNameLookup mockLookup = mock( TokenNameLookup.class );

        List<IndexRule> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexRule populatingIndex = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( populatingIndex.getId(), populatingIndex.getIndexDescriptor() ) ).thenReturn( POPULATING );
        indexes.add( populatingIndex );
        IndexRule failedIndex = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
        when( provider.getInitialState( failedIndex.getId(), failedIndex.getIndexDescriptor() ) ).thenReturn( FAILED );
        indexes.add( failedIndex );
        for ( int i = 0; i < 10; i++ )
        {
            IndexRule indexRule = indexRule( nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR );
            when( provider.getInitialState( indexRule.getId(), indexRule.getIndexDescriptor() ) ).thenReturn( ONLINE );
            indexes.add( indexRule );
        }
        for ( int i = 0; i < nextIndexId; i++ )
        {
            when( mockLookup.labelGetName( i ) ).thenReturn( "Label" + i );
        }

        IndexingService indexingService = IndexingServiceFactory.createIndexingService( Config.defaults(),
                mock( JobScheduler.class ), providerMap, storeView, mockLookup, indexes,
                logProvider, IndexingService.NO_MONITOR, schemaState );
        when( storeView.indexSample( anyLong(), any( DoubleLongRegister.class ) ) )
                .thenReturn( newDoubleLongRegister( 32L, 32L ) );
        when( mockLookup.propertyKeyGetName( 1 ) ).thenReturn( "prop" );

        // when
        indexingService.init();
        logProvider.clear();
        indexingService.start();

        // then
        logProvider.assertAtLeastOnce(
                logMatch.info( "IndexingService.start: index 1 on :Label1(prop) is POPULATING" ),
                logMatch.info( "IndexingService.start: index 2 on :Label2(prop) is FAILED" ),
                logMatch.info( "IndexingService.start: indexes not specifically mentioned above are ONLINE" )
        );
        logProvider.assertNone( logMatch.info( "IndexingService.start: index 3 on :Label3(prop) is ONLINE" ) );
    }

    @Test
    public void flushAllIndexesWhileSomeOfThemDropped() throws IOException
    {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxy validIndex = createIndexProxyMock();
        IndexProxy deletedIndexProxy = createIndexProxyMock();
        indexMapReference.modify( indexMap ->
        {
            indexMap.putIndexProxy( 1, validIndex );
            indexMap.putIndexProxy( 2, validIndex );
            indexMap.putIndexProxy( 3, deletedIndexProxy );
            indexMap.putIndexProxy( 4, validIndex );
            indexMap.putIndexProxy( 5, validIndex );
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
        } ).when( deletedIndexProxy ).force();

        IndexingService indexingService = createIndexServiceWithCustomIndexMap( indexMapReference );

        indexingService.forceAll();
        verify( validIndex, times( 4 ) ).force();
    }

    @Test
    public void failForceAllWhenOneOfTheIndexesFailToForce() throws IOException
    {
        IndexMapReference indexMapReference = new IndexMapReference();
        indexMapReference.modify( indexMap ->
        {
            IndexProxy validIndex = createIndexProxyMock();
            indexMap.putIndexProxy( 1, validIndex );
            indexMap.putIndexProxy( 2, validIndex );
            IndexProxy strangeIndexProxy = createIndexProxyMock();
            indexMap.putIndexProxy( 3, strangeIndexProxy );
            indexMap.putIndexProxy( 4, validIndex );
            indexMap.putIndexProxy( 5, validIndex );
            doThrow( new UncheckedIOException( new IOException( "Can't force" ) ) ).when( strangeIndexProxy ).force();
            return indexMap;
        } );

        IndexingService indexingService = createIndexServiceWithCustomIndexMap( indexMapReference );

        expectedException.expectMessage( "Unable to force" );
        expectedException.expect( UnderlyingStorageException.class );
        indexingService.forceAll();
    }

    private IndexProxy createIndexProxyMock()
    {
        IndexProxy proxy = mock( IndexProxy.class );
        IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 1, 2 );
        when( proxy.getDescriptor() ).thenReturn( descriptor );
        return proxy;
    }

    private static Matcher<? extends Throwable> causedBy( final Throwable exception )
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable item )
            {
                while ( item != null )
                {
                    if ( item == exception )
                    {
                        return true;
                    }
                    item = item.getCause();
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "exception caused by " ).appendValue( exception );
            }
        };
    }

    private String storedFailure() throws IOException
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
        public void add( Collection<? extends IndexEntryUpdate<?>> updates )
                throws IndexEntryConflictException, IOException
        {
            latch.waitForAllToStart();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            latch.finish();
        }
    }

    private Answer<Void> waitForLatch( final CountDownLatch latch )
    {
        return invocationOnMock ->
        {
            latch.await();
            return null;
        };
    }

    private Answer<ResourceIterator<File>> newResourceIterator( final File theFile )
    {
        return invocationOnMock -> asResourceIterator(iterator( theFile ));
    }

    private NodeUpdates addNodeUpdate( long nodeId, Object propertyValue )
    {
        return addNodeUpdate( nodeId, propertyValue, labelId );
    }

    private NodeUpdates addNodeUpdate( long nodeId, Object propertyValue, int labelId )
    {
        return NodeUpdates.forNode( nodeId, new long[]{labelId} )
                .added( index.schema().getPropertyId(), Values.of( propertyValue ) ).build();
    }

    private IndexEntryUpdate<LabelSchemaDescriptor> add( long nodeId, Object propertyValue )
    {
        return IndexEntryUpdate.add( nodeId, index.schema(), Values.of( propertyValue ) );
    }

    private IndexEntryUpdate<LabelSchemaDescriptor> add( long nodeId, Object propertyValue, int labelId )
    {
        LabelSchemaDescriptor schema = SchemaDescriptorFactory.forLabel( labelId, index.schema().getPropertyId() );
        return IndexEntryUpdate.add( nodeId, schema, Values.of( propertyValue ) );
    }

    private IndexingService newIndexingServiceWithMockedDependencies( IndexPopulator populator,
                                                                      IndexAccessor accessor,
                                                                      DataUpdates data,
                                                                      IndexRule... rules ) throws IOException
    {
        return newIndexingServiceWithMockedDependencies( populator, accessor, data, IndexingService.NO_MONITOR, rules );
    }

    private IndexingService newIndexingServiceWithMockedDependencies( IndexPopulator populator,
                                                                      IndexAccessor accessor,
                                                                      DataUpdates data,
                                                                      IndexingService.Monitor monitor,
                                                                      IndexRule... rules ) throws IOException
    {
        when( indexProvider.getInitialState( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( ONLINE );
        when( indexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( indexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( populator );
        data.getsProcessedByStoreScanFrom( storeView );
        when( indexProvider.getOnlineAccessor( anyLong(), any( IndexDescriptor.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( accessor );
        when( indexProvider.snapshotMetaFiles() ).thenReturn( Iterators.emptyResourceIterator() );
        when( indexProvider.storeMigrationParticipant( any( FileSystemAbstraction.class ), any( PageCache.class ) ) )
                .thenReturn( StoreMigrationParticipant.NOT_PARTICIPATING );

        when( nameLookup.labelGetName( anyInt() ) ).thenAnswer( new NameLookupAnswer( "label" ) );
        when( nameLookup.propertyKeyGetName( anyInt() ) ).thenAnswer( new NameLookupAnswer( "property" ) );

        Config config = Config.defaults( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled, "false" );

        return life.add( IndexingServiceFactory.createIndexingService( config,
                        life.add( new Neo4jJobScheduler() ),
                        new DefaultSchemaIndexProviderMap( indexProvider ),
                        storeView,
                        nameLookup,
                        loop( iterator( rules ) ),
                        logProvider,
                        monitor,
                        schemaState )
        );
    }

    private DataUpdates withData( NodeUpdates... updates )
    {
        return new DataUpdates( updates );
    }

    private static class DataUpdates implements Answer<StoreScan<IndexPopulationFailedKernelException>>
    {
        private final NodeUpdates[] updates;

        DataUpdates()
        {
            this.updates = new NodeUpdates[0];
        }

        DataUpdates( NodeUpdates[] updates )
        {
            this.updates = updates;
        }

        @SuppressWarnings( "unchecked" )
        void getsProcessedByStoreScanFrom( IndexStoreView mock )
        {
            when( mock.visitNodes( any(int[].class), any( IntPredicate.class ),
                    any( Visitor.class ), any( Visitor.class ), anyBoolean() ) ).thenAnswer( this );
        }

        @Override
        public StoreScan<IndexPopulationFailedKernelException> answer( InvocationOnMock invocation ) throws Throwable
        {
            final Visitor<NodeUpdates,IndexPopulationFailedKernelException> visitor =
                    visitor( invocation.getArguments()[2] );
            return new StoreScan<IndexPopulationFailedKernelException>()
            {
                @Override
                public void run() throws IndexPopulationFailedKernelException
                {
                    for ( NodeUpdates update : updates )
                    {
                        visitor.visit( update );
                    }
                }

                @Override
                public void stop()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater,
                        IndexEntryUpdate<?> update,
                        long currentlyIndexedNodeId )
                {
                    // no-op
                }

                @Override
                public PopulationProgress getProgress()
                {
                    return new PopulationProgress( 42, 100 );
                }
            };
        }

        @SuppressWarnings( {"unchecked", "rawtypes"} )
        private static Visitor<NodeUpdates, IndexPopulationFailedKernelException> visitor( Object v )
        {
            return (Visitor) v;
        }

        @Override
        public String toString()
        {
            return Arrays.toString( updates );
        }
    }

    private static class NameLookupAnswer implements Answer<String>
    {
        private final String kind;

        NameLookupAnswer( String kind )
        {

            this.kind = kind;
        }

        @Override
        public String answer( InvocationOnMock invocation ) throws Throwable
        {
            int id = (Integer) invocation.getArguments()[0];
            return kind + "[" + id + "]";
        }
    }

    private static class TrackingIndexAccessor extends IndexAccessor.Adapter
    {
        private final IndexUpdater updater = mock( IndexUpdater.class );

        @Override
        public void drop()
        {
            throw new UnsupportedOperationException( "Not required" );
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            return updater;
        }

        @Override
        public IndexReader newReader()
        {
            throw new UnsupportedOperationException( "Not required" );
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader()
        {
            throw new UnsupportedOperationException( "Not required" );
        }

        @Override
        public ResourceIterator<File> snapshotFiles()
        {
            throw new UnsupportedOperationException( "Not required" );
        }
    }

    private IndexRule indexRule( long ruleId, int labelId, int propertyKeyId, SchemaIndexProvider.Descriptor
            providerDescriptor )
    {
        return IndexRule.indexRule( ruleId, IndexDescriptorFactory.forLabel( labelId, propertyKeyId ),
                providerDescriptor );
    }

    private IndexRule constraintIndexRule( long ruleId, int labelId, int propertyKeyId, SchemaIndexProvider.Descriptor
            providerDescriptor )
    {
        return IndexRule.indexRule( ruleId, IndexDescriptorFactory.uniqueForLabel( labelId, propertyKeyId ),
                providerDescriptor );
    }

    private IndexRule constraintIndexRule( long ruleId, int labelId, int propertyKeyId, SchemaIndexProvider.Descriptor
            providerDescriptor, long constraintId )
    {
        return IndexRule.constraintIndexRule(
                ruleId, IndexDescriptorFactory.uniqueForLabel( labelId, propertyKeyId ), providerDescriptor, constraintId );
    }

    private IndexingService createIndexServiceWithCustomIndexMap( IndexMapReference indexMapReference )
    {
        return new IndexingService( mock( IndexProxyCreator.class ), mock( SchemaIndexProviderMap.class ),
                indexMapReference, mock( IndexStoreView.class ), Collections.emptyList(),
                mock( IndexSamplingController.class ), mock( TokenNameLookup.class ),
                mock( JobScheduler.class ), mock( SchemaState.class ), mock( MultiPopulatorFactory.class ),
                NullLogProvider.getInstance(), IndexingService.NO_MONITOR );
    }
}
