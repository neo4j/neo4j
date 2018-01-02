/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.register.Register;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.test.DoubleLatch;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
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
import static org.neo4j.collection.primitive.PrimitiveLongCollections.setOf;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asResourceIterator;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.BATCHED;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_ALL;
import static org.neo4j.kernel.impl.store.record.IndexRule.constraintIndexRule;
import static org.neo4j.kernel.impl.store.record.IndexRule.indexRule;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.test.AwaitAnswer.afterAwaiting;

public class IndexingServiceTest
{
    private static final LogMatcherBuilder logMatch = inLog( IndexingService.class );

    @Rule
    public final LifeRule life = new LifeRule();
    private final int labelId = 7;
    private final int propertyKeyId = 15;
    private final IndexPopulator populator = mock( IndexPopulator.class );
    private final IndexUpdater updater = mock( IndexUpdater.class );
    private final SchemaIndexProvider indexProvider = mock( SchemaIndexProvider.class );
    private final IndexAccessor accessor = mock( IndexAccessor.class, RETURNS_MOCKS );
    private final IndexStoreView storeView  = mock( IndexStoreView.class );
    private final TokenNameLookup nameLookup = mock( TokenNameLookup.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    {
        when( storeView.indexSample( any( IndexDescriptor.class ), any( DoubleLongRegister.class ) ) ).thenAnswer(
                new Answer<DoubleLongRegister>()
                {
                    @Override
                    public DoubleLongRegister answer( InvocationOnMock invocation ) throws Throwable
                    {
                        return (DoubleLongRegister) invocation.getArguments()[1];
                    }
                } );
    }

    @Test
    public void shouldBringIndexOnlineAndFlipOverToIndexAccessor() throws Exception
    {
        // given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn(updater);

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        life.start();

        // when
        indexingService.createIndex( indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );

        verify( populator, timeout( 1000 ) ).close( true );

        try (IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( add( 10, "foo" ) );
        }

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).close( true );
        order.verify( accessor ).newUpdater( IndexUpdateMode.ONLINE );
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
        indexingService.createIndex( IndexRule.indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        indexingService.createIndex( IndexRule.indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );

        // We are asserting that the second call to createIndex does not throw an exception.
    }

    @Test
    public void shouldDeliverUpdatesThatOccurDuringPopulationToPopulator() throws Exception
    {
        // given
        when( populator.newPopulatingUpdater( storeView ) ).thenReturn( updater );

        CountDownLatch latch = new CountDownLatch( 1 );
        doAnswer( afterAwaiting( latch ) ).when( populator ).add( anyLong(), any() );

        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies( populator, accessor, withData( add( 1, "value1" ) ) );

        life.start();

        // when
        indexingService.createIndex( indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );
        assertEquals( InternalIndexState.POPULATING, proxy.getState() );

        NodePropertyUpdate value2 = add( 2, "value2" );
        try (IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( value2 );
        }

        latch.countDown();

        verify( populator, timeout( 1000 ) ).close( true );

        // then
        assertEquals( InternalIndexState.ONLINE, proxy.getState() );
        InOrder order = inOrder( populator, accessor, updater);
        order.verify( populator ).create();
        order.verify( populator ).add( 1, "value1" );


        // invoked from indexAllNodes(), empty because the id we added (2) is bigger than the one we indexed (1)
        //
        // (We don't get an update for value2 here because we mock a fake store that doesn't contain it
        //  just for the purpose of testing this behavior)
        order.verify( populator ).newPopulatingUpdater( storeView );
        order.verify( updater ).close();
        order.verify( populator ).verifyDeferredConstraints( storeView );
        order.verify( populator ).sampleResult( any( Register.DoubleLong.Out.class ) );
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
        indexingService.createIndex( constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR, null ) );
        IndexProxy proxy = indexingService.getIndexProxy( 0 );

        verify( populator, timeout( 1000 ) ).close( true );

        try (IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE ) )
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
        indexingService.createIndex( constraintIndexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR, null ) );
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
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        TokenNameLookup mockLookup = mock( TokenNameLookup.class );

        IndexRule onlineIndex     = indexRule( 1, 1, 1, PROVIDER_DESCRIPTOR );
        IndexRule populatingIndex = indexRule( 2, 1, 2, PROVIDER_DESCRIPTOR );
        IndexRule failedIndex     = indexRule( 3, 2, 2, PROVIDER_DESCRIPTOR );

        IndexingService indexingService = life.add( IndexingService.create( new IndexSamplingConfig( new Config() ),
                mock( JobScheduler.class ), providerMap, mock( IndexStoreView.class ), mockLookup,
                mock( UpdateableSchemaState.class ), asList( onlineIndex, populatingIndex, failedIndex ),
                logProvider, IndexingService.NO_MONITOR ) );


        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );

        when(mockLookup.labelGetName( 1 )).thenReturn( "LabelOne" );
        when( mockLookup.labelGetName( 2 ) ).thenReturn( "LabelTwo" );
        when( mockLookup.propertyKeyGetName( 1 ) ).thenReturn( "propertyOne" );
        when( mockLookup.propertyKeyGetName( 2 ) ).thenReturn( "propertyTwo" );

        // when
        life.init();

        // then
        logProvider.assertExactly(
                logMatch.info( "IndexingService.init: index 1 on :LabelOne(propertyOne) is ONLINE" ),
                logMatch.info( "IndexingService.init: index 2 on :LabelOne(propertyTwo) is POPULATING" ),
                logMatch.info( "IndexingService.init: index 3 on :LabelTwo(propertyTwo) is FAILED" )
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

        IndexingService indexingService = IndexingService.create( new IndexSamplingConfig( new Config() ),
                mock( JobScheduler.class ), providerMap, storeView, mockLookup, mock( UpdateableSchemaState.class ),
                asList( onlineIndex, populatingIndex, failedIndex ), logProvider, IndexingService.NO_MONITOR );

        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );

        indexingService.init();

        when(mockLookup.labelGetName( 1 )).thenReturn( "LabelOne" );
        when(mockLookup.labelGetName( 2 )).thenReturn( "LabelTwo" );
        when(mockLookup.propertyKeyGetName( 1 )).thenReturn( "propertyOne" );
        when(mockLookup.propertyKeyGetName( 2 )).thenReturn( "propertyTwo" );
        when( storeView.indexSample( any( IndexDescriptor.class ), any( DoubleLongRegister.class ) ) ).thenReturn( newDoubleLongRegister( 32l, 32l ) );

        logProvider.clear();

        // when
        indexingService.start();

        // then
        verify( provider ).getPopulationFailure( 3 );
        logProvider.assertAtLeastOnce(
                logMatch.info( "IndexingService.start: index 1 on :LabelOne(propertyOne) is ONLINE" ),
                logMatch.info( "IndexingService.start: index 2 on :LabelOne(propertyTwo) is POPULATING" ),
                logMatch.info( "IndexingService.start: index 3 on :LabelTwo(propertyTwo) is FAILED" )
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
                new DataUpdates( new NodePropertyUpdate[0] ), rule );

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
                new DataUpdates( new NodePropertyUpdate[0] ), rule1, rule2 );
        File theFile = new File( "Blah" );

        when( indexAccessor.snapshotFiles()).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( indexId ) ).thenReturn( ONLINE );
        when( indexProvider.getInitialState( indexId2 ) ).thenReturn( ONLINE );
        when( storeView.indexSample( any( IndexDescriptor.class ), any( DoubleLongRegister.class ) ) )
                .thenReturn( newDoubleLongRegister( 32l, 32l ) );

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
        CountDownLatch populatorLatch = new CountDownLatch(1);
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        int indexId = 1;
        int indexId2 = 2;
        IndexRule rule1 = indexRule( indexId, 2, 3, PROVIDER_DESCRIPTOR );
        IndexRule rule2 = indexRule( indexId2, 4, 5, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, indexAccessor,
                new DataUpdates( new NodePropertyUpdate[0] ), rule1, rule2 );
        File theFile = new File( "Blah" );

        doAnswer( waitForLatch( populatorLatch ) ).when( populator ).create();
        when( indexAccessor.snapshotFiles() ).thenAnswer( newResourceIterator( theFile ) );
        when( indexProvider.getInitialState( indexId ) ).thenReturn( POPULATING );
        when( indexProvider.getInitialState( indexId2 ) ).thenReturn( ONLINE );
        when( storeView.indexSample( any( IndexDescriptor.class ), any( DoubleLongRegister.class ) ) ).thenReturn( newDoubleLongRegister( 32l, 32l ) );
        life.start();

        // WHEN
        ResourceIterator<File> files = indexing.snapshotStoreFiles();
        populatorLatch.countDown(); // only now, after the snapshot, is the population job allowed to finish

        // THEN
        // We get a snapshot from the online index, but no snapshot from the populating one
        assertThat( asCollection( files ), equalTo( asCollection( iterator( theFile ) ) ) );
    }

    @Test
    public void shouldIgnoreActivateCallDuringRecovery() throws Exception
    {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

//        life.start();

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
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        IndexSamplingMode mode = TRIGGER_REBUILD_ALL;
        IndexDescriptor descriptor = new IndexDescriptor( 0, 1 );

        // when
        indexingService.triggerIndexSampling( descriptor, mode );

        // then
        String userDescription = descriptor.userDescription( nameLookup );
        logProvider.assertAtLeastOnce(
                logMatch.info( "Manual trigger for sampling index " + userDescription + " [" + mode + "]" )
        );
    }

    @Test
    public void recordingOfRecoveredNodesShouldThrowIfServiceIsStarted() throws Exception
    {
        // Given
        long recoveredNodeId = 1;

        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();

        try
        {
            // When
            indexingService.visited( recoveredNodeId );
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Can't queue recovered node ids" ) );
        }
    }

    @Test
    public void validationOfIndexUpdatesShouldThrowIfServiceIsNotStarted() throws IOException
    {
        // Given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );

        try
        {
            // When
            indexingService.validate( asSet( add( 1, "foo" ) ), IndexUpdateMode.ONLINE );
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Can't validate index updates" ) );
        }
    }

    @Test
    public void validationOfIndexUpdatesShouldThrowIfServiceIsStopped() throws IOException
    {
        // Given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();
        life.stop();

        try
        {
            // When
            indexingService.validate( asSet( add( 1, "foo" ) ), IndexUpdateMode.ONLINE );
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Can't validate index updates" ) );
        }
    }

    @Test
    public void validatedUpdatesShouldFlush() throws Exception
    {
        // Given
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater );
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();

        indexing.createIndex( indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        verify( populator, timeout( 1000 ) ).close( true );

        // When
        try ( ValidatedIndexUpdates updates = indexing.validate( asList( add( 1, "foo" ), add( 2, "bar" ) ),
                IndexUpdateMode.ONLINE ) )
        {
            updates.flush();
        }

        // Then
        InOrder inOrder = inOrder( updater );
        inOrder.verify( updater ).validate( asList( add( 1, "foo" ), add( 2, "bar" ) ) );
        inOrder.verify( updater ).process( add( 1, "foo" ) );
        inOrder.verify( updater ).process( add( 2, "bar" ) );
        inOrder.verify( updater ).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void closingOfValidatedUpdatesShouldRemoveReservation() throws Exception
    {
        // Given
        Reservation reservation = mock( Reservation.class );
        when( updater.validate( any( Iterable.class ) ) ).thenReturn( reservation );
        when( accessor.newUpdater( any( IndexUpdateMode.class ) ) ).thenReturn( updater );

        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData() );
        life.start();

        indexing.createIndex( indexRule( 0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        verify( populator, timeout( 1000 ) ).close( true );

        // When
        try ( ValidatedIndexUpdates updates = indexing.validate( asList( add( 1, "1" ), add( 2, "2" ) ),
                IndexUpdateMode.ONLINE ) )
        {
            verifyZeroInteractions( reservation );
            updates.flush();
            verifyZeroInteractions( reservation );
        }

        // Then
        verify( reservation ).release();
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
        when( accessor1.newUpdater( IndexUpdateMode.ONLINE ) ).thenReturn( updater1 );

        IndexAccessor accessor2 = mock( IndexAccessor.class );
        IndexUpdater updater2 = mock( IndexUpdater.class );
        when( accessor2.newUpdater( IndexUpdateMode.ONLINE ) ).thenReturn( updater2 );

        when( indexProvider.getOnlineAccessor( eq( 1L ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( accessor1 );
        when( indexProvider.getOnlineAccessor( eq( 2L ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( accessor2 );

        life.start();

        indexing.createIndex( indexRule( indexId1, labelId1, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        indexing.createIndex( indexRule( indexId2, labelId2, propertyKeyId, PROVIDER_DESCRIPTOR ) );

        verify( populator, timeout( 1000 ).times( 2 ) ).close( true );

        // When
        try ( ValidatedIndexUpdates updates = indexing.validate( asList(
                NodePropertyUpdate.add( 1, propertyKeyId, "foo", new long[]{labelId1} ),
                NodePropertyUpdate.add( 2, propertyKeyId, "bar", new long[]{labelId2} ) ), IndexUpdateMode.ONLINE ) )
        {
            updates.flush();
        }

        // Then
        verify( updater1 ).close();
        verify( updater2 ).close();
    }

    @Test
    public void recoveredUpdatesShouldBeApplied() throws IOException
    {
        // Given
        final long nodeId1 = 1;
        final long nodeId2 = 2;
        final PrimitiveLongSet nodeIds = setOf( nodeId1, nodeId2 );

        final NodePropertyUpdate nodeUpdate1 = add( nodeId1, "foo" );
        final NodePropertyUpdate nodeUpdate2 = add( nodeId2, "bar" );
        final Set<NodePropertyUpdate> nodeUpdates = asSet( nodeUpdate1, nodeUpdate2 );

        final AtomicBoolean applyingRecoveredDataCalled = new AtomicBoolean();
        final AtomicBoolean appliedRecoveredDataCalled = new AtomicBoolean();

        // Mockito not used here because it does not work well with mutable objects (set of recovered node ids in
        // this case, which is cleared at the end of recovery).
        // See https://code.google.com/p/mockito/issues/detail?id=126 and
        // https://groups.google.com/forum/#!topic/mockito/_A4BpsEAY9s
        IndexingService.Monitor monitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void applyingRecoveredData( PrimitiveLongSet recoveredNodeIds )
            {
                assertEquals( nodeIds, recoveredNodeIds );
                applyingRecoveredDataCalled.set( true );
            }

            @Override
            public void appliedRecoveredData( Iterable<NodePropertyUpdate> updates )
            {
                assertEquals( nodeUpdates, asSet( updates ) );
                appliedRecoveredDataCalled.set( true );
            }
        };

        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(), monitor );

        when( storeView.nodeAsUpdates( nodeId1 ) ).thenReturn( asSet( nodeUpdate1 ) );
        when( storeView.nodeAsUpdates( nodeId2 ) ).thenReturn( asSet( nodeUpdate2 ) );

        // When
        nodeIds.visitKeys( indexing );
        life.start();

        // Then
        assertTrue( "applyingRecoveredData was not called", applyingRecoveredDataCalled.get() );
        assertTrue( "appliedRecoveredData was not called", appliedRecoveredDataCalled.get() );
    }

    /*
     * See comments in IndexingService#createIndex
     */
    @Test
    public void shouldNotLoseIndexDescriptorDueToOtherSimilarIndexDuringRecovery() throws Exception
    {
        // GIVEN
        long nodeId = 0, indexId = 1, otherIndexId = 2;
        NodePropertyUpdate update = add( nodeId, "value" );
        when( storeView.nodeAsUpdates( nodeId ) ).thenReturn( asList( update ) );
        DoubleLongRegister register = mock( DoubleLongRegister.class );
        when( register.readSecond() ).thenReturn( 42L );
        when( storeView.indexSample( any( IndexDescriptor.class ), any( DoubleLongRegister.class ) ) )
                .thenReturn( register );
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy( new TrackingIndexAccessor() );
        IndexRule index = indexRule( 1, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData( update ), index
        );
        when( indexProvider.getInitialState( indexId ) ).thenReturn( ONLINE );
        life.init();

        // WHEN dropping another index, which happens to have the same label/property... while recovering
        IndexRule otherIndex = indexRule( otherIndexId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        indexing.createIndex( otherIndex );
        indexing.dropIndex( otherIndex );
        indexing.visited( nodeId );
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndex( index );
        reset( accessor );
        // and WHEN starting, i.e. completing recovery
        life.start();

        // THEN our index should still have been recovered properly
        // apparently we create updaters two times during recovery, get over it
        verify( accessor, times( 2 ) ).newUpdater( BATCHED );
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
                latch.start();
            }
        };
        // leaving out the IndexRule here will have the index being populated from scratch
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor,
                withData( NodePropertyUpdate.add( 0, 0, "value", new long[] {1} ) ), monitor );

        // WHEN initializing, i.e. preparing for recovery
        life.init();
        // simulating an index being created as part of applying recovered transactions
        long fakeOwningConstraintRuleId = 1;
        indexing.createIndex( new IndexRule( 2, labelId, propertyKeyId, PROVIDER_DESCRIPTOR,
                fakeOwningConstraintRuleId ) );
        // and then starting, i.e. considering recovery completed
        life.start();

        // THEN afterwards the index should be ONLINE
        assertEquals( 2, indexId.get() );
        assertEquals( ONLINE, indexing.getIndexProxy( indexId.get() ).getState() );
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
                eq( indexId ), any( IndexConfiguration.class ), any( IndexSamplingConfig.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        indexing.createIndex( indexRule( indexId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        verify( populator, timeout( 1000 ).times( 2 ) ).close( closeArgs.capture() );

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
        IndexingService indexing = newIndexingServiceWithMockedDependencies( populator, accessor, withData(),
                indexRule( indexId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR ) );

        IOException exception = new IOException( "Expected failure" );
        when( nameLookup.labelGetName( labelId ) ).thenReturn( "TheLabel" );
        when( nameLookup.propertyKeyGetName( propertyKeyId ) ).thenReturn( "propertyKey" );

        when( indexProvider.getInitialState( indexId ) ).thenReturn( POPULATING );
        when( indexProvider.getOnlineAccessor(
                eq( indexId ), any( IndexConfiguration.class ), any( IndexSamplingConfig.class ) ) )
                .thenThrow( exception );

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass( Boolean.class );

        // when
        verify( populator, timeout( 1000 ).times( 2 ) ).close( closeArgs.capture() );

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

    static Matcher<? extends Throwable> causedBy( final Throwable exception )
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
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            latch.awaitStart();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            latch.finish();
        }
    }

    private Answer<Void> waitForLatch( final CountDownLatch latch )
    {
        return new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                latch.await();
                return null;
            }
        };
    }

    private Answer<ResourceIterator<File>> newResourceIterator( final File theFile )
    {
        return new Answer<ResourceIterator<File>>(){

            @Override
            public ResourceIterator<File> answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                return asResourceIterator(iterator( theFile ));
            }
        };
    }

    private NodePropertyUpdate add( long nodeId, Object propertyValue )
    {
        return NodePropertyUpdate.add( nodeId, propertyKeyId, propertyValue, new long[]{labelId} );
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
        UpdateableSchemaState schemaState = mock( UpdateableSchemaState.class );

        when( indexProvider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        when( indexProvider.getPopulator( anyLong(), any( IndexDescriptor.class ), any( IndexConfiguration.class ),
                any( IndexSamplingConfig.class ) ) ).thenReturn( populator );
        data.getsProcessedByStoreScanFrom( storeView );
        when( indexProvider.getOnlineAccessor(
                        anyLong(), any( IndexConfiguration.class ), any( IndexSamplingConfig.class ) ) )
                .thenReturn( accessor );
        when( indexProvider.snapshotMetaFiles() ).thenReturn( IteratorUtil.<File>emptyIterator() );
        when( indexProvider.storeMigrationParticipant( any( FileSystemAbstraction.class ), any( PageCache.class ) ) )
                .thenReturn( StoreMigrationParticipant.NOT_PARTICIPATING );

        when( nameLookup.labelGetName( anyInt() ) ).thenAnswer( new NameLookupAnswer( "label" ) );
        when( nameLookup.propertyKeyGetName( anyInt() ) ).thenAnswer( new NameLookupAnswer( "property" ) );

        return life.add( IndexingService.create( new IndexSamplingConfig( new Config() ),
                        life.add( new Neo4jJobScheduler() ),
                        new DefaultSchemaIndexProviderMap( indexProvider ),
                        storeView,
                        nameLookup,
                        schemaState,
                        loop( iterator( rules ) ),
                        logProvider,
                        monitor )
        );
    }

    private DataUpdates withData( NodePropertyUpdate... updates )
    {
        return new DataUpdates( updates );
    }

    private static class DataUpdates implements Answer<StoreScan<RuntimeException>>, Iterable<NodePropertyUpdate>
    {
        private final NodePropertyUpdate[] updates;

        DataUpdates( NodePropertyUpdate[] updates )
        {
            this.updates = updates;
        }

        void getsProcessedByStoreScanFrom( IndexStoreView mock )
        {
            when( mock.visitNodesWithPropertyAndLabel( any( IndexDescriptor.class ), visitor( any( Visitor.class ) ) ) )
                    .thenAnswer( this );
        }

        @Override
        public StoreScan<RuntimeException> answer( InvocationOnMock invocation ) throws Throwable
        {
            final Visitor<NodePropertyUpdate, RuntimeException> visitor = visitor( invocation.getArguments()[1] );
            return new StoreScan<RuntimeException>()
            {
                @Override
                public void run()
                {
                    for ( NodePropertyUpdate update : updates )
                    {
                        visitor.visit( update );
                    }
                }

                @Override
                public void stop()
                {
                    // throw new UnsupportedOperationException( "not implemented" );
                }
            };
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Visitor<NodePropertyUpdate, RuntimeException> visitor( Object v )
        {
            return (Visitor) v;
        }

        @Override
        public Iterator<NodePropertyUpdate> iterator()
        {
            return new ArrayIterator<>( updates );
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

        public NameLookupAnswer( String kind )
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

    private static class TrackingIndexAccessor implements IndexAccessor
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
        public void force()
        {
        }

        @Override
        public void flush()
        {
        }

        @Override
        public void close()
        {
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
}
