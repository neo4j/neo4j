/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.internal.helpers.collection.Iterables.filter;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

/**
 * This test validates that we count the correct amount of index updates. In the process it also verifies that the populated index has
 * the correct nodes, after the index have been flipped.
 * <p>
 * We build the index async with a node scan in the background and consume transactions to keep the index updated. Once
 * the index is build and becomes online, we save the index size(number of entries) and begin tracking updates. These
 * values can then be used to determine when to re-sample the index for example.
 * <p>
 * The area around when the index population is done is controlled using a {@link Barrier} so that we can assert sample data
 * with 100% accuracy against the updates we know that the test has done during the time the index was populating.
 */
@RunWith( Parameterized.class )
public class IndexStatisticsTest
{
    private static final double UNIQUE_NAMES = 10.0;
    private static final String[] NAMES = new String[]{
            "Andres", "Davide", "Jakub", "Chris", "Tobias", "Stefan", "Petra", "Rickard", "Mattias", "Emil", "Chris",
            "Chris"
    };

    private static final int CREATION_MULTIPLIER =
            Integer.getInteger( IndexStatisticsTest.class.getName() + ".creationMultiplier", 1_000 );
    private static final String PERSON_LABEL = "Person";
    private static final String NAME_PROPERTY = "name";

    @Parameter
    public boolean multiThreadedPopulationEnabled;

    @Rule
    public final DbmsRule dbRule = new EmbeddedDbmsRule()
            .withSetting( GraphDatabaseSettings.index_background_sampling_enabled, false )
            .startLazily();
    @Rule
    public final RandomRule random = new RandomRule();

    private GraphDatabaseAPI db;
    private final IndexOnlineMonitor indexOnlineMonitor = new IndexOnlineMonitor();

    @Parameters( name = "multiThreadedIndexPopulationEnabled = {0}" )
    public static Object[] multiThreadedIndexPopulationEnabledValues()
    {
        return new Object[]{true, false};
    }

    @Before
    public void before()
    {
        dbRule.withSetting( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled, multiThreadedPopulationEnabled );

        int batchSize = random.nextInt( 1, 5 );
        FeatureToggles.set( MultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME, batchSize );
        FeatureToggles.set( BatchingMultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME, batchSize );
        FeatureToggles.set( MultipleIndexPopulator.class, "print_debug", true );

        GraphDatabaseAPI graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        this.db = graphDatabaseAPI;
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( Monitors.class )
                .addMonitorListener( indexOnlineMonitor );
    }

    @After
    public void tearDown()
    {
        FeatureToggles.clear( MultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME );
        FeatureToggles.clear( BatchingMultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME );
        FeatureToggles.clear( MultipleIndexPopulator.class, "print_debug" );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedWhenPopulationBeforeTheIndexIsOnline() throws KernelException
    {
        // given
        indexOnlineMonitor.initialize( 0 );
        createSomePersons();

        // when
        IndexDescriptor index = createPersonNameIndex();
        awaitIndexesOnline();

        // then
        assertEquals( 0.75d, indexSelectivity( index ), 0d );
        assertEquals( 4L, indexSize( index ) );
        assertEquals( 0L, indexUpdates( index ) );
    }

    @Test
    public void shouldNotSeeDataCreatedAfterPopulation() throws KernelException
    {
        // given
        indexOnlineMonitor.initialize( 0 );
        IndexDescriptor index = createPersonNameIndex();
        awaitIndexesOnline();

        // when
        createSomePersons();

        // then
        assertEquals( 1.0d, indexSelectivity( index ), 0d );
        assertEquals( 0L, indexSize( index ) );
        assertEquals( 4L, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataSeenDuringPopulationAndIgnoreDataCreatedAfterPopulation()
            throws KernelException
    {
        // given
        indexOnlineMonitor.initialize( 0 );
        createSomePersons();
        IndexDescriptor index = createPersonNameIndex();
        awaitIndexesOnline();

        // when
        createSomePersons();

        // then
        assertEquals( 0.75d, indexSelectivity( index ), 0d );
        assertEquals( 4L, indexSize( index ) );
        assertEquals( 4L, indexUpdates( index ) );
    }

    @Test
    public void shouldRemoveIndexStatisticsAfterIndexIsDeleted() throws KernelException
    {
        // given
        indexOnlineMonitor.initialize( 0 );
        createSomePersons();
        IndexDescriptor index = createPersonNameIndex();
        awaitIndexesOnline();

        // when
        dropIndex( index );

        // then
        try
        {
            indexSelectivity( index );
            fail( "Expected IndexNotFoundKernelException to be thrown" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            var sample = getIndexingStatisticsStore().indexSample( index.getId() );
            assertEquals( 0, sample.uniqueValues() );
            assertEquals( 0, sample.sampleSize() );
        }

        // and then index size and index updates are zero on disk
        var indexSample = getIndexingStatisticsStore().indexSample( index.getId() );
        assertEquals( 0, indexSample.indexSize() );
        assertEquals( 0, indexSample.updates() );
    }

    @Test
    public void shouldProvideIndexSelectivityWhenThereAreManyDuplicates() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 0 );
        int created = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when
        IndexDescriptor index = createPersonNameIndex();
        awaitIndexesOnline();

        // then
        assertCorrectIndexSelectivity( index, created );
        assertCorrectIndexSize( created, indexSize( index ) );
        assertEquals( 0L, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditions() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 1 );
        int initialNodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when populating while creating
        IndexDescriptor index = createPersonNameIndex();
        final UpdatesTracker updatesTracker = executeCreations( CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        assertCorrectIndexSelectivity( index, seenWhilePopulating );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        int expectedUpdates = updatesTracker.createdAfterPopulation() + toIntExact( indexOnlineMonitor.indexSampleOnCompletion.updates() );
        assertCorrectIndexUpdates( expectedUpdates, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndDeletions() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 1 );
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexDescriptor index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsAndDeletions( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes( index );
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
        assertCorrectIndexSelectivity( index, seenWhilePopulating );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = updatesTracker.deletedAfterPopulation() + updatesTracker.createdAfterPopulation() +
                toIntExact( indexOnlineMonitor.indexSampleOnCompletion.updates() );
        assertCorrectIndexUpdates( expectedIndexUpdates, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndChanges() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 1 );
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexDescriptor index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsAndUpdates( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes( index );
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        assertCorrectIndexSelectivity( index, seenWhilePopulating );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = updatesTracker.createdAfterPopulation() + updatesTracker.updatedAfterPopulation() +
                toIntExact( indexOnlineMonitor.indexSampleOnCompletion.updates() );
        assertCorrectIndexUpdates( expectedIndexUpdates, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndChangesAndDeletions() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 1 );
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexDescriptor index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsDeletionsAndUpdates( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes( index );
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
        int expectedIndexUpdates = updatesTracker.deletedAfterPopulation() + updatesTracker.createdAfterPopulation() + updatesTracker.updatedAfterPopulation() +
                toIntExact( indexOnlineMonitor.indexSampleOnCompletion.updates() );
        assertCorrectIndexSelectivity( index, seenWhilePopulating );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        assertCorrectIndexUpdates( expectedIndexUpdates, indexUpdates( index ) );
    }

    @Test
    public void shouldWorkWhileHavingHeavyConcurrentUpdates() throws Exception
    {
        // given some initial data
        final long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;
        int threads = 5;
        indexOnlineMonitor.initialize( threads );
        ExecutorService executorService = Executors.newFixedThreadPool( threads );

        // when populating while creating
        final IndexDescriptor index = createPersonNameIndex();

        final Collection<Callable<UpdatesTracker>> jobs = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            jobs.add( () -> executeCreationsDeletionsAndUpdates( nodes, CREATION_MULTIPLIER ) );
        }

        List<Future<UpdatesTracker>> futures = executorService.invokeAll( jobs );
        // sum result into empty result
        UpdatesTracker result = new UpdatesTracker();
        result.notifyPopulationCompleted();
        for ( Future<UpdatesTracker> future : futures )
        {
            result.add( future.get() );
        }
        awaitIndexesOnline();

        executorService.shutdown();
        assertTrue( executorService.awaitTermination( 1, TimeUnit.MINUTES ) );

        // then
        assertIndexedNodesMatchesStoreNodes( index );
        int seenWhilePopulating = initialNodes + result.createdDuringPopulation() - result.deletedDuringPopulation();
        assertCorrectIndexSelectivity( index, seenWhilePopulating );
        assertCorrectIndexSize( "Tracker had " + result, seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = result.deletedAfterPopulation() + result.createdAfterPopulation() + result.updatedAfterPopulation() +
                toIntExact( indexOnlineMonitor.indexSampleOnCompletion.updates() );
        assertCorrectIndexUpdates( "Tracker had " + result, expectedIndexUpdates, indexUpdates( index ) );
    }

    private void assertIndexedNodesMatchesStoreNodes( IndexDescriptor index ) throws Exception
    {
        int nodesInStore = 0;
        Label label = Label.label( PERSON_LABEL );
        try ( Transaction transaction = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            List<String> mismatches = new ArrayList<>();
            int propertyKeyId = ktx.tokenRead().propertyKey( NAME_PROPERTY );
            IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                // Node --> Index
                for ( Node node : filter( n -> n.hasLabel( label ) && n.hasProperty( NAME_PROPERTY ), transaction.getAllNodes() ) )
                {
                    nodesInStore++;
                    String name = (String) node.getProperty( NAME_PROPERTY );
                    ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.exact( propertyKeyId, name ) );
                    boolean found = false;
                    while ( cursor.next() )
                    {
                        long indexedNode = cursor.nodeReference();
                        if ( indexedNode == node.getId() )
                        {
                            if ( found )
                            {
                                mismatches.add( "Index has multiple entries for " + name + " and " + indexedNode );
                            }
                            found = true;
                        }
                    }
                    if ( !found )
                    {
                        mismatches.add( "Index is missing entry for " + name + " " + node );
                    }
                }
                if ( !mismatches.isEmpty() )
                {
                    fail( join( mismatches.toArray(), format( "%n" ) ) );
                }
                // Node count == indexed node count
                ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.exists( propertyKeyId ) );
                int nodesInIndex = 0;
                while ( cursor.next() )
                {
                    nodesInIndex++;
                }
                assertEquals( nodesInStore, nodesInIndex );
            }
        }
    }

    private void deleteNode( long nodeId )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeId ).delete();
            tx.commit();
        }
    }

    private boolean changeName( long nodeId, Object newValue )
    {
        boolean changeIndexedNode = false;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            Object oldValue = node.getProperty( NAME_PROPERTY );
            if ( !oldValue.equals( newValue ) )
            {
                // Changes are only propagated when the value actually change
                changeIndexedNode = true;
            }
            node.setProperty( NAME_PROPERTY, newValue );
            tx.commit();
        }
        return changeIndexedNode;
    }

    private int createNamedPeople( long[] nodes, int offset ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            for ( String name : NAMES )
            {
                long nodeId = createPersonNode( ktx, name );
                if ( nodes != null )
                {
                    nodes[offset++] = nodeId;
                }
            }
            tx.commit();
        }
        return NAMES.length;
    }

    private long[] repeatCreateNamedPeopleFor( int totalNumberOfPeople ) throws Exception
    {
        // Parallelize the creation of persons
        final long[] nodes = new long[totalNumberOfPeople];
        final int threads = 100;
        final int peoplePerThread = totalNumberOfPeople / threads;

        final ExecutorService service = Executors.newFixedThreadPool( threads );
        final AtomicReference<KernelException> exception = new AtomicReference<>();

        final List<Callable<Void>> jobs = new ArrayList<>( threads );
        // Start threads that creates these people, relying on batched writes to speed things up
        for ( int i = 0; i < threads; i++ )
        {
            final int finalI = i;

            jobs.add( () ->
            {
                int offset = finalI * peoplePerThread;
                while ( offset < (finalI + 1) * peoplePerThread )
                {
                    try
                    {
                        offset += createNamedPeople( nodes, offset );
                    }
                    catch ( KernelException e )
                    {
                        exception.compareAndSet( null, e );
                        throw new RuntimeException( e );
                    }
                }
                return null;
            } );
        }

        for ( Future<?> job : service.invokeAll( jobs ) )
        {
            job.get();
        }

        service.awaitTermination( 1, TimeUnit.SECONDS );
        service.shutdown();

        // Make any KernelException thrown from a creation thread visible in the main thread
        Exception ex = exception.get();
        if ( ex != null )
        {
            throw ex;
        }

        return nodes;
    }

    private void dropIndex( IndexDescriptor index ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            ktx.schemaWrite().indexDrop( index );
            tx.commit();
        }
    }

    private long indexSize( IndexDescriptor reference )
    {
        return resolveDependency( IndexStatisticsStore.class ).indexSample( reference.getId() ).indexSize();
    }

    private long indexUpdates( IndexDescriptor reference )
    {
        return resolveDependency( IndexStatisticsStore.class ).indexSample( reference.getId() ).updates();
    }

    private double indexSelectivity( IndexDescriptor reference ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            double selectivity = getSelectivity( tx, reference );
            tx.commit();
            return selectivity;
        }
    }

    private double getSelectivity( Transaction tx, IndexDescriptor reference ) throws IndexNotFoundKernelException
    {
        return ((InternalTransaction) tx).kernelTransaction().schemaRead().indexUniqueValuesSelectivity( reference );
    }

    private IndexStatisticsStore getIndexingStatisticsStore()
    {
        return resolveDependency( IndexStatisticsStore.class );
    }

    private void createSomePersons() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            createPersonNode( ktx, "Davide" );
            createPersonNode( ktx, "Stefan" );
            createPersonNode( ktx, "John" );
            createPersonNode( ktx, "John" );
            tx.commit();
        }
    }

    private long createPersonNode( KernelTransaction ktx, Object value )
            throws KernelException
    {
        int labelId = ktx.tokenWrite().labelGetOrCreateForName( PERSON_LABEL );
        int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( NAME_PROPERTY );
        long nodeId = ktx.dataWrite().nodeCreate();
        ktx.dataWrite().nodeAddLabel( nodeId, labelId );
        ktx.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( value ) );
        return nodeId;
    }

    private IndexDescriptor createPersonNameIndex() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            int labelId = ktx.tokenWrite().labelGetOrCreateForName( PERSON_LABEL );
            int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( NAME_PROPERTY );
            LabelSchemaDescriptor schema = forLabel( labelId, propertyKeyId );
            var index = ktx.schemaWrite().indexCreate( schema, "my index" );
            tx.commit();
            return index;
        }
    }

    private <T> T resolveDependency( Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline(3, TimeUnit.MINUTES );
        }
    }

    private UpdatesTracker executeCreations( int numberOfCreations ) throws KernelException, InterruptedException
    {
        return internalExecuteCreationsDeletionsAndUpdates( null, numberOfCreations, false, false );
    }

    private UpdatesTracker executeCreationsAndDeletions( long[] nodes,
                                                         int numberOfCreations ) throws KernelException, InterruptedException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, numberOfCreations, true, false );
    }

    private UpdatesTracker executeCreationsAndUpdates( long[] nodes,
                                                       int numberOfCreations ) throws KernelException, InterruptedException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, numberOfCreations, false, true );
    }

    private UpdatesTracker executeCreationsDeletionsAndUpdates( long[] nodes,
                                                                int numberOfCreations ) throws KernelException, InterruptedException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, numberOfCreations, true, true );
    }

    private UpdatesTracker internalExecuteCreationsDeletionsAndUpdates( long[] nodes,
                                                                        int numberOfCreations,
                                                                        boolean allowDeletions,
                                                                        boolean allowUpdates ) throws KernelException, InterruptedException
    {
        if ( random.nextBoolean() )
        {
            // 50% of time await the start signal so that updater(s) race as much as possible with the populator.
            indexOnlineMonitor.startSignal.await();
        }
        Random random = ThreadLocalRandom.current();
        UpdatesTracker updatesTracker = new UpdatesTracker();
        int offset = 0;
        while ( updatesTracker.created() < numberOfCreations )
        {
            int created = createNamedPeople( nodes, offset );
            offset += created;
            updatesTracker.increaseCreated( created );
            notifyIfPopulationCompleted( updatesTracker );

            // delete if allowed
            if ( allowDeletions && updatesTracker.created() % 24 == 0 )
            {
                long nodeId = nodes[random.nextInt( nodes.length )];
                try
                {
                    deleteNode( nodeId );
                    updatesTracker.increaseDeleted( 1 );
                }
                catch ( NotFoundException ex )
                {
                    // ignore
                }
                notifyIfPopulationCompleted( updatesTracker );
            }

            // update if allowed
            if ( allowUpdates && updatesTracker.created() % 24 == 0 )
            {
                int randomIndex = random.nextInt( nodes.length );
                try
                {
                    if ( changeName( nodes[randomIndex], NAMES[random.nextInt( NAMES.length )] ) )
                    {
                        updatesTracker.increaseUpdated( 1 );
                    }
                }
                catch ( NotFoundException ex )
                {
                    // ignore
                }
                notifyIfPopulationCompleted( updatesTracker );
            }
        }
        // make sure population complete has been notified
        notifyPopulationCompleted( updatesTracker );
        return updatesTracker;
    }

    private void notifyPopulationCompleted( UpdatesTracker updatesTracker )
    {
        indexOnlineMonitor.updatesDone();
        updatesTracker.notifyPopulationCompleted();
    }

    private void notifyIfPopulationCompleted( UpdatesTracker updatesTracker )
    {
        if ( isCompletedPopulation( updatesTracker ) )
        {
            notifyPopulationCompleted( updatesTracker );
        }
    }

    private boolean isCompletedPopulation( UpdatesTracker updatesTracker )
    {
        return !updatesTracker.isPopulationCompleted() && indexOnlineMonitor.isIndexOnline();
    }

    private void assertCorrectIndexSize( long expected, long actual )
    {
        assertCorrectIndexSize( "", expected, actual );
    }

    private void assertCorrectIndexSize( String info, long expected, long actual )
    {
        long updatesAfterCompletion = indexOnlineMonitor.indexSampleOnCompletion.updates();
        String message = format(
                "Expected number of entries to not differ (expected: %d actual: %d) %s",
                expected, actual, info );
        assertThat( Math.abs( expected - actual ) ).withFailMessage( message ).isLessThanOrEqualTo( updatesAfterCompletion );
    }

    private static void assertCorrectIndexUpdates( long expected, long actual )
    {
        assertCorrectIndexUpdates( "", expected, actual );
    }

    private static void assertCorrectIndexUpdates( String info, long expected, long actual )
    {
        String message = format(
                "Expected number of index updates to not differ (expected: %d actual: %d). %s",
                expected, actual, info );
        assertEquals( message, 0L, Math.abs( expected - actual ) );
    }

    private void assertCorrectIndexSelectivity( IndexDescriptor index, long numberOfEntries ) throws KernelException
    {
        double expected = UNIQUE_NAMES / numberOfEntries;
        double actual = indexSelectivity( index );
        double maxDelta = (double) indexOnlineMonitor.indexSampleOnCompletion.updates() / numberOfEntries;

        String message = format(
                "Expected number of entries to not differ (expected: %f actual: %f)",
                expected, actual );
        assertEquals( message, expected, actual, maxDelta );
    }

    private class IndexOnlineMonitor extends IndexingService.MonitorAdapter
    {
        private CountDownLatch updateTrackerCompletionLatch;
        private final CountDownLatch startSignal = new CountDownLatch( 1 );
        private volatile boolean isOnline;
        private Barrier.Control barrier;
        private IndexSample indexSampleOnCompletion;

        void initialize( int numberOfUpdateTrackers )
        {
            updateTrackerCompletionLatch = new CountDownLatch( numberOfUpdateTrackers );
            if ( numberOfUpdateTrackers > 0 )
            {
                barrier = new Barrier.Control();
            }
        }

        void updatesDone()
        {
            updateTrackerCompletionLatch.countDown();
            try
            {
                updateTrackerCompletionLatch.await();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            if ( barrier != null )
            {
                barrier.reached();
            }
        }

        @Override
        public void indexPopulationScanStarting()
        {
            startSignal.countDown();
        }

        /**
         * Index population is now completed, the populator hasn't yet been flipped and so sample hasn't been extracted.
         * The IndexPopulationJob, who is calling this method will now wait for the UpdatesTracker to notice that the index is online
         * so that it will complete whatever update it's doing and then snapshot its created/deleted values for later assertions.
         * When the UpdatesTracker notices this it will trigger this thread to continue and eventually call populationCompleteOn below,
         * completing the flip and the sampling. The barrier should prevent UpdatesTracker and IndexPopulationJob from racing in this area.
         */
        @Override
        public void indexPopulationScanComplete()
        {
            isOnline = true;
            if ( barrier != null )
            {
                try
                {
                    barrier.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }

        @Override
        public void populationCompleteOn( IndexDescriptor descriptor )
        {
            indexSampleOnCompletion = getIndexingStatisticsStore().indexSample( descriptor.getId() );
            if ( barrier != null )
            {
                barrier.release();
            }
        }

        boolean isIndexOnline()
        {
            return isOnline;
        }
    }
}
