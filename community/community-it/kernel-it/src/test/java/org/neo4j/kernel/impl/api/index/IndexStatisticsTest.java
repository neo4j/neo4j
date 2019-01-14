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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

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
    public final DatabaseRule dbRule = new EmbeddedDatabaseRule()
            .withSetting( GraphDatabaseSettings.index_background_sampling_enabled, "false" )
            .startLazily();
    @Rule
    public final RandomRule random = new RandomRule();

    private GraphDatabaseService db;
    private ThreadToStatementContextBridge bridge;
    private final IndexOnlineMonitor indexOnlineMonitor = new IndexOnlineMonitor();

    @Parameters( name = "multiThreadedIndexPopulationEnabled = {0}" )
    public static Object[] multiThreadedIndexPopulationEnabledValues()
    {
        return new Object[]{true, false};
    }

    @Before
    public void before()
    {
        dbRule.withSetting( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled, multiThreadedPopulationEnabled + "" );

        int batchSize = random.nextInt( 1, 5 );
        FeatureToggles.set( MultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME, batchSize );
        FeatureToggles.set( BatchingMultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME, batchSize );

        GraphDatabaseAPI graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        this.db = graphDatabaseAPI;
        DependencyResolver dependencyResolver = graphDatabaseAPI.getDependencyResolver();
        this.bridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( Monitors.class )
                .addMonitorListener( indexOnlineMonitor );
    }

    @After
    public void tearDown()
    {
        FeatureToggles.clear( MultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME );
        FeatureToggles.clear( BatchingMultipleIndexPopulator.class, MultipleIndexPopulator.QUEUE_THRESHOLD_NAME );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedWhenPopulationBeforeTheIndexIsOnline() throws KernelException
    {
        // given
        indexOnlineMonitor.initialize( 0 );
        createSomePersons();

        // when
        IndexReference index = createPersonNameIndex();
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
        IndexReference index = createPersonNameIndex();
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
        IndexReference index = createPersonNameIndex();
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
        IndexReference index = createPersonNameIndex();
        awaitIndexesOnline();

        SchemaStorage storage = new SchemaStorage( neoStores().getSchemaStore() );
        long indexId = storage.indexGetForSchema( (IndexDescriptor) index ).getId();

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
            DoubleLongRegister actual = getTracker().indexSample( indexId, Registers.newDoubleLongRegister() );
            assertDoubleLongEquals( 0L, 0L, actual );
        }

        // and then index size and index updates are zero on disk
        DoubleLongRegister actual = getTracker().indexUpdatesAndSize( indexId, Registers.newDoubleLongRegister() );
        assertDoubleLongEquals( 0L, 0L, actual );
    }

    @Test
    public void shouldProvideIndexSelectivityWhenThereAreManyDuplicates() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 0 );
        int created = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when
        IndexReference index = createPersonNameIndex();
        awaitIndexesOnline();

        // then
        double expectedSelectivity = UNIQUE_NAMES / created;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
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
        IndexReference index = createPersonNameIndex();
        final UpdatesTracker updatesTracker = executeCreations( CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        assertCorrectIndexUpdates( updatesTracker.createdAfterPopulation(), indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndDeletions() throws Exception
    {
        // given some initial data
        indexOnlineMonitor.initialize( 1 );
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexReference index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsAndDeletions( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes();
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = updatesTracker.deletedAfterPopulation() + updatesTracker.createdAfterPopulation();
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
        IndexReference index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsAndUpdates( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes();
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = updatesTracker.createdAfterPopulation() + updatesTracker.updatedAfterPopulation();
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
        IndexReference index = createPersonNameIndex();
        UpdatesTracker updatesTracker = executeCreationsDeletionsAndUpdates( nodes, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        assertIndexedNodesMatchesStoreNodes();
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        int expectedIndexUpdates = updatesTracker.deletedAfterPopulation() + updatesTracker.createdAfterPopulation() + updatesTracker.updatedAfterPopulation();
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
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
        final IndexReference index = createPersonNameIndex();

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
        assertIndexedNodesMatchesStoreNodes();
        int seenWhilePopulating = initialNodes + result.createdDuringPopulation() - result.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( "Tracker had " + result, seenWhilePopulating, indexSize( index ) );
        int expectedIndexUpdates = result.deletedAfterPopulation() + result.createdAfterPopulation() + result.updatedAfterPopulation();
        assertCorrectIndexUpdates( "Tracker had " + result, expectedIndexUpdates, indexUpdates( index ) );
    }

    private void assertIndexedNodesMatchesStoreNodes() throws Exception
    {
        int nodesInStore = 0;
        Label label = Label.label( PERSON_LABEL );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
            List<String> mismatches = new ArrayList<>();
            int labelId = ktx.tokenRead().nodeLabel( PERSON_LABEL );
            int propertyKeyId = ktx.tokenRead().propertyKey( NAME_PROPERTY );
            IndexReference index = ktx.schemaRead().index( labelId, propertyKeyId );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                // Node --> Index
                for ( Node node : filter( n -> n.hasLabel( label ) && n.hasProperty( NAME_PROPERTY ), db.getAllNodes() ) )
                {
                    nodesInStore++;
                    String name = (String) node.getProperty( NAME_PROPERTY );
                    ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, IndexQuery.exact( propertyKeyId, name ) );
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
                        mismatches.add( "Index is missing entry for " + name );
                    }
                }
                if ( !mismatches.isEmpty() )
                {
                    fail( join( mismatches.toArray(), format( "%n" ) ) );
                }
                // Node count == indexed node count
                ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, IndexQuery.exists( propertyKeyId ) );
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
            db.getNodeById( nodeId ).delete();
            tx.success();
        }
    }

    private boolean changeName( long nodeId, Object newValue )
    {
        boolean changeIndexedNode = false;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            Object oldValue = node.getProperty( NAME_PROPERTY );
            if ( !oldValue.equals( newValue ) )
            {
                // Changes are only propagated when the value actually change
                changeIndexedNode = true;
            }
            node.setProperty( NAME_PROPERTY, newValue );
            tx.success();
        }
        return changeIndexedNode;
    }

    private int createNamedPeople( long[] nodes, int offset ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            for ( String name : NAMES )
            {
                long nodeId = createPersonNode( ktx, name );
                if ( nodes != null )
                {
                    nodes[offset++] = nodeId;
                }
            }
            tx.success();
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

    private void dropIndex( IndexReference index ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            try ( Statement ignore = ktx.acquireStatement() )
            {
                ktx.schemaWrite().indexDrop( index );
            }
            tx.success();
        }
    }

    private long indexSize( IndexReference reference ) throws KernelException
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( IndexingService.class )
                                      .indexUpdatesAndSize( reference.schema() ).readSecond();
    }

    private long indexUpdates( IndexReference reference  ) throws KernelException
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( IndexingService.class )
                                      .indexUpdatesAndSize( reference.schema() ).readFirst();
    }

    private double indexSelectivity( IndexReference reference ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            double selectivity = getSelectivity( reference );
            tx.success();
            return selectivity;
        }
    }

    private double getSelectivity( IndexReference reference ) throws IndexNotFoundKernelException
    {

        return bridge.getKernelTransactionBoundToThisThread( true ).schemaRead().indexUniqueValuesSelectivity( reference );
    }

    private CountsTracker getTracker()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getCounts();
    }

    private void createSomePersons() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            createPersonNode( ktx, "Davide" );
            createPersonNode( ktx, "Stefan" );
            createPersonNode( ktx, "John" );
            createPersonNode( ktx, "John" );
            tx.success();
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

    private IndexReference createPersonNameIndex() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexReference index;
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            try ( Statement ignore = ktx.acquireStatement() )
            {
                int labelId = ktx.tokenWrite().labelGetOrCreateForName( PERSON_LABEL );
                int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( NAME_PROPERTY );
                LabelSchemaDescriptor descriptor = forLabel( labelId, propertyKeyId );
                index = ktx.schemaWrite().indexCreate( descriptor );
            }
            tx.success();
            return index;
        }
    }

    private NeoStores neoStores()
    {
        return ( (GraphDatabaseAPI) db ).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores();
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexesOnline(3, TimeUnit.MINUTES );
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

    private void assertDoubleLongEquals( long expectedUniqueValue, long expectedSampledSize,
                                         DoubleLongRegister register )
    {
        assertEquals( expectedUniqueValue, register.readFirst() );
        assertEquals( expectedSampledSize, register.readSecond() );
    }

    private static void assertCorrectIndexSize( long expected, long actual )
    {
        assertCorrectIndexSize( "", expected, actual );
    }

    private static void assertCorrectIndexSize( String info, long expected, long actual )
    {
        String message = format(
                "Expected number of entries to not differ (expected: %d actual: %d) %s",
                expected, actual, info );
        assertEquals( message, 0L, Math.abs( expected - actual ) );
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

    private static void assertCorrectIndexSelectivity( double expected, double actual )
    {
        String message = format(
                "Expected number of entries to not differ (expected: %f actual: %f)",
                expected, actual );
        assertEquals( message, expected, actual, 0d );
    }

    private static class IndexOnlineMonitor extends IndexingService.MonitorAdapter
    {
        private CountDownLatch updateTrackerCompletionLatch;
        private final CountDownLatch startSignal = new CountDownLatch( 1 );
        private volatile boolean isOnline;
        private Barrier.Control barrier;

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
        public void populationCompleteOn( StoreIndexDescriptor descriptor )
        {
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
