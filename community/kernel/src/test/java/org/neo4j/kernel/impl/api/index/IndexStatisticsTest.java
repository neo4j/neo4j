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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

/**
 * This test validates that we count the correct amount of index updates.
 * <p>
 * We build the index async with a node scan in the background and consume transactions to keep the index updated. Once
 * the index is build and becomes online, we save the index size(number of entries) and begin tracking updates. These
 * values can then then be used to determine when to resample the index for example.
 *
 * <pre>
 *                online
 *    index size    |      updates
 *                  v
 *  |----------------T--------------> stream of transactions
 *                    ^
 *    during          |    after
 *                 observed
 * </pre>
 *
 * Since we observe the index online event without strict synchronization, we cannot determine what transactions are
 * before and after that event. This is illustrated in the drawing above where transaction {@code T} is counted as
 * occurring during the population and not after, which is incorrect.
 * <p>
 * That is why we allow a difference of {@link IndexStatisticsTest#MISSED_UPDATES_TOLERANCE}, to exists. This threshold
 * is the number of updates in the larges transaction and that should be the larges error since we check if the index is
 * online between each transaction.
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
    private static final int MISSED_UPDATES_TOLERANCE = NAMES.length;
    private static final double DOUBLE_ERROR_TOLERANCE = 0.00001d;

    @Parameter
    public boolean multiThreadedPopulationEnabled;

    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule()
            .withSetting( GraphDatabaseSettings.index_background_sampling_enabled, "false" )
            .withSetting( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled,
                    multiThreadedPopulationEnabled + "" );

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
        GraphDatabaseAPI graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        this.db = graphDatabaseAPI;
        DependencyResolver dependencyResolver = graphDatabaseAPI.getDependencyResolver();
        this.bridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( Monitors.class )
                .addMonitorListener( indexOnlineMonitor );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedWhenPopulationBeforeTheIndexIsOnline() throws KernelException
    {
        // given
        createSomePersons();

        // when
        IndexReference index = createIndex( "Person", "name" );
        awaitIndexesOnline();

        // then
        assertEquals( 0.75d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 4L, indexSize( index ) );
        assertEquals( 0L, indexUpdates( index ) );
    }

    @Test
    public void shouldNotSeeDataCreatedAfterPopulation() throws KernelException
    {
        // given
        IndexReference index = createIndex( "Person", "name" );
        awaitIndexesOnline();

        // when
        createSomePersons();

        // then
        assertEquals( 1.0d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 0L, indexSize( index ) );
        assertEquals( 4L, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataSeenDuringPopulationAndIgnoreDataCreatedAfterPopulation()
            throws KernelException
    {
        // given
        createSomePersons();
        IndexReference index = createIndex( "Person", "name" );
        awaitIndexesOnline();

        // when
        createSomePersons();

        // then
        assertEquals( 0.75d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 4L, indexSize( index ) );
        assertEquals( 4L, indexUpdates( index ) );
    }

    @Test
    public void shouldRemoveIndexStatisticsAfterIndexIsDeleted() throws KernelException
    {
        // given
        createSomePersons();
        IndexReference index = createIndex( "Person", "name" );
        awaitIndexesOnline();

        SchemaStorage storage = new SchemaStorage( neoStores().getSchemaStore() );
        long indexId = storage.indexGetForSchema( DefaultIndexReference.toDescriptor( index ) ).getId();

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
        int created = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when
        IndexReference index = createIndex( "Person", "name" );
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
        int initialNodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when populating while creating
        IndexReference index = createIndex( "Person", "name" );
        final UpdatesTracker updatesTracker = executeCreations( index, CREATION_MULTIPLIER );
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
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexReference index = createIndex( "Person", "name" );
        UpdatesTracker updatesTracker = executeCreationsAndDeletions( nodes, index, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
        int seenWhilePopulating =
                initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
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
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexReference index = createIndex( "Person", "name" );
        UpdatesTracker updatesTracker = executeCreationsAndUpdates( nodes, index, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
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
        long[] nodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER );
        int initialNodes = nodes.length;

        // when populating while creating
        IndexReference index = createIndex( "Person", "name" );
        UpdatesTracker updatesTracker = executeCreationsDeletionsAndUpdates( nodes, index, CREATION_MULTIPLIER );
        awaitIndexesOnline();

        // then
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
        ExecutorService executorService = Executors.newFixedThreadPool( threads );

        // when populating while creating
        final IndexReference index = createIndex( "Person", "name" );

        final Collection<Callable<UpdatesTracker>> jobs = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            jobs.add( () -> executeCreationsDeletionsAndUpdates( nodes, index, CREATION_MULTIPLIER ) );
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

        executorService.awaitTermination( 1, TimeUnit.SECONDS );
        executorService.shutdown();

        // then
        int tolerance = MISSED_UPDATES_TOLERANCE * threads;
        double doubleTolerance = DOUBLE_ERROR_TOLERANCE * threads;
        int seenWhilePopulating = initialNodes + result.createdDuringPopulation() - result.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / seenWhilePopulating;
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ), doubleTolerance );
        assertCorrectIndexSize( "Tracker had " + result, seenWhilePopulating, indexSize( index ), tolerance );
        int expectedIndexUpdates = result.deletedAfterPopulation() + result.createdAfterPopulation() + result.updatedAfterPopulation();
        assertCorrectIndexUpdates( "Tracker had " + result, expectedIndexUpdates, indexUpdates( index ), tolerance );
    }

    private void deleteNode( long nodeId ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( nodeId ).delete();
            tx.success();
        }
    }

    private boolean changeName( long nodeId, String propertyKeyName, Object newValue )
    {
        boolean changeIndexedNode = false;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            Object oldValue = node.getProperty( propertyKeyName );
            if ( !oldValue.equals( newValue ) )
            {
                // Changes are only propagated when the value actually change
                changeIndexedNode = true;
            }
            node.setProperty( propertyKeyName, newValue );
            tx.success();
        }
        return changeIndexedNode;
    }

    private int createNamedPeople( long[] nodes, int offset ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( String name : NAMES )
            {
                long nodeId = createNode( bridge.getKernelTransactionBoundToThisThread( true ), "Person", "name", name );
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
                                      .indexUpdatesAndSize( forLabel( reference.label(), reference.properties() ) ).readSecond();
    }

    private long indexUpdates( IndexReference reference  ) throws KernelException
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( IndexingService.class )
                                      .indexUpdatesAndSize( forLabel( reference.label(), reference.properties() ) ).readFirst();
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
            createNode( ktx, "Person", "name", "Davide" );
            createNode( ktx, "Person", "name", "Stefan" );
            createNode( ktx, "Person", "name", "John" );
            createNode( ktx, "Person", "name", "John" );
            tx.success();
        }
    }

    private long createNode( KernelTransaction ktx, String labelName, String propertyKeyName, Object value )
            throws KernelException
    {
        int labelId = ktx.tokenWrite().labelGetOrCreateForName( labelName );
        int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyKeyName );
        long nodeId = ktx.dataWrite().nodeCreate();
        ktx.dataWrite().nodeAddLabel( nodeId, labelId );
        ktx.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( value ) );
        return nodeId;
    }

    private IndexReference createIndex( String labelName, String propertyKeyName ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexReference index;
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            try ( Statement ignore = ktx.acquireStatement() )
            {
                int labelId = ktx.tokenWrite().labelGetOrCreateForName( labelName );
                int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyKeyName );
                LabelSchemaDescriptor descriptor = forLabel( labelId, propertyKeyId );
                index = ktx.schemaWrite().indexCreate( descriptor, null );
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

    private UpdatesTracker executeCreations( IndexReference index, int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( null, index, numberOfCreations, false, false );
    }

    private UpdatesTracker executeCreationsAndDeletions( long[] nodes,
                                                         IndexReference index,
                                                         int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, true, false );
    }

    private UpdatesTracker executeCreationsAndUpdates( long[] nodes,
                                                       IndexReference index,
                                                       int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, false, true );
    }

    private UpdatesTracker executeCreationsDeletionsAndUpdates( long[] nodes,
                                                                IndexReference index,
                                                                int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, true, true );
    }

    private UpdatesTracker internalExecuteCreationsDeletionsAndUpdates( long[] nodes,
                                                                        IndexReference index,
                                                                        int numberOfCreations,
                                                                        boolean allowDeletions,
                                                                        boolean allowUpdates ) throws KernelException
    {
        Random random = ThreadLocalRandom.current();
        UpdatesTracker updatesTracker = new UpdatesTracker();
        int offset = 0;
        while ( updatesTracker.created() < numberOfCreations )
        {
            int created = createNamedPeople( nodes, offset );
            offset += created;
            updatesTracker.increaseCreated( created );
            notifyIfPopulationCompleted( index, updatesTracker );

            // delete if allowed
            if ( allowDeletions && updatesTracker.created() % 24 == 0 )
            {
                long nodeId = nodes[random.nextInt( nodes.length )];
                try
                {
                    deleteNode( nodeId );
                    updatesTracker.increaseDeleted( 1 );
                }
                catch ( EntityNotFoundException | NotFoundException ex )
                {
                    // ignore
                }
                notifyIfPopulationCompleted( index, updatesTracker );
            }

            // update if allowed
            if ( allowUpdates && updatesTracker.created() % 24 == 0 )
            {
                int randomIndex = random.nextInt( nodes.length );
                try
                {
                    if ( changeName( nodes[randomIndex], "name", NAMES[random.nextInt( NAMES.length )] ) )
                    {
                        updatesTracker.increaseUpdated( 1 );
                    }
                }
                catch ( NotFoundException ex )
                {
                    // ignore
                }
                notifyIfPopulationCompleted( index, updatesTracker );
            }
        }
        // make sure population complete has been notified
        updatesTracker.notifyPopulationCompleted();
        return updatesTracker;
    }

    private void notifyIfPopulationCompleted( IndexReference index, UpdatesTracker updatesTracker )
    {
        if ( isCompletedPopulation( index, updatesTracker ) )
        {
            updatesTracker.notifyPopulationCompleted();
        }
    }

    private boolean isCompletedPopulation( IndexReference index, UpdatesTracker updatesTracker )
    {
        return !updatesTracker.isPopulationCompleted() &&
                indexOnlineMonitor.isIndexOnline( index );
    }

    private void assertDoubleLongEquals( long expectedUniqueValue, long expectedSampledSize,
                                         DoubleLongRegister register )
    {
        assertEquals( expectedUniqueValue, register.readFirst() );
        assertEquals( expectedSampledSize, register.readSecond() );
    }

    private static void assertCorrectIndexSize( long expected, long actual )
    {
        assertCorrectIndexSize( "", expected, actual, MISSED_UPDATES_TOLERANCE );
    }

    private static void assertCorrectIndexSize( String info, long expected, long actual, int tolerance )
    {
        String message = String.format(
                "Expected number of entries to not differ by more than %d (expected: %d actual: %d) %s",
                tolerance, expected, actual, info
        );
        assertTrue( message, Math.abs( expected - actual ) <= tolerance );
    }

    private static void assertCorrectIndexUpdates( long expected, long actual )
    {
        assertCorrectIndexUpdates( "", expected, actual, MISSED_UPDATES_TOLERANCE );
    }

    private static void assertCorrectIndexUpdates( String info, long expected, long actual, int tolerance )
    {
        String message = String.format(
                "Expected number of index updates to not differ by more than %d (expected: %d actual: %d). %s",
                tolerance, expected, actual, info
        );
        assertTrue( message, Math.abs( expected - actual ) <= tolerance );
    }

    private static void assertCorrectIndexSelectivity( double expected, double actual )
    {
        assertCorrectIndexSelectivity( expected, actual, DOUBLE_ERROR_TOLERANCE );
    }

    private static void assertCorrectIndexSelectivity( double expected, double actual, double tolerance )
    {
        String message = String.format(
                "Expected number of entries to not differ by more than %f (expected: %f actual: %f)",
                tolerance, expected, actual
        );
        assertEquals( message, expected, actual, tolerance );
    }

    private static class IndexOnlineMonitor extends IndexingService.MonitorAdapter
    {
        private final Set<IndexReference> onlineIndexes = Collections.newSetFromMap( new ConcurrentHashMap<>() );

        @Override
        public void populationCompleteOn( SchemaIndexDescriptor descriptor )
        {
            onlineIndexes.add( DefaultIndexReference.fromDescriptor( descriptor ) );
        }

        public boolean isIndexOnline( IndexReference descriptor )
        {
            return onlineIndexes.contains( descriptor );
        }
    }
}
