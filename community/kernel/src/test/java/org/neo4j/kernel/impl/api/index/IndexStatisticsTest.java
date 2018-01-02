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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexStatisticsTest
{
    @Test
    public void shouldProvideIndexStatisticsForDataCreatedWhenPopulationBeforeTheIndexIsOnline() throws KernelException
    {
        // given
        createSomePersons();

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // then
        assertEquals( 0.75d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 4l, indexSize( index ) );
        assertEquals( 0l, indexUpdates( index ) );
    }

    @Test
    public void shouldNotSeeDataCreatedAfterPopulation() throws KernelException
    {
        // given
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // when
        createSomePersons();

        // then
        assertEquals( 1.0d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 0l, indexSize( index ) );
        assertEquals( 4l, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataSeenDuringPopulationAndIgnoreDataCreatedAfterPopulation()
            throws KernelException
    {
        // given
        createSomePersons();
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // when
        createSomePersons();

        // then
        assertEquals( 0.75d, indexSelectivity( index ), DOUBLE_ERROR_TOLERANCE );
        assertEquals( 4l, indexSize( index ) );
        assertEquals( 4l, indexUpdates( index ) );
    }

    @Test
    public void shouldRemoveIndexStatisticsAfterIndexIsDeleted() throws KernelException
    {
        // given
        createSomePersons();
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

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
            DoubleLongRegister actual = getTracker()
                    .indexSample( index.getLabelId(), index.getPropertyKeyId(), Registers.newDoubleLongRegister() );
            assertDoubleLongEquals( 0l, 0l, actual );
        }

        // and then index size and index updates are zero on disk
        DoubleLongRegister actual = getTracker()
                .indexUpdatesAndSize( index.getLabelId(), index.getPropertyKeyId(), Registers.newDoubleLongRegister() );
        assertDoubleLongEquals( 0l, 0l, actual );
    }

    @Test
    public void shouldProvideIndexSelectivityWhenThereAreManyDuplicates() throws Exception
    {
        // given some initial data
        int created = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // then
        double expectedSelectivity = UNIQUE_NAMES / (created);
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( created, indexSize( index ) );
        assertEquals( 0l, indexUpdates( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditions() throws Exception
    {
        // given some initial data
        int initialNodes = repeatCreateNamedPeopleFor( NAMES.length * CREATION_MULTIPLIER ).length;

        // when populating while creating
        IndexDescriptor index = createIndex( "Person", "name" );
        final UpdatesTracker updatesTracker = executeCreations( index, CREATION_MULTIPLIER );
        awaitOnline( index );

        // then
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / (seenWhilePopulating);
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
        IndexDescriptor index = createIndex( "Person", "name" );
        UpdatesTracker updatesTracker = executeCreationsAndDeletions( nodes, index, CREATION_MULTIPLIER );
        awaitOnline( index );

        // then
        int seenWhilePopulating =
                initialNodes + updatesTracker.createdDuringPopulation() - updatesTracker.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / (seenWhilePopulating);
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
        IndexDescriptor index = createIndex( "Person", "name" );
        UpdatesTracker updatesTracker = executeCreationsAndUpdates( nodes, index, CREATION_MULTIPLIER );
        awaitOnline( index );

        // then
        int seenWhilePopulating = initialNodes + updatesTracker.createdDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / (seenWhilePopulating);
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
        assertCorrectIndexSize( seenWhilePopulating, indexSize( index ) );
        assertCorrectIndexUpdates( updatesTracker.createdAfterPopulation(), indexUpdates( index ) );
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
        final IndexDescriptor index = createIndex( "Person", "name" );

        final Collection<Callable<UpdatesTracker>> jobs = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            jobs.add( new Callable<UpdatesTracker>()
            {
                @Override
                public UpdatesTracker call() throws Exception
                {
                    return executeCreationsDeletionsAndUpdates( nodes, index, CREATION_MULTIPLIER );
                }
            } );
        }

        List<Future<UpdatesTracker>> futures = executorService.invokeAll( jobs );
        // sum result into empty result
        UpdatesTracker result = new UpdatesTracker();
        result.notifyPopulationCompleted();
        for ( Future<UpdatesTracker> future : futures )
        {
            result.add( future.get() );
        }
        awaitOnline( index );

        executorService.awaitTermination( 1, TimeUnit.SECONDS );
        executorService.shutdown();

        // then
        int tolerance = MISSED_UPDATES_TOLERANCE * threads;
        double doubleTolerance = DOUBLE_ERROR_TOLERANCE * threads;
        int seenWhilePopulating = initialNodes + result.createdDuringPopulation() - result.deletedDuringPopulation();
        double expectedSelectivity = UNIQUE_NAMES / (seenWhilePopulating);
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ), doubleTolerance );
        assertCorrectIndexSize( "Tracker had " + result, seenWhilePopulating, indexSize( index ), tolerance );
        int expectedIndexUpdates = result.deletedAfterPopulation() + result.createdAfterPopulation();
        assertCorrectIndexUpdates( "Tracker had " + result, expectedIndexUpdates, indexUpdates( index ), tolerance );
    }

    private void deleteNode( long nodeId ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            statement.dataWriteOperations().nodeDelete( nodeId );
            tx.success();
        }
    }

    private void changeName( long nodeId, String propertyKeyName, Object newValue ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
            statement.dataWriteOperations().nodeSetProperty( nodeId, Property.property( propertyKeyId, newValue ) );
            tx.success();
        }
    }

    private int createNamedPeople( long[] nodes, int offset ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            for ( String name : NAMES )
            {
                long nodeId = createNode( statement, "Person", "name", name );
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

            jobs.add( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
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
                }
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
            Statement statement = bridge.get();
            statement.schemaWriteOperations().indexDrop( index );
            tx.success();
        }
    }

    private long indexSize( IndexDescriptor descriptor ) throws KernelException
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( NeoStoreDataSource.class )
                                      .getIndexService()
                                      .indexUpdatesAndSize( descriptor ).readSecond();
    }

    private long indexUpdates( IndexDescriptor descriptor ) throws KernelException
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( NeoStoreDataSource.class )
                                      .getIndexService()
                                      .indexUpdatesAndSize( descriptor ).readFirst();
    }

    private double indexSelectivity( IndexDescriptor descriptor ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            double selectivity = statement.readOperations().indexUniqueValuesSelectivity( descriptor );
            tx.success();
            return selectivity;
        }
    }

    private CountsTracker getTracker()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
    }

    private void createSomePersons() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            createNode( statement, "Person", "name", "Davide" );
            createNode( statement, "Person", "name", "Stefan" );
            createNode( statement, "Person", "name", "John" );
            createNode( statement, "Person", "name", "John" );
            tx.success();
        }
    }

    private long createNode( Statement statement, String labelName, String propertyKeyName, Object value )
            throws KernelException
    {
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( labelName );
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
        statement.dataWriteOperations().nodeSetProperty( nodeId, Property.property( propertyKeyId, value ) );
        return nodeId;
    }

    private IndexDescriptor createIndex( String labelName, String propertyKeyName ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.get();
            int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( labelName );
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
            IndexDescriptor index = statement.schemaWriteOperations().indexCreate( labelId, propertyKeyId );
            tx.success();
            return index;
        }
    }

    private IndexDescriptor awaitOnline( IndexDescriptor index ) throws KernelException
    {
        long start = System.currentTimeMillis();
        long end = start + 20_000;
        while ( System.currentTimeMillis() < end )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Statement statement = bridge.get();
                switch ( statement.readOperations().indexGetState( index ) )
                {
                case ONLINE:
                    return index;

                case FAILED:
                    throw new IllegalStateException( "Index failed instead of becoming ONLINE" );

                default:
                    break;
                }
                tx.success();

                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // ignored
                }
            }
        }
        throw new IllegalStateException( "Index did not become ONLINE within reasonable time" );
    }

    private UpdatesTracker executeCreations( IndexDescriptor index, int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( null, index, numberOfCreations, false, false );
    }

    private UpdatesTracker executeCreationsAndDeletions( long[] nodes,
                                                         IndexDescriptor index,
                                                         int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, true, false );
    }

    private UpdatesTracker executeCreationsAndUpdates( long[] nodes,
                                                       IndexDescriptor index,
                                                       int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, false, true );
    }


    private UpdatesTracker executeCreationsDeletionsAndUpdates( long[] nodes,
                                                                IndexDescriptor index,
                                                                int numberOfCreations ) throws KernelException
    {
        return internalExecuteCreationsDeletionsAndUpdates( nodes, index, numberOfCreations, true, true );
    }

    private UpdatesTracker internalExecuteCreationsDeletionsAndUpdates( long[] nodes,
                                                                        IndexDescriptor index,
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

            // check index online
            if ( !updatesTracker.isPopulationCompleted() && indexOnlineMonitor.isIndexOnline( index ) )
            {
                updatesTracker.notifyPopulationCompleted();
            }

            // delete if allowed
            if ( allowDeletions && updatesTracker.created() % 5 == 0 )
            {
                long nodeId = nodes[random.nextInt( nodes.length )];
                try
                {
                    deleteNode( nodeId );
                    updatesTracker.increaseDeleted( 1 );
                }
                catch ( EntityNotFoundException ex )
                {
                    // ignore
                }

                // check again index online
                if ( !updatesTracker.isPopulationCompleted() && indexOnlineMonitor.isIndexOnline( index ) )
                {
                    updatesTracker.notifyPopulationCompleted();
                }
            }

            // update if allowed
            if ( allowUpdates && updatesTracker.created() % 5 == 0 )
            {
                int randomIndex = random.nextInt( nodes.length );
                try
                {
                    changeName( nodes[randomIndex], "name", NAMES[randomIndex % NAMES.length] );
                }
                catch ( EntityNotFoundException ex )
                {
                    // ignore
                }

                // check again index online
                if ( !updatesTracker.isPopulationCompleted() && indexOnlineMonitor.isIndexOnline( index ) )
                {
                    updatesTracker.notifyPopulationCompleted();
                }
            }
        }
        // make sure population complete has been notified
        updatesTracker.notifyPopulationCompleted();
        return updatesTracker;
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

    private static final double UNIQUE_NAMES = 10.0;
    private static final String[] NAMES = new String[]{
            "Andres", "Davide", "Jakub", "Chris", "Tobias", "Stefan", "Petra", "Rickard", "Mattias", "Emil", "Chris",
            "Chris"
    };

    private static final int CREATION_MULTIPLIER = 10_000;
    private static final int MISSED_UPDATES_TOLERANCE = NAMES.length;
    private static final double DOUBLE_ERROR_TOLERANCE = 0.00001d;

    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            // make sure we don't sample in these tests
            builder.setConfig( GraphDatabaseSettings.index_background_sampling_enabled, "false" );
        }
    };

    private GraphDatabaseService db;
    private ThreadToStatementContextBridge bridge;
    private final IndexOnlineMonitor indexOnlineMonitor = new IndexOnlineMonitor();

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

    private static class IndexOnlineMonitor extends IndexingService.MonitorAdapter
    {
        private final Set<IndexDescriptor> onlineIndexes = new HashSet<>();

        @Override
        public void populationCompleteOn( IndexDescriptor descriptor )
        {
            onlineIndexes.add( descriptor );
        }

        public boolean isIndexOnline( IndexDescriptor descriptor )
        {
            return onlineIndexes.contains( descriptor );
        }
    }
}
