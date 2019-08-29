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
package org.neo4j.kernel.impl.store.id;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.RandomValues;

import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.STRICTLY_PRIORITIZE_FREELIST_NAME;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
@Timeout( value = 20, unit = MINUTES )
class ReuseStorageSpaceIT
{
    // Data size control center
    private static final int DATA_SIZE = 100_000;
    private static final int CREATION_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int NUMBER_OF_TRANSACTIONS_PER_THREAD = 100;

    private static final int CUSTOM_EXIT_CODE = 99;
    private static final String[] TOKENS = {"One", "Two", "Three", "Four", "Five", "Six"};

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomRule random;

    @Test
    void shouldReuseStorageSpaceWhenCreatingDeletingAndRestarting() throws Exception
    {
        shouldReuseStorageSpace(
                Operation.CREATE_DELETE,
                Operation.CREATE_DELETE,
                ReuseStorageSpaceIT::sameProcess );
    }

    @Test
    void shouldReuseStorageSpaceWhenDeletingCreatingAndRestarting() throws Exception
    {
        shouldReuseStorageSpace(
                Operation.CREATE,
                Operation.DELETE_CREATE,
                ReuseStorageSpaceIT::sameProcess );
    }

    @Test
    void shouldReuseStorageSpaceWhenCreatingDeletingAndCrashing() throws Exception
    {
        shouldReuseStorageSpace(
                Operation.CREATE_DELETE,
                Operation.CREATE_DELETE,
                ReuseStorageSpaceIT::crashingChildProcess );
    }

    @Test
    void shouldReuseStorageSpaceWhenDeletingCreatingAndCrashing() throws Exception
    {
        shouldReuseStorageSpace(
                Operation.CREATE,
                Operation.DELETE_CREATE,
                ReuseStorageSpaceIT::crashingChildProcess );
    }

    private void shouldReuseStorageSpace( Operation initialState, Operation operation, Launcher launcher ) throws Exception
    {
        // given the data inserted into a db and knowledge about its size
        File storeDirectory = directory.storeDir();
        long seed = random.seed();
        Sizes initialStoreSizes = withDb( storeDirectory, db -> initialState.perform( db, seed ) );

        // when going into a loop deleting, re-creating, crashing and recovering that db
        for ( int i = 0; i < 4; i++ )
        {
            Pair<Integer,Sizes> result = launcher.launch( storeDirectory, seed, operation );
            assertEquals( CUSTOM_EXIT_CODE, result.getLeft() );
            Sizes storeFileSizesNow = result.getRight();

            Sizes diff = storeFileSizesNow.diffAgainst( initialStoreSizes );
            long storeFilesDiff = diff.sum();
            int round = i;
            assertEquals( 0, storeFilesDiff, () -> format( "Initial sizes %s%n%nStore sizes after operation (round %d)%s%n%nDiff between the two above %s%n",
                    initialStoreSizes, round, storeFileSizesNow, diff ) );
        }
    }

    private static Pair<Integer,Sizes> sameProcess( File storeDirectory, long seed, Operation operation )
    {
        enableStrictPrioritizationOfFreelist();
        try
        {
            return Pair.of( CUSTOM_EXIT_CODE, withDb( storeDirectory, db -> operation.perform( db, seed ) ) );
        }
        finally
        {
            restoreStrictPrioritizationOfFreelist();
        }
    }

    private static Pair<Integer,Sizes> crashingChildProcess( File storeDirectory, long seed, Operation operation ) throws Exception
    {
        // See "main" method in this class
        Process process = new ProcessBuilder( getJavaExecutable().toString(),
                "-cp", getClassPath(), ReuseStorageSpaceIT.class.getCanonicalName(), storeDirectory.getPath(), String.valueOf( seed ), operation.name() )
                .inheritIO()
                .start();

        // then storage size should be comparable (the store part, not the logs and all that)
        int exitCode = process.waitFor();
        Sizes storeFileSizes = withDb( storeDirectory, db -> {} );
        return Pair.of( exitCode, storeFileSizes );
    }

    /**
     * This test spawns sub processes and kills them. This is their main method.
     */
    public static void main( String[] args )
    {
        // No need to restore this later because we exit the JVM anyway
        enableStrictPrioritizationOfFreelist();

        File storeDirectory = new File( args[0] );
        long seed = Long.parseLong( args[1] );
        Operation operation = Operation.valueOf( args[2] );
        withDb( storeDirectory, db ->
        {
            operation.perform( db, seed );
            System.exit( CUSTOM_EXIT_CODE ); // <-- so that we know that we got to the crash correctly
        } );
    }

    private static Sizes withDb( File storeDir, ThrowingConsumer<GraphDatabaseService,Exception> transaction )
    {
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder( storeDir ).build();
        try
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
            transaction.accept( db );
            return new Sizes( db );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    /**
     * The seed will make this method create the same data every time.
     * @param db {@link GraphDatabaseService} db.
     * @param seed starting seed for the randomness.
     */
    private static void createStuff( GraphDatabaseService db, long seed )
    {
        Race race = new Race();
        AtomicLong createdNodes = new AtomicLong();
        AtomicLong createdRelationships = new AtomicLong();
        int dataSizePerTransaction = DATA_SIZE / NUMBER_OF_TRANSACTIONS_PER_THREAD / CREATION_THREADS;
        AtomicLong nextSeed = new AtomicLong( seed );
        race.addContestants( CREATION_THREADS, throwing( () ->
        {
            RandomValues random = RandomValues.create( new Random( nextSeed.getAndIncrement() ) );
            int nodeCount = 0;
            int relationshipCount = 0;
            for ( int t = 0; t < NUMBER_OF_TRANSACTIONS_PER_THREAD; t++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    // Nodes
                    Node[] nodes = new Node[dataSizePerTransaction];
                    for ( int n = 0; n < nodes.length; n++ )
                    {
                        Node node = nodes[n] = db.createNode( labels( random.selection( TOKENS, 0, TOKENS.length, false ) ) );
                        setProperties( random, node );
                        nodeCount++;
                    }

                    // Relationships
                    for ( int r = 0; r < nodes.length; r++ )
                    {
                        Relationship relationship = random.among( nodes ).createRelationshipTo( random.among( nodes ), withName( random.among( TOKENS ) ) );
                        setProperties( random, relationship );
                        relationshipCount++;
                    }
                    tx.commit();
                }
            }
            createdNodes.addAndGet( nodeCount );
            createdRelationships.addAndGet( relationshipCount );
        } ), 1 );
        race.goUnchecked();
    }

    /**
     * Deletes all nodes and relationships and their associated properties from the db, leaving the db effectively empty.
     */
    private static void deleteStuff( GraphDatabaseService db )
    {
        batchedDelete( db, db::getAllRelationships, Relationship::delete );
        batchedDelete( db, db::getAllNodes, Node::delete );
    }

    private static <ENTITY extends PropertyContainer> void batchedDelete( GraphDatabaseService db,
            Supplier<ResourceIterable<ENTITY>> stream, Consumer<ENTITY> deleter )
    {
        int deleted;
        do
        {
            deleted = 0;
            try ( Transaction tx = db.beginTx() )
            {
                try ( ResourceIterator<ENTITY> iterator = stream.get().iterator() )
                {
                    for ( ; iterator.hasNext() && deleted < 10_000; deleted++ )
                    {
                        ENTITY entity = iterator.next();
                        deleter.accept( entity );
                    }
                }
                tx.commit();
            }
        }
        while ( deleted > 0 );
    }

    private static void setProperties( RandomValues random, PropertyContainer entity )
    {
        for ( String propertyKey : random.selection( TOKENS, 0, TOKENS.length, false ) )
        {
            entity.setProperty( propertyKey, random.nextValue().asObject() );
        }
    }

    private static Label[] labels( String[] names )
    {
        Label[] labels = new Label[names.length];
        for ( int i = 0; i < names.length; i++ )
        {
            labels[i] = label( names[i] );
        }
        return labels;
    }

    private static void enableStrictPrioritizationOfFreelist()
    {
        FeatureToggles.set( IndexedIdGenerator.class, STRICTLY_PRIORITIZE_FREELIST_NAME, TRUE );
    }

    private static void restoreStrictPrioritizationOfFreelist()
    {
        FeatureToggles.clear( IndexedIdGenerator.class, STRICTLY_PRIORITIZE_FREELIST_NAME );
    }

    private static class Sizes
    {
        private final Map<String,Long> sizes;

        Sizes( GraphDatabaseAPI db )
        {
            sizes = new HashMap<>();
            IdGeneratorFactory idGeneratorFactory = db.getDependencyResolver().resolveDependency( IdGeneratorFactory.class );
            for ( IdType idType : IdType.values() )
            {
                sizes.put( idType.name(), idGeneratorFactory.get( idType ).getHighId() );
            }
        }

        private Sizes( Map<String,Long> sizes )
        {
            this.sizes = sizes;
        }

        Sizes diffAgainst( Sizes other )
        {
            Map<String,Long> diff = new HashMap<>();
            for ( Map.Entry<String,Long> entry : sizes.entrySet() )
            {
                Long otherSize = other.sizes.get( entry.getKey() );
                if ( otherSize != null )
                {
                    long diffSize = entry.getValue() - otherSize;
                    if ( diffSize != 0 )
                    {
                        diff.put( entry.getKey(), diffSize );
                    }
                }
            }
            return new Sizes( diff );
        }

        @Override
        public String toString()
        {
            List<Map.Entry<String,Long>> nonEmptyEntries = sizes.entrySet().stream()
                    .filter( e -> e.getValue() != 0 )
                    .sorted( comparing( Map.Entry::getKey ) )
                    .collect( Collectors.toList() );
            long sum = sum();
            return format( "SUM %s(%d):%n%s", ByteUnit.bytesToString( sum ), sum, StringUtils.join( nonEmptyEntries, format( "%n" ) ) );
        }

        long sum()
        {
            return sum( all -> true );
        }

        long sum( Predicate<String> filter )
        {
            return sizes.entrySet().stream().filter( e -> filter.test( e.getKey() ) ).mapToLong( Map.Entry::getValue ).sum();
        }
    }

    private enum Operation
    {
        CREATE
                {
                    @Override
                    void perform( GraphDatabaseService db, long seed )
                    {
                        createStuff( db, seed );
                    }
                },
        CREATE_DELETE
                {
                    @Override
                    public void perform( GraphDatabaseService db, long seed )
                    {
                        createStuff( db, seed );
                        deleteStuff( db );
                    }
                },
        DELETE_CREATE
                {
                    @Override
                    public void perform( GraphDatabaseService db, long seed ) throws InterruptedException
                    {
                        deleteStuff( db );
                        // Why park here? Our isolation mechanism works by buffering deleted ids until all transactions that had a chance
                        // to see that deleted version of that particular record have been closed. This decisions happens in the background
                        // and is checked once per second or so, so for the most part this happens seemingly instantaneous.
                        // Although since this test moves on to create immediately after deleting things it may not be enough and therefore
                        // we park here for a couple of maintenance intervals (which is 1s at the time of writing this).
                        // TODO properly monitor this somehow instead of blindly sleeping 2 secs. Although this will probably be difficult
                        //      before the architectural problems of when in the lifecycle the IdGeneratorFactory is created, because ideally
                        //      this would be a monitor on the IdController to see when it has completed maintenance at least one time
                        //      after we deleted all the stuff. Hence the to do.
                        Thread.sleep( 2_000 );
                        createStuff( db, seed );
                    }
                };

        abstract void perform( GraphDatabaseService db, long seed ) throws Exception;
    }

    interface Launcher
    {
        Pair<Integer,Sizes> launch( File storeDirectory, long seed, Operation operation ) throws Exception;
    }
}
