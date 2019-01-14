/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.index.population;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.apache.commons.lang3.SystemUtils.JAVA_IO_TMPDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

public class LucenePartitionedIndexStressTesting
{
    private static final String LABEL = "label";
    private static final String PROPERTY_PREFIX = "property";
    private static final String UNIQUE_PROPERTY_PREFIX = "uniqueProperty";

    private static final int NUMBER_OF_PROPERTIES = 2;

    private static final int NUMBER_OF_POPULATORS =
            Integer.valueOf( fromEnv( "LUCENE_INDEX_NUMBER_OF_POPULATORS",
                    String.valueOf( Runtime.getRuntime().availableProcessors() - 1 ) ) );
    private static final int BATCH_SIZE = Integer.valueOf( fromEnv( "LUCENE_INDEX_POPULATION_BATCH_SIZE",
            String.valueOf( 10000 ) ) );

    private static final long NUMBER_OF_NODES = Long.valueOf( fromEnv(
            "LUCENE_PARTITIONED_INDEX_NUMBER_OF_NODES", String.valueOf( 100000 ) ) );
    private static final String WORK_DIRECTORY =
            fromEnv( "LUCENE_PARTITIONED_INDEX_WORKING_DIRECTORY", JAVA_IO_TMPDIR );
    private static final int WAIT_DURATION_MINUTES = Integer.valueOf( fromEnv(
            "LUCENE_PARTITIONED_INDEX_WAIT_TILL_ONLINE", String.valueOf( 30 ) ) );

    private ExecutorService populators;
    private GraphDatabaseService db;
    private File storeDir;

    @Before
    public void setUp() throws IOException
    {
        storeDir = prepareStoreDir();
        System.out.println( String.format( "Starting database at: %s", storeDir ) );

        populators = Executors.newFixedThreadPool( NUMBER_OF_POPULATORS );
        db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                                           .newGraphDatabase();
    }

    @After
    public void tearDown() throws IOException
    {
        db.shutdown();
        populators.shutdown();
        FileUtils.deleteRecursively( storeDir );
    }

    @Test
    public void indexCreationStressTest() throws Exception
    {
        createIndexes();
        createUniqueIndexes();
        PopulationResult populationResult = populateDatabase();
        findLastTrackedNodesByLabelAndProperties( db, populationResult );
        dropAllIndexes();

        createUniqueIndexes();
        createIndexes();
        findLastTrackedNodesByLabelAndProperties( db, populationResult );
    }

    private void dropAllIndexes()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.getConstraints().forEach( ConstraintDefinition::drop );
            schema.getIndexes().forEach( IndexDefinition::drop );
            transaction.success();
        }
    }

    private void createIndexes()
    {
        createIndexes( false );
    }

    private void createUniqueIndexes()
    {
        createIndexes( true );
    }

    private void createIndexes( boolean unique )
    {
        System.out.println( String.format( "Creating %d%s indexes.", NUMBER_OF_PROPERTIES, unique ? " unique" : "" ) );
        long creationStart = System.nanoTime();
        createAndWaitForIndexes( unique );
        System.out.println( String.format( "%d%s indexes created.", NUMBER_OF_PROPERTIES, unique ? " unique" : "" ) );
        System.out.println( "Creation took: " + TimeUnit.NANOSECONDS.toMillis( System.nanoTime() -
                                                                               creationStart ) + " ms." );
    }

    private PopulationResult populateDatabase() throws ExecutionException, InterruptedException
    {
        System.out.println( "Starting database population." );
        long populationStart = System.nanoTime();
        PopulationResult populationResult = populateDb( db );

        System.out.println( "Database population completed. Inserted " + populationResult.numberOfNodes + " nodes." );
        System.out.println( "Population took: " + TimeUnit.NANOSECONDS.toMillis( System.nanoTime() -
                                                                                 populationStart ) + " ms." );
        return populationResult;
    }

    private void findLastTrackedNodesByLabelAndProperties( GraphDatabaseService db, PopulationResult populationResult )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            Node nodeByUniqueStringProperty = db.findNode( Label.label( LABEL ), getUniqueStringProperty(),
                    populationResult.maxPropertyId + "" );
            Node nodeByStringProperty = db.findNode( Label.label( LABEL ), getStringProperty(),
                    populationResult.maxPropertyId + "" );
            assertNotNull( "Should find last inserted node", nodeByStringProperty );
            assertEquals( "Both nodes should be the same last inserted node", nodeByStringProperty,
                    nodeByUniqueStringProperty );

            Node nodeByUniqueLongProperty = db.findNode( Label.label( LABEL ), getUniqueLongProperty(),
                    populationResult.maxPropertyId );
            Node nodeByLongProperty = db.findNode( Label.label( LABEL ), getLongProperty(),
                    populationResult.maxPropertyId );
            assertNotNull( "Should find last inserted node", nodeByLongProperty );
            assertEquals( "Both nodes should be the same last inserted node", nodeByLongProperty,
                    nodeByUniqueLongProperty );

        }
    }

    private File prepareStoreDir() throws IOException
    {
        Path storeDirPath = Paths.get( WORK_DIRECTORY ).resolve( Paths.get( "storeDir" ) );
        File storeDirectory = storeDirPath.toFile();
        FileUtils.deleteRecursively( storeDirectory );
        storeDirectory.deleteOnExit();
        return storeDirectory;
    }

    private PopulationResult populateDb( GraphDatabaseService db ) throws ExecutionException, InterruptedException
    {
        AtomicLong nodesCounter = new AtomicLong();

        List<Future<Long>> futures = new ArrayList<>( NUMBER_OF_POPULATORS );
        for ( int i = 0; i < NUMBER_OF_POPULATORS; i++ )
        {
            futures.add( populators.submit( new Populator( i, NUMBER_OF_POPULATORS, db, nodesCounter ) ) );
        }

        long maxPropertyId = 0;
        for ( Future<Long> future : futures )
        {
            maxPropertyId = Math.max( maxPropertyId, future.get() );
        }
        return new PopulationResult( maxPropertyId, nodesCounter.get() );
    }

    private void createAndWaitForIndexes( boolean unique )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( int i = 0; i < NUMBER_OF_PROPERTIES; i++ )
            {
                if ( unique )
                {
                    createUniqueConstraint( i );
                }
                else
                {
                    createIndex( i );
                }
            }
            transaction.success();
        }
        awaitIndexesOnline( db );
    }

    private void createUniqueConstraint( int index )
    {
        db.schema().constraintFor( Label.label( LABEL ) ).assertPropertyIsUnique( UNIQUE_PROPERTY_PREFIX + index )
                .create();
    }

    private void createIndex( int index )
    {
        db.schema().indexFor( Label.label( LABEL ) ).on( PROPERTY_PREFIX + index ).create();
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.awaitIndexesOnline( WAIT_DURATION_MINUTES, TimeUnit.MINUTES );
        }
    }

    private static String getLongProperty()
    {
        return PROPERTY_PREFIX + 1;
    }

    private static String getStringProperty()
    {
        return PROPERTY_PREFIX + 0;
    }

    private static String getUniqueLongProperty()
    {
        return UNIQUE_PROPERTY_PREFIX + 1;
    }

    private static String getUniqueStringProperty()
    {
        return UNIQUE_PROPERTY_PREFIX + 0;
    }

    private static class SequentialStringSupplier implements Supplier<String>
    {
        private final int step;
        long value;

        SequentialStringSupplier( int populatorNumber, int step )
        {
            this.value = populatorNumber;
            this.step = step;
        }

        @Override
        public String get()
        {
            value += step;
            return value + "";
        }
    }

    private static class SequentialLongSupplier implements LongSupplier
    {
        long value;
        private int step;

        SequentialLongSupplier( int populatorNumber, int step )
        {
            value = populatorNumber;
            this.step = step;
        }

        @Override
        public long getAsLong()
        {
            value += step;
            return value;
        }
    }

    private static class Populator implements Callable<Long>
    {
        private final int populatorNumber;
        private final int step;
        private GraphDatabaseService db;
        private AtomicLong nodesCounter;

        Populator( int populatorNumber, int step, GraphDatabaseService db, AtomicLong nodesCounter )
        {
            this.populatorNumber = populatorNumber;
            this.step = step;
            this.db = db;
            this.nodesCounter = nodesCounter;
        }

        @Override
        public Long call()
        {
            SequentialLongSupplier longSupplier = new SequentialLongSupplier( populatorNumber, step );
            SequentialStringSupplier stringSupplier = new SequentialStringSupplier( populatorNumber, step );

            while ( nodesCounter.get() < NUMBER_OF_NODES )
            {
                long nodesInTotal = nodesCounter.addAndGet( insertBatchNodes( db, stringSupplier, longSupplier ) );
                if ( nodesInTotal % 1_000_000 == 0 )
                {
                    System.out.println( "Inserted " + nodesInTotal + " nodes." );
                }
            }
            return longSupplier.value;
        }

        private int insertBatchNodes( GraphDatabaseService db, Supplier<String> stringValueSupplier,
                LongSupplier longSupplier )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                for ( int i = 0; i < BATCH_SIZE; i++ )
                {
                    Node node = db.createNode( Label.label( LABEL ) );

                    String stringValue = stringValueSupplier.get();
                    long longValue = longSupplier.getAsLong();

                    node.setProperty( getStringProperty(), stringValue );
                    node.setProperty( getLongProperty(), longValue );

                    node.setProperty( getUniqueStringProperty(), stringValue );
                    node.setProperty( getUniqueLongProperty(), longValue );
                }
                transaction.success();
            }
            return BATCH_SIZE;
        }
    }

    private class PopulationResult
    {
        private long maxPropertyId;
        private long numberOfNodes;

        PopulationResult( long maxPropertyId, long numberOfNodes )
        {
            this.maxPropertyId = maxPropertyId;
            this.numberOfNodes = numberOfNodes;
        }
    }
}
