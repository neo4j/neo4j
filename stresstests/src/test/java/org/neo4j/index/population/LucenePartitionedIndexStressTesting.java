/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.population;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;

import static org.apache.commons.lang3.SystemUtils.JAVA_IO_TMPDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LucenePartitionedIndexStressTesting
{
    private static final String LABEL = "label";
    private static final String PROPERTY_PREFIX = "property";
    private static final String UNIQUE_PROPERTY_PREFIX = "uniqueProperty";

    private static final int NUMBER_OF_PROPERTIES = 2;
    private static final int BATCH_SIZE = 1000;

    private static final long NUMBER_OF_NODES = Long.valueOf( getEnvVariable(
            "LUCENE_PARTITIONED_INDEX_NUMBER_OF_NODES", String.valueOf( 1000000 ) ) );
    private static final String WORK_DIRECTORY =
            getEnvVariable( "LUCENE_PARTITIONED_INDEX_WORKING_DIRECTORY", JAVA_IO_TMPDIR );
    private static final int WAIT_DURATION_MINUTES = Integer.valueOf( getEnvVariable(
            "LUCENE_PARTITIONED_INDEX_WAIT_TILL_ONLINE", String.valueOf( 30 ) ) );
    private GraphDatabaseService db;
    private Path storeDir;

    @Before
    public void setUp() throws IOException
    {
        storeDir = getStorePath();
        FileUtils.deletePathRecursively( storeDir );
        System.out.println( String.format( "Starting database at: %s", storeDir ) );

        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir.toFile() )
                .newGraphDatabase();
    }

    @After
    public void tearDown() throws IOException
    {
        db.shutdown();
        FileUtils.deletePathRecursively( storeDir );
    }

    @Test
    public void indexPopulationAndCreationStressTest() throws IOException
    {
        createIndexes();
        createUniqueIndexes();
        long totalNodeCount = populateDatabase();
        findLastNodesByLabelAndProperties( db, totalNodeCount );

        dropAllIndexes();
        createUniqueIndexes();
        createIndexes();
        findLastNodesByLabelAndProperties( db, totalNodeCount );
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

    private long populateDatabase()
    {
        System.out.println( "Starting database population." );
        long populationStart = System.nanoTime();
        long insertedNodes = populateDb( db );

        System.out.println( "Database population completed. Inserted " + insertedNodes + " nodes." );
        System.out.println( "Population took: " + TimeUnit.NANOSECONDS.toMillis( System.nanoTime() -
                                                                                 populationStart ) + " ms." );
        return insertedNodes;
    }

    private void findLastNodesByLabelAndProperties( GraphDatabaseService db, long totalNodes )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            Node nodeByUniqueStringProperty = db.findNode( Label.label( LABEL ), getUniqueStringProperty(),
                    getLastNodePropertyValue( totalNodes ) + "" );
            Node nodeByStringProperty = db.findNode( Label.label( LABEL ), getStringProperty(),
                    (getLastNodePropertyValue( totalNodes ) - 1) + "" );
            assertNotNull( "Should find last inserted node", nodeByStringProperty );
            assertEquals( "Both nodes should be the same last inserted node", nodeByStringProperty,
                    nodeByUniqueStringProperty );

            Node nodeByUniqueLongProperty = db.findNode( Label.label( LABEL ), getUniqueLongProperty(),
                    getLastNodePropertyValue( totalNodes ) );
            Node nodeByLongProperty = db.findNode( Label.label( LABEL ), getLongProperty(),
                    (getLastNodePropertyValue( totalNodes ) - 1) );
            assertNotNull( "Should find last inserted node", nodeByLongProperty );
            assertEquals( "Both nodes should be the same last inserted node", nodeByLongProperty,
                    nodeByUniqueLongProperty );

        }
    }

    private long getLastNodePropertyValue( long insertedNodes )
    {
        return 2 * insertedNodes - 1;
    }

    private Path getStorePath() throws IOException
    {
        return Files.createTempDirectory( Paths.get( WORK_DIRECTORY ), "storeDir" );
    }

    private long populateDb( GraphDatabaseService db )
    {
        long insertedNodes = 0;

        SequentialLongSupplier longSupplier = new SequentialLongSupplier();
        SequentialStringSupplier stringSupplier = new SequentialStringSupplier();

        while ( insertedNodes < NUMBER_OF_NODES )
        {
            insertedNodes += insertBatchNodes( db, stringSupplier, longSupplier );
            if ( insertedNodes % 1_000_000 == 0 )
            {
                System.out.println( "Inserted " + insertedNodes + " nodes." );
            }
        }
        return insertedNodes;
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

    private int insertBatchNodes( GraphDatabaseService db, Supplier<String> stringValueSupplier,
            LongSupplier longSupplier )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( int i = 0; i < BATCH_SIZE; i++ )
            {
                Node node = db.createNode( Label.label( LABEL ) );

                node.setProperty( getStringProperty(), stringValueSupplier.get() );
                node.setProperty( getLongProperty(), longSupplier.getAsLong() );

                node.setProperty( getUniqueStringProperty(), stringValueSupplier.get() );
                node.setProperty( getUniqueLongProperty(), longSupplier.getAsLong() );
            }
            transaction.success();
        }
        return BATCH_SIZE;
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


    private static String getEnvVariable( String propertyName, String defaultValue )
    {
        String value = System.getenv( propertyName );
        return StringUtils.defaultIfEmpty( value, defaultValue );
    }

    private static class SequentialStringSupplier implements Supplier<String>
    {
        long value = 0;

        @Override
        public String get()
        {
            return value++ + "";
        }
    }

    private static class SequentialLongSupplier implements LongSupplier
    {

        long value = 0;

        @Override
        public long getAsLong()
        {
            return value++;
        }
    }

}
