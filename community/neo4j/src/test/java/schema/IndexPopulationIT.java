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
package schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@ExtendWith( TestDirectoryExtension.class )
class IndexPopulationIT
{
    @Resource
    private TestDirectory directory;

    private static final int TEST_TIMEOUT = 120_000;
    private static GraphDatabaseService database;
    private static ExecutorService executorService;

    @BeforeEach
    void setUp()
    {
        database = new GraphDatabaseFactory().newEmbeddedDatabase( directory.graphDbDir() );
        executorService = newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
        database.shutdown();
    }

    @Test
    void indexCreationDoNotBlockQueryExecutions() throws Exception
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () -> {
            Label nodeLabel = label( "nodeLabel" );
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode( nodeLabel );
                transaction.success();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                database.schema().indexFor( label( "testLabel" ) ).on( "testProperty" ).create();

                Future<Number> countFuture = executorService.submit( countNodes() );
                assertEquals( 1, countFuture.get().intValue() );

                transaction.success();
            }
        } );
    }

    @Test
    void createIndexesFromDifferentTransactionsWithoutBlocking() throws ExecutionException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () -> {
            long numberOfIndexesBeforeTest = countIndexes();
            Label nodeLabel = label( "nodeLabel2" );
            String testProperty = "testProperty";
            try ( Transaction transaction = database.beginTx() )
            {
                database.schema().indexFor( label( "testLabel2" ) ).on( testProperty ).create();

                Future<?> creationFuture =
                        executorService.submit( createIndexForLabelAndProperty( nodeLabel, testProperty ) );
                creationFuture.get();
                transaction.success();
            }
            waitForOnlineIndexes();

            assertEquals( numberOfIndexesBeforeTest + 2, countIndexes() );
        } );
    }

    @Test
    void indexCreationDoNotBlockWritesOnOtherLabel() throws ExecutionException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () -> {
            Label markerLabel = label( "testLabel3" );
            Label nodesLabel = label( "testLabel4" );
            try ( Transaction transaction = database.beginTx() )
            {
                database.schema().indexFor( markerLabel ).on( "testProperty" ).create();

                Future<?> creation = executorService.submit( createNodeWithLabel( nodesLabel ) );
                creation.get();

                transaction.success();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                try ( ResourceIterator<Node> nodes = database.findNodes( nodesLabel ) )
                {
                    assertEquals( 1, count( nodes ) );
                }
            }
        } );
    }

    @Test
    void shutdownDatabaseDuringIndexPopulations()
    {
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider( true );
        File storeDir = directory.directory( "shutdownDbTest" );
        Label testLabel = label( "testLabel" );
        String propertyName = "testProperty";
        GraphDatabaseService shutDownDb = new TestGraphDatabaseFactory().setInternalLogProvider( assertableLogProvider )
                .newEmbeddedDatabase( storeDir );
        prePopulateDatabase( shutDownDb, testLabel, propertyName );

        try ( Transaction transaction = shutDownDb.beginTx() )
        {
            shutDownDb.schema().indexFor( testLabel ).on( propertyName ).create();
            transaction.success();
        }
        shutDownDb.shutdown();
        assertableLogProvider.assertNone( inLog( IndexPopulationJob.class ).anyError() );
    }

    private void prePopulateDatabase( GraphDatabaseService database, Label testLabel, String propertyName )
    {
        for ( int j = 0; j < 10_000; j++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( testLabel );
                node.setProperty( propertyName, randomAlphabetic( 10 ) );
                transaction.success();
            }
        }
    }

    private Runnable createNodeWithLabel( Label label )
    {
        return () -> {
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode( label );
                transaction.success();
            }
        };
    }

    private long countIndexes()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            return Iterables.count( database.schema().getIndexes() );
        }
    }

    private Runnable createIndexForLabelAndProperty( Label label, String propertyKey )
    {
        return () -> {
            try ( Transaction transaction = database.beginTx() )
            {
                database.schema().indexFor( label ).on( propertyKey ).create();
                transaction.success();
            }

            waitForOnlineIndexes();
        };
    }

    private void waitForOnlineIndexes()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, MINUTES );
            transaction.success();
        }
    }

    private Callable<Number> countNodes()
    {
        return () -> {
            try ( Transaction transaction = database.beginTx() )
            {
                Result result = database.execute( "MATCH (n) RETURN count(n) as count" );
                Map<String,Object> resultMap = result.next();
                return (Number) resultMap.get( "count" );
            }
        };
    }
}
