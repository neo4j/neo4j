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
package schema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.RandomValues;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class IndexPopulationIT
{
    @ClassRule
    public static final TestDirectory directory = TestDirectory.testDirectory();

    private static final int TEST_TIMEOUT = 120_000;
    private static GraphDatabaseService database;
    private static ExecutorService executorService;
    private static AssertableLogProvider logProvider;

    @BeforeClass
    public static void setUp()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        logProvider = new AssertableLogProvider( true );
        factory.setInternalLogProvider( logProvider );
        database = factory.newEmbeddedDatabase( directory.storeDir() );
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void tearDown()
    {
        executorService.shutdown();
        database.shutdown();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void indexCreationDoNotBlockQueryExecutions() throws Exception
    {
        Label nodeLabel = Label.label( "nodeLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode(nodeLabel);
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( Label.label( "testLabel" ) ).on( "testProperty" ).create();

            Future<Number> countFuture = executorService.submit( countNodes() );
            assertEquals( 1, countFuture.get().intValue() );

            transaction.success();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void createIndexesFromDifferentTransactionsWithoutBlocking() throws ExecutionException, InterruptedException
    {
        long numberOfIndexesBeforeTest = countIndexes();
        Label nodeLabel = Label.label( "nodeLabel2" );
        String testProperty = "testProperty";
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( Label.label( "testLabel2" ) ).on( testProperty ).create();

            Future<?> creationFuture = executorService.submit( createIndexForLabelAndProperty( nodeLabel, testProperty ) );
            creationFuture.get();
            transaction.success();
        }
        waitForOnlineIndexes();

        assertEquals( numberOfIndexesBeforeTest + 2, countIndexes() );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void indexCreationDoNotBlockWritesOnOtherLabel() throws ExecutionException, InterruptedException
    {
        Label markerLabel = Label.label( "testLabel3" );
        Label nodesLabel = Label.label( "testLabel4" );
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
                assertEquals( 1, Iterators.count( nodes ) );
            }
        }
    }

    @Test
    public void shutdownDatabaseDuringIndexPopulations()
    {
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider( true );
        File storeDir = directory.directory( "shutdownDbTest" );
        Label testLabel = Label.label( "testLabel" );
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
        assertableLogProvider.assertNone( AssertableLogProvider.inLog( IndexPopulationJob.class ).anyError() );
    }

    @Test
    public void mustLogPhaseTracker()
    {
        Label nodeLabel = Label.label( "testLabel5" );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode( nodeLabel ).setProperty( "key", "hej" );
            transaction.success();
        }

        // when
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().indexFor( nodeLabel ).on( "key" ).create();
            tx.success();
        }
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        // then
        //noinspection unchecked
        logProvider.assertContainsMessageMatching( allOf(
                containsString( "TIME/PHASE" ),
                containsString( "Final: " ),
                containsString( "SCAN" ),
                containsString( "WRITE" ),
                containsString( "FLIP" ),
                containsString( "totalTime=" ),
                containsString( "avgTime=" ),
                containsString( "minTime=" ),
                containsString( "maxTime=" ),
                containsString( "nbrOfReports=" )
        ) );
    }

    private void prePopulateDatabase( GraphDatabaseService database, Label testLabel, String propertyName )
    {
        final RandomValues randomValues = RandomValues.create();
        for ( int j = 0; j < 10_000; j++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( testLabel );
                Object property = randomValues.nextValue().asObject();
                node.setProperty( propertyName, property );
                transaction.success();
            }
        }
    }

    private Runnable createNodeWithLabel( Label label )
    {
        return () ->
        {
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
        return () ->
        {
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
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.success();
        }
    }

    private Callable<Number> countNodes()
    {
        return () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Result result = database.execute( "MATCH (n) RETURN count(n) as count" );
                Map<String,Object> resultMap = result.next();
                return (Number) resultMap.get( "count" );
            }
        };
    }
}
