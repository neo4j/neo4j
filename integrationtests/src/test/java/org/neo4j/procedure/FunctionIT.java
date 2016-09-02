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
package org.neo4j.procedure;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

public class FunctionIT
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GraphDatabaseService db;

    @Test
    public void shouldGiveNiceErrorMessageOnWrongStaticType() throws Throwable
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Type mismatch: expected Integer but was String (line 1, column 43 (offset: 42))" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
            db.execute( "RETURN org.neo4j.procedure.simpleArgument('42')" );
        }
    }

    @Test
    public void shouldGiveNiceErrorMessageWhenNoArguments() throws Throwable
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage(
                "Function call does not provide the required number of arguments (1) (line 1, column 8 (offset: 7)" );
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.simpleArgument()" );
        }
    }

    @Test
    public void shouldCallDelegatingFunction() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.delegatingFunction({name}) AS someVal",
                    map( "name", 43L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 43L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallRecursiveFunction() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res =
                    db.execute( "RETURN org.neo4j.procedure.recursiveSum({order}) AS someVal", map( "order", 10L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 55L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallFunctionWithGenericArgument() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " +
                    "[ [[1, 2, 3]], [[4, 5]]] ) AS someVal" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 5L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallFunctionWithMapArgument() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'}) AS someVal" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 2L ) ) );
            assertFalse( res.hasNext() );
        }
    }


    @Test
    public void shouldCallFunctionWithNodeReturn() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            long nodeId = db.createNode().getId();

            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.node({id}) AS node", map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId(), equalTo( nodeId ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldGiveHelpfulErrorOnMissingFunction() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Unknown function 'org.someFunctionThatDoesNotExist' (line 1, column 8 (offset: 7))" +
                                 "\n" +
                                 "\"RETURN org.someFunctionThatDoesNotExist()");

        // When
        db.execute( "RETURN org.someFunctionThatDoesNotExist()" );
    }

    @Test
    public void shouldGiveHelpfulErrorOnExceptionMidStream() throws Throwable
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            Result result = db.execute( "RETURN org.neo4j.procedure.throwsExceptionInStream()" );

            // Expect
            exception.expect( QueryExecutionException.class );
            exception.expectMessage(
                    "Failed to invoke function `org.neo4j.procedure.throwsExceptionInStream`: Caused by: java.lang.RuntimeException: Kaboom" );

            // When
            result.next();
        }
    }

    @Test
    public void shouldShowCauseOfError() throws Throwable
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            // Expect
            exception.expect( QueryExecutionException.class );
            exception.expectMessage(
                    "Failed to invoke function `org.neo4j.procedure.indexOutOfBounds`: Caused by: java.lang" +
                    ".ArrayIndexOutOfBoundsException" );
            // When
            db.execute( "RETURN org.neo4j.procedure.indexOutOfBounds()" ).next();
        }
    }

    @Test
    public void shouldCallFunctionWithAccessToDB() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Buddy Holly" );
            tx.success();
        }

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.listCoolPeopleInDatabase() AS cool" );

            assertEquals( res.next().get("cool"), singletonList( "Buddy Holly" ) );
        }
    }

    @Test
    public void shouldLogLikeThereIsNoTomorrow() throws Throwable
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        db.shutdown();
        db = new TestGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .setUserLogProvider( logProvider )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute( "RETURN org.neo4j.procedure.logAround()" );
            while ( res.hasNext() )
            { res.next(); }
        }

        // Then
        AssertableLogProvider.LogMatcherBuilder match = inLog( Procedures.class );
        logProvider.assertAtLeastOnce(
                match.debug( "1" ),
                match.info( "2" ),
                match.warn( "3" ),
                match.error( "4" )
        );
    }

    @Test
    public void shouldDenyReadOnlyFunctionToPerformWrites() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Write operations are not allowed" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.readOnlyTryingToWrite()" ).next();
        }
    }

    @Test
    public void shouldAllowWriteFunctionToPerformWrites() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.writingFunction()" ).next();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, db.getAllNodes().stream().count() );
            tx.success();
        }
    }

    @Test
    public void shouldNotBeAbleToCallWriteFunctionThroughReadFunction() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Write operations are not allowed" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.readOnlyCallingWriteFunction()" ).next();
        }
    }

    @Test
    public void shouldNotBeAbleToCallWriteProcedureThroughReadFunction() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Write operations are not allowed" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.readOnlyCallingWriteProcedure()" ).next();
        }
    }

    @Test
    public void shouldNotBeAbleToCallReadFunctionThroughWriteFunctionInWriteOnlyTransaction() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Read operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.WRITE_ONLY ) )
        {
            db.execute( "RETURN org.neo4j.procedure.writeFunctionCallingReadFunction()" ).next();
        }
    }

    @Test
    public void shouldBeAbleToCallWriteFunctionThroughWriteFunction() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.writeFunctionCallingWriteFunction()" ).next();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, db.getAllNodes().stream().count() );
            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToCallWriteProcedureThroughWriteFunction() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.writeFunctionCallingWriteProcedure()" ).next();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, db.getAllNodes().stream().count() );
            tx.success();
        }
    }

    @Test
    public void shouldNotBeAbleToCallSchemaFunctionThroughWriteFunctionInWriteTransaction() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Schema operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.WRITE ) )
        {
            db.execute( "RETURN org.neo4j.procedure.writeFunctionCallingSchemaFunction()" ).next();
        }
    }

    @Test
    public void shouldDenyReadOnlyFunctionToPerformSchema() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Schema operations are not allowed" );

        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            db.execute( "RETURN org.neo4j.procedure.readOnlyTryingToWriteSchema()" ).next();
        }
    }

    @Test
    public void shouldDenyReadWriteFunctionToPerformSchema() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Cannot perform schema updates in a transaction that has performed data updates" );

        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            db.execute( "RETURN org.neo4j.procedure.readWriteTryingToWriteSchema()" ).next();
        }
    }

    @Test
    public void shouldAllowSchemaFunctionToPerformSchema() throws Throwable
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            db.execute( "RETURN org.neo4j.procedure.schemaFunction()" ).next();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( db.schema().getConstraints().iterator().hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldAllowSchemaCallReadOnly() throws Throwable
    {
        // Given
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode().getId();
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.schemaCallReadFunction({id}) AS node",
                    map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId(), equalTo( nodeId ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldDenySchemaFunctionToPerformWrite() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Cannot perform data updates in a transaction that has performed schema updates" );

        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            db.execute( "RETURN org.neo4j.procedure.schemaTryingToWrite()" ).next();
        }
    }

    @Test
    public void shouldCoerceLongToDoubleAtRuntimeWhenCallingFunction() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.squareDouble({value}) AS result", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 16.0d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingFunction() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.avgNumberList({param}) AS result",
                    map( "param", Arrays.<Number>asList( 1L, 2L, 3L ) ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 2.0d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCoerceListOfMixedNumbers() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.avgDoubleList([{long}, {double}]) AS result",
                    map( "long", 1L, "double", 2.0d ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 1.5d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCoerceDoubleToLongAtRuntimeWhenCallingFunction() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.squareLong({value}) as someVal", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 16L ) ) );
            assertFalse( res.hasNext() );
        }
    }


    @Test
    public void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyFunction() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = db.createNode().getId();
            Node node = Iterators.single(
                    db.execute( "RETURN org.neo4j.procedure.node({id}) AS node", map( "id", nodeId ) ).columnAs(
                            "node" ) );
            node.setProperty( "name", "Stefan" );
            tx.success();
        }
    }

    @Test
    public void shouldFailToShutdown()
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage(
                "Failed to invoke function `org.neo4j.procedure.shutdown`: Caused by: java.lang" +
                ".UnsupportedOperationException" );

        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.shutdown()" ).next();
        }
    }

    @Test
    public void shouldBeAbleToWriteAfterCallingReadOnlyFunction()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.simpleArgument(12)" );
            db.createNode();
        }
    }

    private static List<Exception> exceptionsInFunction = Collections.<Exception>synchronizedList( new ArrayList<>() );

    @Test
    public void shouldGracefullyFailWhenSpawningThreadsCreatingTransactionInFunctions() throws Throwable
    {
        // given
        Runnable doIt = () -> {
            Result result = db.execute( "RETURN org.neo4j.procedure.unsupportedFunction()" );
            result.resultAsString();
            result.close();
        };

        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];
        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i] = new Thread( doIt );
        }

        // when
        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i].start();
        }

        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i].join();
        }

        // then
        Predicates.await( () -> exceptionsInFunction.size() >= numThreads, 5, TimeUnit.SECONDS );

        for ( Exception exceptionInFunction : exceptionsInFunction )
        {
            assertThat( Exceptions.stringify( exceptionInFunction ),
                    exceptionInFunction, instanceOf( UnsupportedOperationException.class ) );
            assertThat( Exceptions.stringify( exceptionInFunction ), exceptionInFunction.getMessage(),
                    equalTo( "Creating new transactions and/or spawning threads " +
                             "are not supported operations in store procedures." ) );
        }
    }

    @Test
    public void shouldBeAbleToUseFunctionCallWithPeriodicCommit() throws IOException
    {
        // GIVEN
        String[] lines = IntStream.rangeClosed( 1, 100 )
                .boxed()
                .map( i -> Integer.toString( i ) )
                .toArray( String[]::new );
        String url = createCsvFile( lines );

        //WHEN
        Result result = db.execute( "USING PERIODIC COMMIT 1 " +
                                    "LOAD CSV FROM '" + url + "' AS line " +
                                    "WITH org.neo4j.procedure.createNode(line[0]) AS n " +
                                    "RETURN n.prop" );
        // THEN
        for ( int i = 1; i <= 100; i++ )
        {
            assertThat( result.next().get( "n.prop" ), equalTo( Integer.toString( i ) ) );
        }

        //Make sure all the lines has been properly commited to the database.
        String[] dbContents = db.execute( "MATCH (n) return n.prop" ).stream().map( m -> (String) m.get( "n.prop" ) )
                .toArray( String[]::new );
        assertThat( dbContents, equalTo( lines ) );
    }

    @Test
    public void shouldFailIfUsingPeriodicCommitWithReadOnlyQuery() throws IOException
    {
        // GIVEN
        String url = createCsvFile( "13" );

        // EXPECT
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Cannot use periodic commit in a non-updating query (line 1, column 1 (offset: 0))" );

        //WHEN
        db.execute( "USING PERIODIC COMMIT 1 " +
                    "LOAD CSV FROM '" + url + "' AS line " +
                    "WITH org.neo4j.procedure.simpleArgument(toInt(line[0])) AS val " +
                    "RETURN val" );
    }

    @Test
    public void shouldBeAbleToUseCallYieldWithLoadCsvAndSet() throws IOException
    {
        // GIVEN
        String url = createCsvFile( "foo" );

        //WHEN
        Result result = db.execute(
                "LOAD CSV FROM '" + url + "' AS line " +
                "WITH org.neo4j.procedure.createNode(line[0]) AS n " +
                "SET n.p = 42 " +
                "RETURN n.p" );
        // THEN
        assertThat( result.next().get( "n.p" ), equalTo( 42L ) );
    }

    @Test
    public void shouldCallFunctionReturningPaths() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "KNOWS" ) );

            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.nodePaths({node}) AS path", map( "node", node1 ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> value = res.next();
            Path path = (Path) value.get( "path" );
            assertThat( path.length(), equalTo( 1 ) );
            assertThat( path.startNode(), equalTo( node1 ) );
            assertThat( asList( path.relationships() ), equalTo( singletonList( rel ) ) );
            assertThat( path.endNode(), equalTo( node2 ) );
            assertFalse( res.hasNext() );
        }
    }

    private String createCsvFile( String... lines ) throws IOException
    {
        File file = plugins.newFile();

        try ( PrintWriter writer = FileUtils.newFilePrintWriter( file, StandardCharsets.UTF_8 ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }

        return file.toURI().toURL().toString();
    }

    @Test
    public void shouldReturnNodeListTypedAsNodeList()
    {
        // When
        Result res = db.execute(
                "WITH org.neo4j.procedure.nodeList() AS nodes RETURN extract( x IN nodes | id(x) ) as ids" );

        // Then
        assertTrue( res.hasNext() );
        assertThat( ((List<?>) res.next().get( "ids" )).size(), equalTo( 2 ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldHandleAggregationFunctionInFunctionCall()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            db.createNode( Label.label( "Person" ) );
            assertEquals(
                    db.execute( "MATCH (n:Person) RETURN org.neo4j.procedure.nodeListArgument(collect(n)) AS someVal" )
                            .next()
                            .get("someVal"),
                    2L);
        }
    }

    @Test
    public void shouldWorkWhenUsingWithToProjectList()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            db.createNode( Label.label( "Person" ) );

            // When
            Result res = db.execute(
                    "MATCH (n:Person) WITH collect(n) as persons RETURN org.neo4j.procedure.nodeListArgument(persons)" +
                    " AS someVal" );

            // THEN
            assertThat( res.next().get( "someVal" ), equalTo( 2L ) );
        }
    }

    @Test
    public void shouldNotAllowReadFunctionInNoneTransaction() throws Throwable
    {
        // Expect
        exception.expect( AuthorizationViolationException.class );
        exception.expectMessage( "Read operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.NONE ) )
        {
            db.execute( "RETURN org.neo4j.procedure.integrationTestMe()" ).next();
            tx.success();
        }
    }

    @Test
    public void shouldNotAllowWriteFunctionInReadOnlyTransaction() throws Throwable
    {
        // Expect
        exception.expect( AuthorizationViolationException.class );
        exception.expectMessage( "Write operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.READ ) )
        {
            db.execute( "RETURN org.neo4j.procedure.writingFunction()" ).next();
            tx.success();
        }
    }

    @Test
    public void shouldNotAllowSchemaWriteFunctionInWriteTransaction() throws Throwable
    {
        // Expect
        exception.expect( AuthorizationViolationException.class );
        exception.expectMessage( "Schema operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.WRITE ) )
        {
            db.execute( "RETURN org.neo4j.procedure.schemaFunction()" ).next();
            tx.success();
        }
    }

    @Before
    public void setUp() throws IOException
    {
        exceptionsInFunction.clear();
        new JarBuilder().createJarFor( plugins.newFile( "myFunctions.jar" ), ClassWithFunctions.class );
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

    }

    @After
    public void tearDown()
    {
        if ( this.db != null )
        {
            this.db.shutdown();
        }
    }

    public static class ClassWithFunctions
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        @Function
        public long integrationTestMe()
        {
            return 1337L;
        }

        @Function
        public long simpleArgument( long someValue )
        {
            return someValue;
        }

        @Function
        public long nodeListArgument( List<Node> nodes )
        {
            return nodes.size();
        }

        @Function
        public long delegatingFunction( long someValue )
        {
            return (long) db
                    .execute( "RETURN org.neo4j.procedure.simpleArgument({name}) AS result", map( "name", someValue ) )
                    .next().get( "result" );
        }

        @Function
        public long recursiveSum( long order )
        {
            if ( order == 0L )
            {
                return 0L;
            }
            else
            {
                Long prev =
                        (Long) db.execute( "RETURN org.neo4j.procedure.recursiveSum({order}) AS someVal",
                                map( "order", order - 1 ) )
                                .next().get( "someVal" );
                return order + prev;
            }
        }

        @Function
        public long genericArguments( List<List<String>> stringList, List<List<List<Long>>> longList )
        {
            return stringList.size() + longList.size();
        }

        @Function
        public long mapArgument( Map<String,Object> map )
        {
            return map.size();
        }

        @Function
        public Node node( @Name( "id" ) long id )
        {
            return db.getNodeById( id );
        }

        @Function
        public double squareDouble( double value )
        {
            return value * value;
        }

        @Function( mode = WRITE )
        public List<Node> nodeList()
        {
            List<Node> nodesList = new ArrayList<>();
            nodesList.add( db.createNode() );
            nodesList.add( db.createNode() );

            return nodesList;
        }

        @Function
        public double avgNumberList( List<Number> list )
        {
            return list.stream().reduce( ( l, r ) -> l.doubleValue() + r.doubleValue() ).orElse( 0.0d ).doubleValue() /
                   list.size();
        }

        @Function
        public double avgDoubleList(  List<Double> list )
        {
            return list.stream().reduce( ( l, r ) -> l + r ).orElse( 0.0d ) / list.size();
        }

        @Function
        public long squareLong( long value )
        {
            return value * value;
        }

        @Function
        public long throwsExceptionInStream()
        {
            throw new RuntimeException( "Kaboom" );
        }

        @Function
        public long indexOutOfBounds()
        {
            int[] ints = {1, 2, 3};
            return ints[4];
        }

        @Function
        public List<String> listCoolPeopleInDatabase()
        {
            return db.findNodes( label( "Person" ) )
                    .map( node -> (String) node.getProperty( "name" ) )
                    .stream()
                    .collect( Collectors.toList() );
        }

        @Function
        public long logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return 1337L;
        }

        @Function
        public Node  readOnlyTryingToWrite()
        {
            return db.createNode();
        }

        @Function( mode = WRITE )
        public Node writingFunction()
        {
            return db.createNode();
        }

        @Function( mode = WRITE )
        public Node createNode(String value )
        {
            Node node = db.createNode();
            node.setProperty( "prop", value );
          return  node;
        }

        @Function
        public Node readOnlyCallingWriteFunction()
        {
            return (Node) db.execute( "RETURN org.neo4j.procedure.writingFunction() AS node" ).next().get("node");
        }

        @Function
        public long readOnlyCallingWriteProcedure()
        {
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" );
            return 1337L;
        }

        @Procedure( mode = WRITE )
        public void writingProcedure()
        {
            db.createNode();
        }

        @Function( mode = WRITE )
        public Node writeFunctionCallingWriteFunction()
        {
            return (Node) db.execute( "RETURN org.neo4j.procedure.writingFunction() AS node" ).next().get("node");
        }

        @Function( mode = WRITE )
        public long writeFunctionCallingWriteProcedure()
        {
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" );
            return 1337L;
        }

        @Function( mode = WRITE )
        public long writeFunctionCallingReadFunction()
        {
            return (long) db.execute( "RETURN org.neo4j.procedure.integrationTestMe() AS result" ).next()
                    .get( "result" );
        }

        @Function( mode = WRITE )
        public String writeFunctionCallingSchemaFunction()
        {
            return (String) db.execute( "RETURN  org.neo4j.procedure.schemaFunction() AS result" ).next().get("result");
        }


        @Function
        public String shutdown()
        {
            db.shutdown();
            return "oh no!";
        }


        @Function( mode = WRITE )
        public String unsupportedFunction()
        {
            jobs.submit( () -> {
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }
                catch ( Exception e )
                {
                    exceptionsInFunction.add( e );
                }
            } );

            return "why!?";
        }

        @Function
        public Path nodePaths( Node node )
        {
            return (Path) db
                    .execute( "WITH {node} AS node MATCH p=(node)-[*]->() RETURN p", map( "node", node ) )
                    .next()
                    .getOrDefault( "p", null );
        }


        @Description( "This is a description" )
        @Function( mode = WRITE )
        public Node nodeWithDescription( Node node )
        {
            return node;
        }


        @Function
        public String readOnlyTryingToWriteSchema()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return "done";
        }

        @Function( mode = WRITE )
        public String readWriteTryingToWriteSchema()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return "done";
        }

        @Function( mode = SCHEMA )
        public String schemaFunction()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return "done";
        }

        @Function( mode = SCHEMA )
        public Node schemaCallReadFunction( long id )
        {
            return (Node) db.execute( "RETURN org.neo4j.procedure.node(" + id + ") AS node" ).next().get( "node" );
        }

        @Function( mode = SCHEMA )
        public Node schemaTryingToWrite()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return db.createNode();
        }
    }

    private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );
}
