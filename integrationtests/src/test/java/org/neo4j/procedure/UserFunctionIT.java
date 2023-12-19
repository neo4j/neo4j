/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.procedure;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.StringMatcherIgnoresNewlines.containsStringIgnoreNewlines;

public class UserFunctionIT
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static List<Exception> exceptionsInFunction = Collections.synchronizedList( new ArrayList<>() );
    private GraphDatabaseService db;

    @Test
    public void shouldGiveNiceErrorMessageOnWrongStaticType()
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
    public void shouldGiveNiceErrorMessageWhenNoArguments()
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( containsStringIgnoreNewlines(String.format("Function call does not provide the " +
                "required number of arguments: expected 1 got 0.%n%n" +
                "Function org.neo4j.procedure.simpleArgument has signature: " +
                "org.neo4j.procedure.simpleArgument(someValue :: INTEGER?) :: INTEGER?%n" +
                "meaning that it expects 1 argument of type INTEGER? (line 1, column 8 (offset: 7))" )));
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.simpleArgument()" );
        }
    }

    @Test
    public void shouldShowDescriptionWhenMissingArguments()
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( containsStringIgnoreNewlines(String.format("Function call does not provide the " +
                "required number of arguments: expected 1 got 0.%n%n" +
                "Function org.neo4j.procedure.nodeWithDescription has signature: " +
                "org.neo4j.procedure.nodeWithDescription(someValue :: NODE?) :: NODE?%n" +
                "meaning that it expects 1 argument of type NODE?%n" +
                "Description: This is a description (line 1, column 8 (offset: 7))" )));
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "RETURN org.neo4j.procedure.nodeWithDescription()" );
        }
    }

    @Test
    public void shouldCallDelegatingFunction()
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
    public void shouldCallRecursiveFunction()
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
    public void shouldCallFunctionWithGenericArgument()
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
    public void shouldCallFunctionWithMapArgument()
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
    public void shouldCallFunctionWithMapArgumentContainingNullFromParameter()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.mapArgument({foo: $p}) AS someVal", map("p", null) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 1L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallFunctionWithNull()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.mapArgument(null) AS someVal" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 0L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallFunctionWithNullFromParameter()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "RETURN org.neo4j.procedure.mapArgument($p) AS someVal", map("p", null) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 0L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallFunctionWithNodeReturn()
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
    public void shouldGiveHelpfulErrorOnMissingFunction()
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage(
                String.format( "Unknown function 'org.someFunctionThatDoesNotExist' (line 1, column 8 (offset: 7))" +
                               "%n" +
                               "\"RETURN org.someFunctionThatDoesNotExist()" ) );

        // When
        db.execute( "RETURN org.someFunctionThatDoesNotExist()" );
    }

    @Test
    public void shouldGiveHelpfulErrorOnExceptionMidStream()
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            Result result = db.execute( "RETURN org.neo4j.procedure.throwsExceptionInStream()" );

            // Expect
            exception.expect( QueryExecutionException.class );
            exception.expectMessage(
                    "Failed to invoke function `org.neo4j.procedure.throwsExceptionInStream`: Caused by: java.lang" +
                    ".RuntimeException: Kaboom" );

            // When
            result.next();
        }
    }

    @Test
    public void shouldShowCauseOfError()
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
    public void shouldCallFunctionWithAccessToDB()
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

            assertEquals( res.next().get( "cool" ), singletonList( "Buddy Holly" ) );
        }
    }

    @Test
    public void shouldLogLikeThereIsNoTomorrow()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        db.shutdown();
        db = new TestGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .setUserLogProvider( logProvider )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
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
    public void shouldDenyReadOnlyFunctionToPerformWrites()
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
    public void shouldNotBeAbleToCallWriteProcedureThroughReadFunction()
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
    public void shouldDenyReadOnlyFunctionToPerformSchema()
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
    public void shouldCoerceLongToDoubleAtRuntimeWhenCallingFunction()
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
    public void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingFunction()
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
    public void shouldCoerceListOfMixedNumbers()
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
    public void shouldCoerceDoubleToLongAtRuntimeWhenCallingFunction()
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
    public void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyFunction()
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
            db.execute( "RETURN org.neo4j.procedure.simpleArgument(12)" ).close();
            db.createNode();
        }
    }

    @Test
    public void shouldPreserveSecurityContextWhenSpawningThreadsCreatingTransactionInFunctions() throws Throwable
    {
        // given
        Runnable doIt = () ->
        {
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

        // Then
        Predicates.await( () -> exceptionsInFunction.size() >= numThreads, 5, TimeUnit.SECONDS );

        for ( Exception exceptionInFunction : exceptionsInFunction )
        {
            assertThat( Exceptions.stringify( exceptionInFunction ),
                    exceptionInFunction, instanceOf( AuthorizationViolationException.class ) );
            assertThat( Exceptions.stringify( exceptionInFunction ), exceptionInFunction.getMessage(),
                    startsWith( "Write operations are not allowed" ) );
        }

        Result result = db.execute( "MATCH () RETURN count(*) as n" );
        assertThat( result.hasNext(), equalTo( true ) );
        while ( result.hasNext() )
        {
            assertThat( result.next().get( "n" ), equalTo( 0L ) );
        }
        result.close();
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
                                    "CREATE (n {prop: org.neo4j.procedure.simpleArgument(toInt(line[0]))}) " +
                                    "RETURN n.prop" );
        // THEN
        for ( long i = 1; i <= 100L; i++ )
        {
            assertThat( result.next().get( "n.prop" ), equalTo( i ) );
        }

        //Make sure all the lines has been properly commited to the database.
        String[] dbContents =
                db.execute( "MATCH (n) return n.prop" ).stream().map( m -> Long.toString( (long) m.get( "n.prop" ) ) )
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
    public void shouldCallFunctionReturningPaths()
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
    public void shouldHandleAggregationFunctionInFunctionCall()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            db.createNode( Label.label( "Person" ) );
            assertEquals(
                    db.execute( "MATCH (n:Person) RETURN org.neo4j.procedure.nodeListArgument(collect(n)) AS someVal" )
                            .next()
                            .get( "someVal" ),
                    2L );
        }
    }

    @Test
    public void shouldHandleNullInList()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            assertEquals(
                    db.execute( "MATCH (n:Person) RETURN org.neo4j.procedure.nodeListArgument([n, null]) AS someVal" )
                            .next()
                            .get( "someVal" ),
                    1L );
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
    public void shouldNotAllowReadFunctionInNoneTransaction()
    {
        // Expect
        exception.expect( AuthorizationViolationException.class );
        exception.expectMessage( "Read operations are not allowed" );

        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.none() ) )
        {
            db.execute( "RETURN org.neo4j.procedure.integrationTestMe()" ).next();
            tx.success();
        }
    }

    @Test
    public void shouldCallProcedureWithAllDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "RETURN org.neo4j.procedure.defaultValues() AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "a string,42,3.14,true" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldHandleNullAsParameter()
    {
        //Given/When
        Result res = db.execute( "RETURN org.neo4j.procedure.defaultValues($p) AS result", map( "p", null ) );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "null,42,3.14,true" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldCallFunctionWithOneProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "RETURN org.neo4j.procedure.defaultValues('another string') AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "another string,42,3.14,true" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldCallFunctionWithTwoProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "RETURN org.neo4j.procedure.defaultValues('another string', 1337) AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "another string,1337,3.14,true" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldCallFunctionWithThreeProvidedRestDefaultArgument()
    {
        //Given/When
        Result res =
                db.execute( "RETURN org.neo4j.procedure.defaultValues('another string', 1337, 2.718281828) AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "another string,1337,2.72,true" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldCallFunctionWithFourProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute(
                "RETURN org.neo4j.procedure.defaultValues('another string', 1337, 2.718281828, false) AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( "another string,1337,2.72,false" ) );
        assertFalse( res.hasNext() );
    }

    @Test
    public void shouldCallFunctionReturningNull()
    {
        //Given/When
        Result res = db.execute(
                "RETURN org.neo4j.procedure.node(-1) AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( null) );
        assertFalse( res.hasNext() );
    }

    /**
     * NOTE: this test tests user-defined functions added in this file {@link ClassWithFunctions}. These are not
     * built-in functions in any shape or form.
     */
    @Test
    public void shouldListAllUserDefinedFunctions()
    {
        //Given/When
        Result res = db.execute( "CALL dbms.functions()" );

        try ( BufferedReader reader = new BufferedReader(
                new InputStreamReader( UserFunctionIT.class.getResourceAsStream( "/misc/userDefinedFunctions" ) ) ) )
        {
            String expected = reader.lines().collect( Collectors.joining( System.lineSeparator() ) );
            String actual = res.resultAsString();
            // Be aware that the text file "userDefinedFunctions" must end with two newlines
            assertThat( actual, equalTo(expected) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to read userDefinedFunctions file." );
        }
    }

    @Test
    public void shouldCallFunctionWithSameNameAsBuiltIn()
    {
        //Given/When
        Result res = db.execute( "RETURN this.is.test.only.sum([1337, 2.718281828, 3.1415]) AS result" );

        // Then
        assertThat( res.next().get( "result" ), equalTo( 1337 + 2.718281828 + 3.1415 ) );
        assertFalse( res.hasNext() );
    }

    @Before
    public void setUp() throws IOException
    {
        exceptionsInFunction.clear();
        new JarBuilder().createJarFor( plugins.newFile( "myFunctions.jar" ), ClassWithFunctions.class );
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.record_id_batch_size, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
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

        @UserFunction
        public long integrationTestMe()
        {
            return 1337L;
        }

        @UserFunction
        public long simpleArgument( @Name( "someValue" ) long someValue )
        {
            return someValue;
        }

        @UserFunction
        public String defaultValues(
                @Name( value = "string", defaultValue = "a string" ) String string,
                @Name( value = "integer", defaultValue = "42" ) long integer,
                @Name( value = "float", defaultValue = "3.14" ) double aFloat,
                @Name( value = "boolean", defaultValue = "true" ) boolean aBoolean
        )
        {
            return String.format( "%s,%d,%.2f,%b", string, integer, aFloat, aBoolean );
        }

        @UserFunction
        public long nodeListArgument( @Name( "nodes" ) List<Node> nodes )
        {
            long count = 0L;
            for ( Node node : nodes )
            {
                if ( node != null )
                {
                    count++;
                }
            }
            return count;
        }

        @UserFunction
        public long delegatingFunction( @Name( "someValue" ) long someValue )
        {
            return (long) db
                    .execute( "RETURN org.neo4j.procedure.simpleArgument({name}) AS result", map( "name", someValue ) )
                    .next().get( "result" );
        }

        @UserFunction
        public long recursiveSum( @Name( "someValue" ) long order )
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

        @UserFunction
        public long genericArguments( @Name( "strings" ) List<List<String>> stringList,
                @Name( "longs" ) List<List<List<Long>>> longList )
        {
            return stringList.size() + longList.size();
        }

        @UserFunction
        public long mapArgument( @Name( "map" ) Map<String,Object> map )
        {
            if ( map == null )
            {
                return 0;
            }
            return map.size();
        }

        @UserFunction
        public Node node( @Name( "id" ) long id )
        {
            if ( id < 0 )
            {
                return null;
            }
            return db.getNodeById( id );
        }

        @UserFunction
        public double squareDouble( @Name( "someValue" ) double value )
        {
            return value * value;
        }

        @UserFunction
        public double avgNumberList( @Name( "someValue" ) List<Number> list )
        {
            return list.stream().reduce( ( l, r ) -> l.doubleValue() + r.doubleValue() ).orElse( 0.0d ).doubleValue() /
                   list.size();
        }

        @UserFunction
        public double avgDoubleList( @Name( "someValue" ) List<Double> list )
        {
            return list.stream().reduce( ( l, r ) -> l + r ).orElse( 0.0d ) / list.size();
        }

        @UserFunction
        public long squareLong( @Name( "someValue" ) long value )
        {
            return value * value;
        }

        @UserFunction
        public long throwsExceptionInStream()
        {
            throw new RuntimeException( "Kaboom" );
        }

        @UserFunction
        public long indexOutOfBounds()
        {
            int[] ints = {1, 2, 3};
            return ints[4];
        }

        @UserFunction
        public List<String> listCoolPeopleInDatabase()
        {
            return db.findNodes( label( "Person" ) )
                    .map( node -> (String) node.getProperty( "name" ) )
                    .stream()
                    .collect( Collectors.toList() );
        }

        @UserFunction
        public long logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return 1337L;
        }

        @UserFunction
        public Node readOnlyTryingToWrite()
        {
            return db.createNode();
        }

        @UserFunction
        public Node readOnlyCallingWriteFunction()
        {
            return (Node) db.execute( "RETURN org.neo4j.procedure.writingFunction() AS node" ).next().get( "node" );
        }

        @UserFunction
        public long readOnlyCallingWriteProcedure()
        {
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" );
            return 1337L;
        }

        @UserFunction( "this.is.test.only.sum" )
        public Number sum( @Name( "numbers" ) List<Number> numbers )
        {
            return numbers.stream().mapToDouble( Number::doubleValue ).sum();
        }

        @Procedure( mode = WRITE )
        public void writingProcedure()
        {
            db.createNode();
        }

        @UserFunction
        public String shutdown()
        {
            db.shutdown();
            return "oh no!";
        }

        @UserFunction
        public String unsupportedFunction()
        {
            jobs.submit( () ->
            {
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

        @UserFunction
        public Path nodePaths( @Name( "someValue" ) Node node )
        {
            return (Path) db
                    .execute( "WITH {node} AS node MATCH p=(node)-[*]->() RETURN p", map( "node", node ) )
                    .next()
                    .getOrDefault( "p", null );
        }

        @Description( "This is a description" )
        @UserFunction
        public Node nodeWithDescription( @Name( "someValue" ) Node node )
        {
            return node;
        }

        @UserFunction
        public String readOnlyTryingToWriteSchema()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return "done";
        }
    }

    private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );
}
