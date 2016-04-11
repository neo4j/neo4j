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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class ProcedureIT
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GraphDatabaseService db;

    @Test
    public void shouldCallProcedureWithParameterMap() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.simpleArgument", map( "name", 42L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 42L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldGiveNiceErrorMessageOnWrongStaticType() throws Throwable
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Type mismatch: expected Integer but was String (line 1, column 41 (offset: 40))" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
            db.execute( "CALL org.neo4j.procedure.simpleArgument('42')" );
        }
    }

    @Test
    public void shouldGiveNiceErrorMessageWhenNoArguments() throws Throwable
    {
        //Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage(
                "Procedure call does not provide the required number of arguments (1) (line 1, column 1 (offset: 0))" );
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.simpleArgument()" );
        }
    }

    @Test
    public void shouldCallDelegatingProcedure() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.delegatingProcedure", map( "name", 43L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 43L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallRecursiveProcedure() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.recursiveSum", map( "order", 10L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 55L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallProcedureWithGenericArgument() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "CALL org.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " +
                    "[ [[1, 2, 3]], [[4, 5]]] )" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 5L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallProcedureWithMapArgument() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute(
                    "CALL org.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'})" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 2L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCallProcedureWithNodeReturn() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            long nodeId = db.createNode().getId();

            // When
            Result res = db.execute( "CALL org.neo4j.procedure.node({id})", map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId(), equalTo( nodeId ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldGiveHelpfulErrorOnMissingProcedure() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "There is no procedure with the name `someProcedureThatDoesNotExist` " +
                                 "registered for this database instance. Please ensure you've spelled the " +
                                 "procedure name correctly and that the procedure is properly deployed." );

        // When
        db.execute( "CALL someProcedureThatDoesNotExist" );
    }

    @Test
    public void shouldGiveHelpfulErrorOnExceptionMidStream() throws Throwable
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            Result result = db.execute( "CALL org.neo4j.procedure.throwsExceptionInStream" );

            // Expect
            exception.expect( QueryExecutionException.class );
            exception.expectMessage(
                    "Failed to call procedure `org.neo4j.procedure.throwsExceptionInStream() :: (someVal :: INTEGER?)" +
                    "`: " +
                    "Kaboom" );

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
                    "Failed to invoke procedure `org.neo4j.procedure.indexOutOfBounds`: Caused by: java.lang" +
                    ".ArrayIndexOutOfBoundsException" );
            // When
            db.execute( "CALL org.neo4j.procedure.indexOutOfBounds" );
        }
    }

    @Test
    public void shouldCallProcedureWithAccessToDB() throws Throwable
    {
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Buddy Holly" );
        }

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute(
                    "CALL org.neo4j.procedure.listCoolPeopleInDatabase" );

            assertFalse( res.hasNext() );
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
            Result res = db.execute( "CALL org.neo4j.procedure.logAround()" );
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
    public void shouldDenyReadOnlyProcedureToPerformWrites() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Write operations are not allowed" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.readOnlyTryingToWrite()" ).next();
        }
    }

    @Test
    public void shouldAllowWriteProcedureToPerformWrites() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" );
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
    public void shouldNotBeAbleToCallWriteProcedureThroughReadProcedure() throws Throwable
    {
        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Write operations are not allowed" );

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.readOnlyCallingWriteProcedure" ).next();
        }
    }

    @Test
    public void shouldBeAbleToCallWriteProcedureThroughWriteProcedure() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.writeProcedureCallingWriteProcedure()" );
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
    public void shouldCoerceLongToDoubleAtRuntimeWhenCallingProcedure() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.squareDouble", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 16.0d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingProcedure() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.avgNumberList({param})",
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
            Result res = db.execute( "CALL org.neo4j.procedure.avgDoubleList([{long}, {double}])",
                    map( "long", 1L, "double", 2.0d ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 1.5d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldCoerceDoubleToLongAtRuntimeWhenCallingProcedure() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.squareLong", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 16L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldBeAbleToCallVoidProcedure() throws Throwable
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.sideEffect('PONTUS')" );

            assertThat( db.execute( "MATCH (n:PONTUS) RETURN count(n) AS c" ).next().get( "c" ), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToCallDelegatingVoidProcedure() throws Throwable
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.delegatingSideEffect('SUTNOP')" );

            assertThat( db.execute( "MATCH (n:SUTNOP) RETURN count(n) AS c" ).next().get( "c" ), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyProcedure() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = db.createNode().getId();
            Node node = Iterators.single( db.execute( "CALL org.neo4j.procedure.node", map( "id", nodeId ) ).columnAs(
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
                "Failed to invoke procedure `org.neo4j.procedure.shutdown`: Caused by: java.lang" +
                ".UnsupportedOperationException" );

        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.shutdown()" );
        }
    }

    @Test
    public void shouldBeAbleToWriteAfterCallingReadOnlyProcedure()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.simpleArgument(12)" );
            db.createNode();
        }
    }

    private static List<Exception> exceptionsInProcedure = Collections.<Exception>synchronizedList( new ArrayList<>() );

    @Test
    public void shouldGracefullyFailWhenSpawningThreadsCreatingTransactionInProcedures() throws Throwable
    {
        // given
        Runnable doIt = () -> {
            Result result = db.execute( "CALL org.neo4j.procedure.unsupportedProcedure()" );
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
        Predicates.await( () -> exceptionsInProcedure.size() >= numThreads, 5, TimeUnit.SECONDS );

        for ( Exception exceptionInProcedure : exceptionsInProcedure )
        {
            assertThat( Exceptions.stringify( exceptionInProcedure ),
                    exceptionInProcedure, instanceOf( UnsupportedOperationException.class ) );
            assertThat( Exceptions.stringify( exceptionInProcedure ), exceptionInProcedure.getMessage(),
                    equalTo( "Creating new transactions and/or spawning threads " +
                             "are not supported operations in store procedures." ) );
        }
    }

    @Test
    public void shouldBeAbleToUseCallYieldWithPeriodicCommit() throws IOException
    {
        // GIVEN
        String[] lines = IntStream.rangeClosed( 1, 100 )
                .boxed()
                .map( i -> Integer.toString( i ) )
                .toArray( String[]::new );
        String url = createCsvFile( lines);

        //WHEN
        Result result = db.execute( "USING PERIODIC COMMIT 1 " +
                                    "LOAD CSV FROM '" + url + "' AS line " +
                                    "CALL org.neo4j.procedure.createNode(line[0]) YIELD node as n " +
                                    "RETURN n.prop" );
        // THEN
        for (int i = 1; i <= 100; i++)
        {
            assertThat( result.next().get( "n.prop" ), equalTo( Integer.toString( i ) ) );
        }

        //Make sure all the lines has been properly commited to the database.
        String[] dbContents = db.execute( "MATCH (n) return n.prop" ).stream().map( m -> (String) m.get( "n.prop" ) )
                .toArray( String[]::new );
        assertThat(dbContents, equalTo(lines));
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
                    "CALL org.neo4j.procedure.simpleArgument(toInt(line[0])) YIELD someVal as val " +
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
                                    "CALL org.neo4j.procedure.createNode(line[0]) YIELD node as n " +
                                    "SET n.p = 42 " +
                                    "RETURN n.p" );
        // THEN
        assertThat( result.next().get( "n.p" ), equalTo( 42L ) );
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

    @Before
    public void setUp() throws IOException
    {
        exceptionsInProcedure.clear();
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );
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

    public static class Output
    {
        public long someVal = 1337;

        public Output()
        {
        }

        public Output( long someVal )
        {
            this.someVal = someVal;
        }
    }

    public static class DoubleOutput
    {
        public double result = 0.0d;

        public DoubleOutput()
        {
        }

        public DoubleOutput( double result )
        {
            this.result = result;
        }
    }


    public static class NodeOutput
    {
        public Node node;

        public NodeOutput()
        {

        }

        void setNode( Node node )
        {
            this.node = node;
        }
    }

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        @Procedure
        public Stream<Output> integrationTestMe()
        {
            return Stream.of( new Output() );
        }

        @Procedure
        public Stream<Output> simpleArgument( @Name( "name" ) long someValue )
        {
            return Stream.of( new Output( someValue ) );
        }

        @Procedure
        public Stream<Output> delegatingProcedure( @Name( "name" ) long someValue )
        {
            return db.execute( "CALL org.neo4j.procedure.simpleArgument", map( "name", someValue ) )
                    .stream()
                    .map( ( row ) -> new Output( (Long) row.get( "someVal" ) ) );
        }

        @Procedure
        public Stream<Output> recursiveSum( @Name( "order" ) long order )
        {
            if ( order == 0L )
            {
                return Stream.of( new Output( 0L ) );
            }
            else
            {
                Long prev =
                        (Long) db.execute( "CALL org.neo4j.procedure.recursiveSum", map( "order", order - 1 ) )
                                .next().get( "someVal" );
                return Stream.of( new Output( order + prev ) );
            }
        }

        @Procedure
        public Stream<Output> genericArguments( @Name( "stringList" ) List<List<String>> stringList,
                @Name( "longList" ) List<List<List<Long>>> longList )
        {
            return Stream.of( new Output( stringList.size() + longList.size() ) );
        }

        @Procedure
        public Stream<Output> mapArgument( @Name( "map" ) Map<String,Object> map )
        {
            return Stream.of( new Output( map.size() ) );
        }

        @Procedure
        public Stream<NodeOutput> node( @Name( "id" ) long id )
        {
            NodeOutput nodeOutput = new NodeOutput();
            nodeOutput.setNode( db.getNodeById( id ) );
            return Stream.of( nodeOutput );
        }

        @Procedure
        public Stream<DoubleOutput> squareDouble( @Name( "value" ) double value )
        {
            DoubleOutput output = new DoubleOutput( value * value );
            return Stream.of( output );
        }

        @Procedure
        public Stream<DoubleOutput> avgNumberList( @Name( "list" ) List<Number> list )
        {
            double sum =
                    list.stream().reduce( ( l, r ) -> l.doubleValue() + r.doubleValue() ).orElse( 0.0d ).doubleValue();
            int count = list.size();
            DoubleOutput output = new DoubleOutput( sum / count );
            return Stream.of( output );
        }

        @Procedure
        public Stream<DoubleOutput> avgDoubleList( @Name( "list" ) List<Double> list )
        {
            double sum = list.stream().reduce( ( l, r ) -> l + r ).orElse( 0.0d );
            int count = list.size();
            DoubleOutput output = new DoubleOutput( sum / count );
            return Stream.of( output );
        }

        @Procedure
        public Stream<Output> squareLong( @Name( "value" ) long value )
        {
            Output output = new Output( value * value );
            return Stream.of( output );
        }

        @Procedure
        public Stream<Output> throwsExceptionInStream()
        {
            return Stream.generate( () -> { throw new RuntimeException( "Kaboom" ); } );
        }

        @Procedure
        public Stream<Output> indexOutOfBounds()
        {
            int[] ints = {1, 2, 3};
            int foo = ints[4];
            return Stream.of( new Output() );
        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeopleInDatabase()
        {
            return stream( spliteratorUnknownSize( db.findNodes( label( "Person" ) ), ORDERED | IMMUTABLE ), false )
                    .map( ( n ) -> new MyOutputRecord( (String) n.getProperty( "name" ) ) );
        }

        @Procedure
        public Stream<Output> logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return Stream.empty();
        }

        @Procedure
        public Stream<Output> readOnlyTryingToWrite()
        {
            db.createNode();
            return Stream.empty();
        }

        @Procedure
        @PerformsWrites
        public Stream<Output> writingProcedure()
        {
            db.createNode();
            return Stream.empty();
        }

        @Procedure
        @PerformsWrites
        public Stream<NodeOutput> createNode( @Name( "value" ) String value )
        {
            Node node = db.createNode();
            node.setProperty( "prop", value );
            NodeOutput out = new NodeOutput();
            out.setNode( node );
            return Stream.of( out );
        }

        @Procedure
        public Stream<Output> readOnlyCallingWriteProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.writingProcedure" )
                    .stream()
                    .map( ( row ) -> new Output( 0 ) );
        }

        @Procedure
        @PerformsWrites
        public Stream<Output> writeProcedureCallingWriteProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.writingProcedure" )
                    .stream()
                    .map( ( row ) -> new Output( 0 ) );
        }

        @Procedure
        @PerformsWrites
        public void sideEffect( @Name( "value" ) String value )
        {
            db.createNode( Label.label( value ) );
        }

        @Procedure
        public void shutdown()
        {
            db.shutdown();
        }

        @Procedure
        @PerformsWrites
        public void delegatingSideEffect( @Name( "value" ) String value )
        {
            db.execute( "CALL org.neo4j.procedure.sideEffect", map( "value", value ) );
        }

        private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );

        @Procedure
        @PerformsWrites
        public void unsupportedProcedure()
        {
            jobs.submit( () -> {
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }
                catch ( Exception e )
                {
                    exceptionsInProcedure.add( e );
                }
            } );
        }
    }
}
