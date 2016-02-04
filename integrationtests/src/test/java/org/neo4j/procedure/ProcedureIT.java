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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
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
            assertThat(node.getId(), equalTo( nodeId ));
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
        try( Transaction ignore = db.beginTx() )
        {
            Result result = db.execute( "CALL org.neo4j.procedure.throwsExceptionInStream" );

            // Expect
            exception.expect( QueryExecutionException.class );
            exception.expectMessage(
                    "Failed to call procedure `org.neo4j.procedure.throwsExceptionInStream() :: (someVal :: INTEGER?)`: " +
                    "Kaboom" );

            // When
            result.next();
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
            .setUserLogProvider( logProvider  )
            .newImpermanentDatabaseBuilder()
            .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
            .newGraphDatabase();

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute( "CALL org.neo4j.procedure.logAround()" );
            while ( res.hasNext() ) res.next();
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
    public void readOnlyProcedureCannotWrite() throws Throwable
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
    public void procedureMarkedForWritingCanWrite() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            // TODO: #hasNext should not be needed here, result of writes should be eagerized
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" ).hasNext();
            tx.success();
        }

        // Then
        try( Transaction tx = db.beginTx() )
        {
            assertEquals(1, db.getAllNodes().stream().count());
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
    public void procedureMarkedForWritingCanCallOtherWritingProcedure() throws Throwable
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            // TODO: #hasNext should not be needed here, result of writes should be eagerized
            db.execute( "CALL org.neo4j.procedure.writeProcedureCallingWriteProcedure()" ).hasNext();
            tx.success();
        }

        // Then
        try( Transaction tx = db.beginTx() )
        {
            assertEquals(1, db.getAllNodes().stream().count());
            tx.success();
        }
    }

    @Before
    public void setUp() throws IOException
    {
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
        public Stream<Output> genericArguments( @Name( "stringList" ) List<List<String>> stringList,
                @Name( "longList" ) List<List<List<Long>>> longList )
        {
            return Stream.of( new Output( stringList.size() + longList.size() ) );
        }

        @Procedure
        public Stream<Output> mapArgument( @Name( "map" )Map<String,Object> map )
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
        public Stream<Output> throwsExceptionInStream()
        {
            return Stream.generate( () -> { throw new RuntimeException( "Kaboom" ); } );
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
        public Stream<Output> readOnlyCallingWriteProcedure()
        {
            return db.execute("CALL org.neo4j.procedure.writingProcedure")
                    .stream()
                    .map( (row) -> new Output( 0 ) );
        }

        @Procedure
        @PerformsWrites
        public Stream<Output> writeProcedureCallingWriteProcedure()
        {
            return db.execute("CALL org.neo4j.procedure.writingProcedure")
                    .stream()
                    .map( (row) -> new Output( 0 ) );
        }
    }
}
