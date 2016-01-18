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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.proc.Resource;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.kernel.impl.proc.Name;
import org.neo4j.kernel.impl.proc.ReadOnlyProcedure;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ProcedureIT
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldLoadProcedureFromPluginDirectory() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res1 = db.execute( "CALL org.neo4j.procedure.integrationTestMe" );
            assertThat( res1.next(), equalTo( map( "someVal", 1337L ) ));
            assertFalse( res1.hasNext() );

            Result res2 = db.execute( "CALL org.neo4j.procedure.simpleArgument(42)" );
            assertThat( res2.next(), equalTo( map( "someVal", 42L ) ));
            assertFalse( res2.hasNext() );
        }
    }

    @Test
    public void shouldLoadBeAbleToCallMethodWithParameterMap() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute( "CALL org.neo4j.procedure.simpleArgument", map( "name", 42L ) );
            assertThat( res.next(), equalTo( map( "someVal", 42L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldLoadBeAbleToCallProcedureWithGenericArgument() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute(
                    "CALL org.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " +
                    "[ [[1, 2, 3]], [[4, 5]]] )" );
            assertThat( res.next(), equalTo( map( "someVal", 5L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldLoadBeAbleToCallProcedureWithMapArgument() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute(
                    "CALL org.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'})" );
            assertThat( res.next(), equalTo( map( "someVal", 2L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldLoadBeAbleToCallProcedureWithNodeReturn() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            long nodeId = db.createNode().getId();
            Result res = db.execute( "CALL org.neo4j.procedure.node({id})", map( "id", nodeId ) );
            Node node = (Node) res.next().get( "node" );
            assertThat(node.getId(), equalTo( nodeId ));
            assertFalse( res.hasNext() );
        }
    }

    @Test
    public void shouldGiveHelpfulErrorOnMissingProcedure() throws Throwable
    {
        // Given
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

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
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        Result result = db.execute( "CALL org.neo4j.procedure.throwsExceptionInStream" );

        // Expect
        exception.expect( QueryExecutionException.class );
        exception.expectMessage(
                "Failed to call procedure `org.neo4j.procedure.throwsExceptionInStream() :: (someVal :: INTEGER?)`: " +
                "Kaboom" );

        // When
        result.next();
    }

    @Test
    public void shouldLoadBeAbleToCallProcedureWithAccessToDB() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ClassWithProcedures.class );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();
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

        public MyOutputRecord()
        {

        }

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class ClassWithProcedures
    {
        @Resource
        public GraphDatabaseService db;

        @ReadOnlyProcedure
        public Stream<Output> integrationTestMe()
        {
            return Stream.of( new Output() );
        }

        @ReadOnlyProcedure
        public Stream<Output> simpleArgument( @Name( "name" ) long someValue )
        {
            return Stream.of( new Output( someValue ) );
        }

        @ReadOnlyProcedure
        public Stream<Output> genericArguments( @Name( "stringList" ) List<List<String>> stringList,
                @Name( "longList" ) List<List<List<Long>>> longList )
        {
            return Stream.of( new Output( stringList.size() + longList.size() ) );
        }

        @ReadOnlyProcedure
        public Stream<Output> mapArgument( @Name( "map" )Map<String,Object> map )
        {
            return Stream.of( new Output( map.size() ) );
        }

        @ReadOnlyProcedure
        public Stream<NodeOutput> node( @Name( "id" ) long id )
        {
            NodeOutput nodeOutput = new NodeOutput();
            nodeOutput.setNode( db.getNodeById( id ) );
            return Stream.of( nodeOutput );
        }

        @ReadOnlyProcedure
        public Stream<Output> throwsExceptionInStream()
        {
            return Stream.generate( () -> { throw new RuntimeException( "Kaboom" ); } );
        }

        @ReadOnlyProcedure
        public Stream<MyOutputRecord> listCoolPeopleInDatabase()
        {
            return stream( spliteratorUnknownSize( db.findNodes( label( "Person" ) ), ORDERED | IMMUTABLE ), false )
                    .map( ( n ) -> new MyOutputRecord( (String) n.getProperty( "name" ) ) );
        }
    }
}
