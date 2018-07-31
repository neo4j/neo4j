/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.procedure.Mode.WRITE;

public class EagerProcedureIT
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GraphDatabaseService db;

    @Test
    public void shouldNotGetPropertyAccessFailureWhenStreamingToAnEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        // Then we can run an eagerized destructive procedure
        Result res = db.execute( "MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "Should get as many rows as original nodes", res.resultAsString(), containsString( "2 rows" ) );
    }

    @Test
    public void shouldGetPropertyAccessFailureWhenStreamingToANonEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        // Expect a specific error
        exception.expect( QueryExecutionException.class );
        exception.expectMessage( "Node with id 1 has been deleted in this transaction" );

        // When we try to run an eagerized destructive procedure
        Result res = db.execute( "MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        res.resultAsString();   // pull all results. The second row will cause the exception
    }

    @Test
    public void shouldGetEagerPlanForAnEagerProcedure()
    {
        // When explaining a call to an eagerized procedure
        Result res = db.execute( "EXPLAIN MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "The plan description should contain the 'Eager' operation", res.getExecutionPlanDescription().toString(), containsString( "+Eager" ) );
    }

    @Test
    public void shouldNotGetEagerPlanForANonEagerProcedure()
    {
        // When explaining a call to an non-eagerized procedure
        Result res = db.execute( "EXPLAIN MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "The plan description shouldn't contain the 'Eager' operation", res.getExecutionPlanDescription().toString(),
                Matchers.not( containsString( "+Eager" ) ) );
    }

    private void setUpTestData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createChainOfNodesWithLabelAndProperty( 2, "FOLLOWS", "User", "key", "value" );
            tx.success();
        }
    }

    private void createChainOfNodesWithLabelAndProperty( int length, String relationshipName, String labelName, String property, String value )
    {
        RelationshipType relationshipType = RelationshipType.withName( relationshipName );
        Label label = Label.label( labelName );
        Node prev = null;
        for ( int i = 0; i < length; i++ )
        {
            Node node = db.createNode( label );
            node.setProperty( property, value );
            if ( !property.equals( "name" ) )
            {
                node.setProperty( "name", labelName + " " + i );
            }
            if ( prev != null )
            {
                prev.createRelationshipTo( node, relationshipType );
            }
            prev = node;
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
        public final long value;

        public Output( long value )
        {
            this.value = value;
        }
    }

    @SuppressWarnings( "unused" )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Procedure( mode = WRITE, eager = true )
        public Stream<Output> deleteNeighboursEagerized( @Name( "node" ) Node node, @Name( "relation" ) String relation )
        {
            return Stream.of( new Output( deleteNeighbours( node, RelationshipType.withName( relation ) ) ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> deleteNeighboursNotEagerized( @Name( "node" ) Node node, @Name( "relation" ) String relation )
        {
            return Stream.of( new Output( deleteNeighbours( node, RelationshipType.withName( relation ) ) ) );
        }

        private long deleteNeighbours( Node node, RelationshipType relType )
        {
            try
            {
                long deleted = 0;
                for ( Relationship rel : node.getRelationships() )
                {
                    Node other = rel.getOtherNode( node );
                    rel.delete();
                    other.delete();
                    deleted++;
                }
                return deleted;
            }
            catch ( NotFoundException e )
            {
                // Procedures should internally handle missing nodes due to lazy interactions
                return 0;
            }
        }
    }
}
