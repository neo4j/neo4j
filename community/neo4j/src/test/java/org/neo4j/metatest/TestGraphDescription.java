/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.metatest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.*;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;

public class TestGraphDescription implements GraphHolder
{
    private static final TargetDirectory target = TargetDirectory.forTest( TestGraphDescription.class );
    private static GraphDatabaseService graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    @Test
    public void havingNoGraphAnnotationCreatesAnEmptyDataCollection()
            throws Exception
    {
        assertTrue( "collection was not empty", data.get().isEmpty() );
    }

    @Test
    @Graph( "I know you" )
    public void canCreateGraphFromSingleString() throws Exception
    {
        verifyIknowYou( "know", "I" );
    }

    @Test
    @Graph( { "a TO b", "b TO c", "c TO a" } )
    public void canCreateGraphFromMultipleStrings() throws Exception
    {
        Map<String, Node> graph = data.get();
        Set<Node> unique = new HashSet<Node>();
        Node n = graph.get( "a" );
        while ( unique.add( n ) )
        {
            Transaction transaction = graphdb.beginTx();
            try
            {
                n = n.getSingleRelationship(
                        DynamicRelationshipType.withName( "TO" ),
                        Direction.OUTGOING ).getEndNode();
            }
            finally
            {
                transaction.finish();
            }
        }
        assertEquals( graph.size(), unique.size() );
    }

    @Test
    @Graph( { "a:Person EATS b:Banana" } )
    public void ensurePeopleCanEatBananas() throws Exception
    {
        Map<String, Node> graph = data.get();
        Node a = graph.get( "a" );
        Node b = graph.get( "b" );

        Transaction transaction = graphdb.beginTx();
        try
        {
            assertTrue( a.hasLabel( label( "Person" ) ) );
            assertTrue( b.hasLabel( label( "Banana" ) ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    @Graph( { "a:Person EATS b:Banana", "a EATS b:Apple" } )
    public void ensurePeopleCanEatBananasAndApples() throws Exception
    {
        Map<String, Node> graph = data.get();
        Node a = graph.get( "a" );
        Node b = graph.get( "b" );

        Transaction transaction = graphdb.beginTx();
        try
        {
            assertTrue( "Person label missing", a.hasLabel( label( "Person" ) ) );
            assertTrue( "Banana label missing", b.hasLabel( label( "Banana" ) ) );
            assertTrue( "Apple label missing", b.hasLabel( label( "Apple" ) ) );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    @Graph( value = { "I know you" }, autoIndexNodes=true )
    public void canAutoIndexNodes() throws Exception
    {
        data.get();

        Transaction transaction = graphdb.beginTx();
        try
        {
            assertTrue(
                    "can't look up node.",
                    graphdb().index().getNodeAutoIndexer().getAutoIndex().get(
                            "name", "I" ).hasNext() );
        }
        finally
        {
            transaction.finish();
        }
    }
    
    @Test
    @Graph( nodes = { @NODE( name = "I", setNameProperty=true, properties = {
                    @PROP( key = "name", value = "I" )})}, autoIndexNodes=true )
    public void canAutoIndexNodesExplicitProps() throws Exception
    {
        data.get();

        Transaction transaction = graphdb.beginTx();
        try
        {
            assertTrue(
                    "can't look up node.",
                    graphdb().index().getNodeAutoIndexer().getAutoIndex().get(
                            "name", "I" ).hasNext() );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    @Graph( nodes = {
            @NODE( name = "I", properties = {
                    @PROP( key = "name", value = "me" ),
                    @PROP( key = "bool", value = "true", type = GraphDescription.PropType.BOOLEAN ) } ),
            @NODE( name = "you", setNameProperty = true ) }, relationships = { @REL( start = "I", end = "you", type = "knows", properties = {
            @PROP( key = "name", value = "relProp" ),
            @PROP( key = "valid", value = "true", type = GraphDescription.PropType.BOOLEAN ) } ) }, autoIndexRelationships = true )
    public void canCreateMoreInvolvedGraphWithPropertiesAndAutoIndex()
            throws Exception
    {
        data.get();
        verifyIknowYou( "knows", "me" );
        Transaction transaction = graphdb.beginTx();
        try
        {
            assertEquals( true, data.get().get( "I" ).getProperty( "bool" ) );
            assertFalse( "node autoindex enabled.",
                    graphdb().index().getNodeAutoIndexer().isEnabled() );
            assertTrue(
                    "can't look up rel.",
                    graphdb().index().getRelationshipAutoIndexer().getAutoIndex().get(
                            "name", "relProp" ).hasNext() );
            assertTrue( "relationship autoindex enabled.",
                    graphdb().index().getRelationshipAutoIndexer().isEnabled() );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Graph( value = { "I know you" }, nodes = { @NODE( name = "I", properties = { @PROP( key = "name", value = "me" ) } ) } )
    private void verifyIknowYou( String type, String myName )
    {
        Transaction transaction = graphdb.beginTx();

        try
        {
            Map<String, Node> graph = data.get();
            assertEquals( "Wrong graph size.", 2, graph.size() );
            Node I = graph.get( "I" );
            assertNotNull( "The node 'I' was not defined", I );
            Node you = graph.get( "you" );
            assertNotNull( "The node 'you' was not defined", you );
            assertEquals( "'I' has wrong 'name'.", myName, I.getProperty( "name" ) );
            assertEquals( "'you' has wrong 'name'.", "you",
                    you.getProperty( "name" ) );

            Iterator<Relationship> rels = I.getRelationships().iterator();
            assertTrue( "'I' has too few relationships", rels.hasNext() );
            Relationship rel = rels.next();
            assertEquals( "'I' is not related to 'you'", you, rel.getOtherNode( I ) );
            assertEquals( "Wrong relationship type.", type, rel.getType().name() );
            assertFalse( "'I' has too many relationships", rels.hasNext() );

            rels = you.getRelationships().iterator();
            assertTrue( "'you' has too few relationships", rels.hasNext() );
            rel = rels.next();
            assertEquals( "'you' is not related to 'i'", I, rel.getOtherNode( you ) );
            assertEquals( "Wrong relationship type.", type, rel.getType().name() );
            assertFalse( "'you' has too many relationships", rels.hasNext() );

            assertEquals( "wrong direction", I, rel.getStartNode() );
        }
        finally
        {
            transaction.finish();
        }
    }

    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    @AfterClass
    public static void stopDatabase()
    {
        if ( graphdb != null ) graphdb.shutdown();
        graphdb = null;
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }
}
