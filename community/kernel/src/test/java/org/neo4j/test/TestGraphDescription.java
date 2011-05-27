package org.neo4j.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;

public class TestGraphDescription implements GraphHolder
{
    private static final TargetDirectory target = TargetDirectory.forTest( TestGraphDescription.class );
    private static EmbeddedGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );

    @Test
    public void havingNoGraphAnnotationCreatesAnEmptyDataCollection() throws Exception
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
            n = n.getSingleRelationship( DynamicRelationshipType.withName( "TO" ), Direction.OUTGOING ).getEndNode();
        }
        assertEquals( graph.size(), unique.size() );
    }

    @Test
    @Graph( nodes = { @NODE( name = "I", properties = { @PROP( key = "name", value = "me" ) } ),
            @NODE( name = "you", setNameProperty = true ) }, relationships = { @REL( start = "I", end = "you", type = "knows" ) } )
    public void canCreateMoreInvolvedGraphWithProperties() throws Exception
    {
        System.out.println( data.get() );
        verifyIknowYou( "knows", "me" );
    }

    @Graph( value = { "I know you" }, nodes = { @NODE( name = "I", properties = { @PROP( key = "name", value = "me" ) } ) } )
    private void verifyIknowYou( String type, String myName )
    {
        Map<String, Node> graph = data.get();
        assertEquals( "Wrong graph size.", 2, graph.size() );
        Node I = graph.get( "I" );
        assertNotNull( "The node 'I' was not defined", I );
        Node you = graph.get( "you" );
        assertNotNull( "The node 'you' was not defined", you );
        assertEquals( "'I' has wrong 'name'.", myName, I.getProperty( "name" ) );
        assertEquals( "'you' has wrong 'name'.", "you", you.getProperty( "name" ) );

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

    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new EmbeddedGraphDatabase( target.graphDbDir( true ).getAbsolutePath() );
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
