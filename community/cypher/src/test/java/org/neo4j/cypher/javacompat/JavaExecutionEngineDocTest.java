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
package org.neo4j.cypher.javacompat;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.cypher.javacompat.RegularExpressionMatcher.matchesPattern;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class JavaExecutionEngineDocTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writerWithDefaultPrettyPrinter();
    private static final File docsTargetDir = new File( "target/docs/dev/general" );
    private GraphDatabaseService db;
    private ExecutionEngine engine;
    private Node andreasNode;
    private Node johanNode;
    private Node michaelaNode;

    @BeforeClass
    public static void prepare()
    {
        docsTargetDir.mkdirs();
    }

    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws IOException
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        engine = new ExecutionEngine( db );
        Transaction tx = db.beginTx();
        andreasNode = db.createNode();
        johanNode = db.createNode();
        michaelaNode = db.getReferenceNode();
        andreasNode.setProperty( "name", "Andreas" );
        johanNode.setProperty( "name", "Johan" );
        michaelaNode.setProperty( "name", "Michaela" );

        index( andreasNode );
        index( johanNode );
        index( michaelaNode );

        tx.success();
        tx.finish();
    }

    @After
    public void shutdownDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }
        db = null;
    }

    private void index( Node n )
    {
        db.index().forNodes( "people" ).add( n, "name", n.getProperty( "name" ) );
    }

    private void dumpToFile( final String id, final String query, final Object params ) throws Exception
    {
        StringBuffer sb = new StringBuffer( 2048 );
        String prettifiedJson = WRITER.writeValueAsString( params );
        sb.append( "\n.Parameters\n[source,javascript]\n----\n" )
                .append( prettifiedJson )
                .append( "\n----\n\n.Query\n" )
                .append( AsciidocHelper.createAsciiDocSnippet( "cypher", engine.prettify( query ) ) );
        AsciiDocGenerator.dumpToSeparateFile( docsTargetDir, id, sb.toString() );
    }

    @Test
    public void exampleQuery() throws Exception
    {
// START SNIPPET: JavaQuery
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute( "start n=node(0) where 1=1 return n" );

        assertThat( result.columns(), hasItem( "n" ) );
        Iterator<Node> n_column = result.columnAs( "n" );
        assertThat( asIterable( n_column ), hasItem( db.getNodeById( 0 ) ) );
// END SNIPPET: JavaQuery
    }

    @Test
    public void shouldBeAbleToEmitJavaIterables() throws Exception
    {
        makeFriends( michaelaNode, andreasNode );
        makeFriends( michaelaNode, johanNode );

        ExecutionEngine engine = new ExecutionEngine( db );

        ExecutionResult result = engine.execute( "start n=node(0) match n-->friend return collect(friend)" );

        Iterable<Node> friends = (Iterable<Node>) result.columnAs( "collect(friend)" ).next();
        assertThat( friends, hasItems( andreasNode, johanNode ) );
        assertThat( friends, instanceOf( Iterable.class ) );
    }

    @Test
    public void testColumnAreInTheRightOrder() throws Exception
    {
        createTenNodes();
        String q = "start one=node(1), two=node(2), three=node(3), four=node(4), five=node(5), six=node(6), " +
                "seven=node(7), eight=node(8), nine=node(9), ten=node(10) " +
                "return one, two, three, four, five, six, seven, eight, nine, ten";
        ExecutionResult result = engine.execute( q );
        assertThat( result.dumpToString(), matchesPattern( "one.*two.*three.*four.*five.*six.*seven.*eight.*nine.*ten" ) );
    }

    private void createTenNodes()
    {
        Transaction tx = db.beginTx();
        for ( int i = 0; i < 10; i++ )
        {
            db.createNode();
        }
        tx.success();
        tx.finish();
    }

    @Test
    public void exampleWithParameterForNodeId() throws Exception
    {
        // START SNIPPET: exampleWithParameterForNodeId
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "id", 0 );
        String query = "start n=node({id}) return n.name";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithParameterForNodeId

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Michaela", n_column.next() );
        dumpToFile( "exampleWithParameterForNodeId", query, params );
    }

    @Test
    public void exampleWithParameterForMultipleNodeIds() throws Exception
    {
        // START SNIPPET: exampleWithParameterForMultipleNodeIds
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "id", Arrays.asList( 0, 1, 2 ) );
        String query = "start n=node({id}) return n.name";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithParameterForMultipleNodeIds

        assertEquals( asList( "Michaela", "Andreas", "Johan" ), this.<String>toList( result, "n.name" ) );
        dumpToFile( "exampleWithParameterForMultipleNodeIds", query, params );
    }

    private <T> List<T> toList( ExecutionResult result, String column )
    {
        List<T> results = new ArrayList<T>();
        IteratorUtil.addToCollection( result.<T>columnAs( column ), results );
        return results;
    }

    @Test
    public void exampleWithStringLiteralAsParameter() throws Exception
    {
        // START SNIPPET: exampleWithStringLiteralAsParameter
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "Johan" );
        String query = "start n=node(0,1,2) where n.name = {name} return n";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithStringLiteralAsParameter

        assertEquals( asList( johanNode ), this.<Node>toList( result, "n" ) );
        dumpToFile( "exampleWithStringLiteralAsParameter", query, params );
    }

    @Test
    public void exampleWithParameterForIndexValue() throws Exception
    {
        Transaction transaction = db.beginTx();
        try
        {
            // START SNIPPET: exampleWithParameterForIndexValue
            Map<String, Object> params = new HashMap<String, Object>();
            params.put( "value", "Michaela" );
            String query = "start n=node:people(name = {value}) return n";
            ExecutionResult result = engine.execute( query, params );
            // END SNIPPET: exampleWithParameterForIndexValue
            assertEquals( asList( michaelaNode ), this.<Node>toList( result, "n" ) );
            dumpToFile( "exampleWithParameterForIndexValue", query, params );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void exampleWithParametersForQuery() throws Exception
    {
        Transaction transaction = db.beginTx();
        try
        {
            // START SNIPPET: exampleWithParametersForQuery
            Map<String, Object> params = new HashMap<String, Object>();
            params.put( "query", "name:Andreas" );
            String query = "start n=node:people({query}) return n";
            ExecutionResult result = engine.execute( query, params );
            // END SNIPPET: exampleWithParametersForQuery
            assertEquals( asList( andreasNode ), this.<Node>toList( result, "n" ) );
            dumpToFile( "exampleWithParametersForQuery", query, params );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void exampleWithParameterForNodeObject() throws Exception
    {
        // START SNIPPET: exampleWithParameterForNodeObject
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "node", andreasNode );
        String query = "start n=node({node}) return n.name";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithParameterForNodeObject

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Andreas", n_column.next() );
    }

    @Test
    public void exampleWithParameterForSkipAndLimit() throws Exception
    {
        // START SNIPPET: exampleWithParameterForSkipLimit
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "s", 1 );
        params.put( "l", 1 );
        String query = "start n=node(0,1,2) return n.name skip {s} limit {l}";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithParameterForSkipLimit

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Andreas", n_column.next() );
        dumpToFile( "exampleWithParameterForSkipLimit", query, params );
    }

    @Test
    public void exampleWithParameterRegularExpression() throws Exception
    {
        // START SNIPPET: exampleWithParameterRegularExpression
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "regex", ".*h.*" );
        String query = "start n=node(0,1,2) where n.name =~ {regex} return n.name";
        ExecutionResult result = engine.execute( query, params );
        // END SNIPPET: exampleWithParameterRegularExpression
        dumpToFile( "exampleWithParameterRegularExpression", query, params );

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Michaela", n_column.next() );
        assertEquals( "Johan", n_column.next() );
    }

    @Test
    public void create_node_from_map() throws Exception
    {
        // START SNIPPET: create_node_from_map
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( "name", "Andres" );
        props.put( "position", "Developer" );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "props", props );
        String query = "create ({props})";
        engine.execute( query, params );
        // END SNIPPET: create_node_from_map
        dumpToFile( "create_node_from_map", query, params );

        ExecutionResult result = engine.execute( "start n=node(*) where n.name = 'Andres' and n.position = 'Developer' return n" );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void create_multiple_nodes_from_map() throws Exception
    {
        // START SNIPPET: create_multiple_nodes_from_map
        Map<String, Object> n1 = new HashMap<String, Object>();
        n1.put( "name", "Andres" );
        n1.put( "position", "Developer" );

        Map<String, Object> n2 = new HashMap<String, Object>();
        n2.put( "name", "Michael" );
        n2.put( "position", "Developer" );

        Map<String, Object> params = new HashMap<String, Object>();
        List<Map<String, Object>> maps = Arrays.asList( n1, n2 );
        params.put( "props", maps );
        String query = "create (n {props}) return n";
        engine.execute( query, params );
        // END SNIPPET: create_multiple_nodes_from_map
        dumpToFile( "create_multiple_nodes_from_map", query, params );

        ExecutionResult result = engine.execute( "start n=node(*) where n.name in ['Andres', 'Michael'] and n.position = 'Developer' return n" );
        assertThat( count( result ), is( 2 ) );
    }

    @Test
    public void set_properties_on_a_node_from_a_map() throws Exception
    {
        // START SNIPPET: set_properties_on_a_node_from_a_map
        Map<String, Object> n1 = new HashMap<String, Object>();
        n1.put( "name", "Andres" );
        n1.put( "position", "Developer" );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "props", n1 );

        String query = "START n=node(0) SET n = {props}";
        engine.execute( query, params );
        // END SNIPPET: set_properties_on_a_node_from_a_map
        dumpToFile( "set_properties_on_a_node_from_a_map", query, params );

        ExecutionResult result = engine.execute( "start n=node(*) where n.name in ['Andres', 'Michael'] and n.position = 'Developer' return n" );
        assertThat( michaelaNode.getProperty( "name" ).toString(), is( "Andres" ) );
    }

    @Test
    public void create_node_using_create_unique_with_java_maps() throws Exception
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( "name", "Andres" );
        props.put( "position", "Developer" );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "props", props );

        String query = "start n=node(0) create unique p = n-[:REL]->({props}) return last(p) as X";
        ExecutionResult result = engine.execute( query, params );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void should_be_able_to_handle_two_params_without_named_nodes() throws Exception
    {
        Map<String, Object> props1 = new HashMap<String, Object>();
        props1.put( "name", "Andres" );
        props1.put( "position", "Developer" );

        Map<String, Object> props2 = new HashMap<String, Object>();
        props2.put( "name", "Lasse" );
        props2.put( "awesome", "true" );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "props1", props1 );
        params.put( "props2", props2 );

        String query = "start n=node(0) create unique p = n-[:REL]->({props1})-[:LER]->({props2}) return p";
        ExecutionResult result = engine.execute( query, params );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void prettifier_makes_pretty() throws Exception
    {
        String given = "match (n)-->() return n";
        String expected = String.format("MATCH (n)-->()%nRETURN n");

        assertEquals(expected, engine.prettify(given));
    }

    private void makeFriends( Node a, Node b )
    {
        Transaction tx = db.beginTx();
        a.createRelationshipTo( b, DynamicRelationshipType.withName( "friend" ) );
        tx.success();
        tx.finish();
    }
}
