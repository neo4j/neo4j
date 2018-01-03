/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.cypher.javacompat.RegularExpressionMatcher.matchesPattern;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class JavaExecutionEngineDocTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writerWithDefaultPrettyPrinter();
    private static final File docsTargetDir = new File( "target/docs/dev/general" );
    private GraphDatabaseService db;
    private Node andreasNode;
    private Node johanNode;
    private Node michaelaNode;

    @BeforeClass
    public static void prepare()
    {
        if( docsTargetDir.exists() )
        {
            return;
        }

        if( !docsTargetDir.mkdirs() )
        {
            fail("Failed to created necessary directories.");
        }
    }

    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws IOException
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            michaelaNode = db.createNode();
            andreasNode = db.createNode();
            johanNode = db.createNode();
            andreasNode.setProperty( "name", "Andreas" );
            johanNode.setProperty( "name", "Johan" );
            michaelaNode.setProperty( "name", "Michaela" );

            index( andreasNode );
            index( johanNode );
            index( michaelaNode );

            tx.success();
        }
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

    public static String parametersToAsciidoc( final Object params ) throws JsonGenerationException,
            JsonMappingException, IOException
    {
        StringBuffer sb = new StringBuffer( 2048 );
        String prettifiedJson = WRITER.writeValueAsString( params );
        sb.append( "\n.Parameters\n[source,javascript]\n----\n" )
                .append( prettifiedJson )
                .append( "\n----\n\n" );
        return sb.toString();
    }

    private void dumpToFile( final String id, final String query, final Object params ) throws Exception
    {
        QueryExecutionEngine engine = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
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
        Result result = db.execute( "MATCH n WHERE id(n) = 0 AND 1=1 RETURN n" );

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

        Result result = db.execute( "MATCH n-->friend WHERE id(n) = 0 RETURN collect(friend)" );

        Iterable<Node> friends = (Iterable<Node>) result.columnAs( "collect(friend)" ).next();
        assertThat( friends, hasItems( andreasNode, johanNode ) );
        assertThat( friends, instanceOf( Iterable.class ) );
    }

    @Test
    public void testColumnAreInTheRightOrder() throws Exception
    {
        createTenNodes();
        String q = "match one, two, three, four, five, six, seven, eight, nine, ten " +
                "where id(one) = 1 and id(two) = 2 and id(three) = 3 and id(four) = 4 and id(five) = 5 " +
                "and id(six) = 6 and id(seven) = 7 and id(eight) = 8 and id(nine) = 9 and id(ten) = 10 " +
                "return one, two, three, four, five, six, seven, eight, nine, ten";
        Result result = db.execute( q );
        assertThat( result.resultAsString(), matchesPattern( "one.*two.*three.*four.*five.*six.*seven.*eight.*nine.*ten" ) );
    }

    private void createTenNodes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                db.createNode();
            }
            tx.success();
        }
    }

    @Test
    public void exampleWithParameterForNodeId() throws Exception
    {
        // START SNIPPET: exampleWithParameterForNodeId
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "id", 0 );
        String query = "MATCH n WHERE id(n) = {id} RETURN n.name";
        Result result = db.execute( query, params );
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
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH n WHERE id(n) in {ids} RETURN n.name";
        Result result = db.execute( query, params );
        // END SNIPPET: exampleWithParameterForMultipleNodeIds

        assertEquals( asList( "Michaela", "Andreas", "Johan" ), this.<String>toList( result, "n.name" ) );
        dumpToFile( "exampleWithParameterForMultipleNodeIds", query, params );
    }

    private <T> List<T> toList( Result result, String column )
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
        String query = "MATCH (n) WHERE n.name = {name} RETURN n";
        Result result = db.execute( query, params );
        // END SNIPPET: exampleWithStringLiteralAsParameter

        assertEquals( asList( johanNode ), this.<Node>toList( result, "n" ) );
        dumpToFile( "exampleWithStringLiteralAsParameter", query, params );
    }

    @Test
    public void exampleWithShortSyntaxStringLiteralAsParameter() throws Exception
    {
        // START SNIPPET: exampleWithShortSyntaxStringLiteralAsParameter
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "Johan" );
        String query = "MATCH (n {name: {name}}) RETURN n";
        Result result = db.execute( query, params );
        // END SNIPPET: exampleWithShortSyntaxStringLiteralAsParameter

        assertEquals( asList( johanNode ), this.<Node>toList( result, "n" ) );
        dumpToFile( "exampleWithShortSyntaxStringLiteralAsParameter", query, params );
    }

    @Test
    public void exampleWithParameterForIndexValue() throws Exception
    {
        try ( Transaction ignored = db.beginTx() )
        {
            // START SNIPPET: exampleWithParameterForIndexValue
            Map<String, Object> params = new HashMap<String, Object>();
            params.put( "value", "Michaela" );
            String query = "START n=node:people(name = {value}) RETURN n";
            Result result = db.execute( query, params );
            // END SNIPPET: exampleWithParameterForIndexValue
            assertEquals( asList( michaelaNode ), this.<Node>toList( result, "n" ) );
            dumpToFile( "exampleWithParameterForIndexValue", query, params );
        }
    }

    @Test
    public void exampleWithParametersForQuery() throws Exception
    {
        try ( Transaction ignored = db.beginTx() )
        {
            // START SNIPPET: exampleWithParametersForQuery
            Map<String, Object> params = new HashMap<String, Object>();
            params.put( "query", "name:Andreas" );
            String query = "START n=node:people({query}) RETURN n";
            Result result = db.execute( query, params );
            // END SNIPPET: exampleWithParametersForQuery
            assertEquals( asList( andreasNode ), this.<Node>toList( result, "n" ) );
            dumpToFile( "exampleWithParametersForQuery", query, params );
        }
    }

    @Test
    public void exampleWithParameterForNodeObject() throws Exception
    {
        // START SNIPPET: exampleWithParameterForNodeObject
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "node", andreasNode );
        String query = "MATCH n WHERE n = {node} RETURN n.name";
        Result result = db.execute( query, params );
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
        String query = "MATCH (n) RETURN n.name SKIP {s} LIMIT {l}";
        Result result = db.execute( query, params );
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
        String query = "MATCH (n) WHERE n.name =~ {regex} RETURN n.name";
        Result result = db.execute( query, params );
        // END SNIPPET: exampleWithParameterRegularExpression
        dumpToFile( "exampleWithParameterRegularExpression", query, params );

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Michaela", n_column.next() );
        assertEquals( "Johan", n_column.next() );
    }

    @Test
    public void exampleWithParameterCSCIStringPatternMatching() throws Exception
    {
        // START SNIPPET: exampleWithParameterCSCIStringPatternMatching
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "Michael" );
        String query = "MATCH (n) WHERE n.name STARTS WITH {name} RETURN n.name";
        Result result = db.execute( query, params );
        // END SNIPPET: exampleWithParameterCSCIStringPatternMatching
        dumpToFile( "exampleWithParameterCSCIStringPatternMatching", query, params );

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Michaela", n_column.next() );
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
        String query = "CREATE ({props})";
        db.execute( query, params );
        // END SNIPPET: create_node_from_map
        dumpToFile( "create_node_from_map", query, params );

        Result result = db.execute( "match (n) where n.name = 'Andres' and n.position = 'Developer' return n" );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void create_multiple_nodes_from_map() throws Exception
    {
        // START SNIPPET: create_multiple_nodes_from_map
        Map<String, Object> n1 = new HashMap<String, Object>();
        n1.put( "name", "Andres" );
        n1.put( "position", "Developer" );
        n1.put( "awesome", true );

        Map<String, Object> n2 = new HashMap<String, Object>();
        n2.put( "name", "Michael" );
        n2.put( "position", "Developer" );
        n2.put( "children", 3 );

        Map<String, Object> params = new HashMap<String, Object>();
        List<Map<String, Object>> maps = Arrays.asList( n1, n2 );
        params.put( "props", maps );
        String query = "CREATE (n:Person {props}) RETURN n";
        db.execute( query, params );
        // END SNIPPET: create_multiple_nodes_from_map
        dumpToFile( "create_multiple_nodes_from_map", query, params );

        Result result = db.execute( "match (n:Person) where n.name in ['Andres', 'Michael'] and n.position = 'Developer' return n" );
        assertThat( count( result ), is( 2 ) );

        result = db.execute( "match (n:Person) where n.children = 3 return n" );
        assertThat( count( result ), is( 1 ) );

        result = db.execute( "match (n:Person) where n.awesome = true return n" );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void set_properties_on_a_node_from_a_map() throws Exception
    {
        try(Transaction tx = db.beginTx())
        {
            // START SNIPPET: set_properties_on_a_node_from_a_map
            Map<String, Object> n1 = new HashMap<>();
            n1.put( "name", "Andres" );
            n1.put( "position", "Developer" );

            Map<String, Object> params = new HashMap<>();
            params.put( "props", n1 );

            String query = "MATCH (n) WHERE n.name='Michaela' SET n = {props}";
            db.execute( query, params );
            // END SNIPPET: set_properties_on_a_node_from_a_map
            dumpToFile( "set_properties_on_a_node_from_a_map", query, params );

            db.execute( "match (n) where n.name in ['Andres', 'Michael'] and n.position = 'Developer' return n" );
            assertThat( michaelaNode.getProperty( "name" ).toString(), is( "Andres" ) );
        }
    }

    @Test
    public void create_node_using_create_unique_with_java_maps() throws Exception
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( "name", "Andres" );
        props.put( "position", "Developer" );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "props", props );

        String query = "MATCH n WHERE id(n) = 0 CREATE UNIQUE p = n-[:REL]->({props}) RETURN last(nodes(p)) AS X";
        Result result = db.execute( query, params );
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

        String query = "MATCH n WHERE id(n) = 0 CREATE UNIQUE p = n-[:REL]->({props1})-[:LER]->({props2}) RETURN p";
        Result result = db.execute( query, params );
        assertThat( count( result ), is( 1 ) );
    }

    @Test
    public void prettifier_makes_pretty() throws Exception
    {
        QueryExecutionEngine engine = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        String given = "match (n)-->() return n";
        String expected = String.format("MATCH (n)-->()%nRETURN n");

        assertEquals(expected, engine.prettify(given));
    }

    @Test
    public void explain_returns_plan() throws Exception
    {
        // START SNIPPET: explain_returns_plan
        Result result = db.execute( "EXPLAIN CREATE (user:User{name:{name}}) RETURN user" );

        assert result.getQueryExecutionType().isExplained();
        assert result.getQueryExecutionType().requestedExecutionPlanDescription();
        assert !result.hasNext();
        assert !result.getQueryStatistics().containsUpdates();
        assert result.columns().isEmpty();
        assert !result.getExecutionPlanDescription().hasProfilerStatistics();
        // END SNIPPET: explain_returns_plan
    }

    private void makeFriends( Node a, Node b )
    {
        try ( Transaction tx = db.beginTx() )
        {
            a.createRelationshipTo( b, DynamicRelationshipType.withName( "friend" ) );
            tx.success();
        }
    }
}
