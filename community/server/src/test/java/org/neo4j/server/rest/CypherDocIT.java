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
package org.neo4j.server.rest;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.LABEL;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class CypherDocIT extends AbstractRestFunctionalTestBase {

    @Test
    @Title( "Send a query" )
    @Documented( "A simple query returning all nodes connected to some node, returning the node and the name " +
                 "property, if it exists, otherwise `NULL`:" )
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = {
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) },
            relationships = {
                    @REL( start = "I", end = "him", type = "know", properties = { } ),
                    @REL( start = "I", end = "you", type = "know", properties = { } ) } )
    public void testPropertyColumn() throws UnsupportedEncodingException {
        String script = createScript( "MATCH (x {name: 'I'})-[r]->(n) RETURN type(r), n.name, n.age" );

        String response = cypherRestCall( script, Status.OK );

        assertThat( response, containsString( "you" ) );
        assertThat( response, containsString( "him" ) );
        assertThat( response, containsString( "25" ) );
        assertThat( response, not( containsString( "\"x\"" ) ) );
    }

    @Test
    @Title( "Retrieve query metadata" )
    @Documented("By passing in an additional GET parameter when you execute Cypher queries, metadata about the " +
                "query will be returned, such as how many labels were added or removed by the query.")
    @Graph( nodes = { @NODE( name = "I", setNameProperty = true, labels = { @LABEL( "Director" ) } ) } )
    public void testQueryStatistics() throws JsonParseException
    {
        // Given
        String script = createScript( "MATCH (n {name: 'I'}) SET n:Actor REMOVE n:Director RETURN labels(n)" );

        // When
        Map<String, Object> output = jsonToMap(doCypherRestCall( cypherUri() + "?includeStats=true", script, Status.OK ));

        // Then
        @SuppressWarnings("unchecked")
        Map<String, Object> stats = (Map<String, Object>) output.get( "stats" );

        assertThat( stats, isA( Map.class ) );
        assertThat( (Boolean) stats.get( "contains_updates" ), is( true ) );
        assertThat( (Integer) stats.get( "labels_added" ), is( 1 ) );
        assertThat( (Integer) stats.get( "labels_removed" ), is( 1 ) );
        assertThat( (Integer) stats.get( "nodes_created" ), is( 0 ) );
        assertThat( (Integer) stats.get( "nodes_deleted" ), is( 0 ) );
        assertThat( (Integer) stats.get( "properties_set" ), is( 0 ) );
        assertThat( (Integer) stats.get( "relationships_created" ), is( 0 ) );
        assertThat( (Integer) stats.get( "relationship_deleted" ), is( 0 ) );
    }

    /**
     * Ensure that order of data and column is ok.
     */
    @Test
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = {
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) },
            relationships = {
                    @REL( start = "I", end = "him", type = "know", properties = { } ),
                    @REL( start = "I", end = "you", type = "know", properties = { } ) } )
    public void testDataColumnOrder() throws UnsupportedEncodingException {
        String script = createScript( "MATCH (x)-[r]->(n) WHERE id(x) = %I% RETURN type(r), n.name, n.age" );

        String response = cypherRestCall( script, Status.OK );

        assertThat( response.indexOf( "columns" ) < response.indexOf( "data" ), is( true ));
    }

    @Test
    @Title( "Errors" )
    @Documented( "Errors on the server will be reported as a JSON-formatted message, exception name and stacktrace." )
    @Graph( "I know you" )
    public void error_gets_returned_as_json() throws Exception {
        String response = cypherRestCall( "MATCH (x {name: 'I'}) RETURN x.dummy/0", Status.BAD_REQUEST );
        Map<String, Object> output = jsonToMap( response );
        assertTrue( output.toString(), output.containsKey( "message" ) );
        assertTrue( output.containsKey( "exception" ) );
        assertTrue( output.containsKey( "stackTrace" ) );
    }

    @Test
    @Title( "Return paths" )
    @Documented( "Paths can be returned just like other return types." )
    @Graph( "I know you" )
    public void return_paths() throws Exception {
        String script = "MATCH path = (x {name: 'I'})--(friend) RETURN path, friend.name";
        String response = cypherRestCall( script, Status.OK );

        assertEquals( 2, ( jsonToMap( response ) ).size() );
        assertThat( response, containsString( "data" ) );
        assertThat( response, containsString( "you" ) );
    }

    @Test
    @Title("Use parameters")
    @Documented( "Cypher supports queries with parameters which are submitted as JSON." )
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_parameters() throws Exception {
        data.get();
        String script = "MATCH (x {name: {startName}})-[r]-(friend) WHERE friend"
                        + ".name = {name} RETURN TYPE(r)";
        String response = cypherRestCall( script, Status.OK, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );


        assertEquals( 2, ( jsonToMap( response ) ).size() );
        assertTrue( response.contains( "know" ) );
        assertTrue( response.contains( "data" ) );
    }

    @Test
    @Documented( "Create a node with a label and a property using Cypher. See the request for the parameter " +
                 "sent with the query." )
    @Title( "Create a node" )
    @Graph
    public void send_query_to_create_a_node() throws Exception {
        data.get();
        String script = "CREATE (n:Person { name : {name} }) RETURN n";
        String response = cypherRestCall( script, Status.OK, Pair.of( "name", "Andres" ) );

        assertTrue( response.contains( "name" ) );
        assertTrue( response.contains( "Andres" ) );
    }

    @Test
    @Title( "Create a node with multiple properties" )
    @Documented( "Create a node with a label and multiple properties using Cypher. See the request for the parameter " +
                 "sent with the query." )
    @Graph
    public void send_query_to_create_a_node_from_a_map() throws Exception
    {
        data.get();
        String script = "CREATE (n:Person { props } ) RETURN n";
        String params = "\"props\" : { \"position\" : \"Developer\", \"name\" : \"Michael\", \"awesome\" : true, \"children\" : 3 }";
        String response = cypherRestCall( script, Status.OK, params );

        assertTrue( response.contains( "name" ) );
        assertTrue( response.contains( "Michael" ) );
    }

    @Test
    @Documented( "Create multiple nodes with properties using Cypher. See the request for the parameter sent " +
                 "with the query." )
    @Title( "Create multiple nodes with properties" )
    @Graph
    public void send_query_to_create_multipe_nodes_from_a_map() throws Exception
    {
        data.get();
        String script = "UNWIND {props} as map CREATE (n:Person) SET n = map RETURN n";
        String params = "\"props\" : [ { \"name\" : \"Andres\", \"position\" : \"Developer\" }, { \"name\" : \"Michael\", \"position\" : \"Developer\" } ]";
        String response = cypherRestCall( script, Status.OK, params );

        assertTrue( response.contains( "name" ) );
        assertTrue( response.contains( "Michael" ) );
        assertTrue( response.contains( "Andres" ) );
    }

    @Test
    @Title( "Set all properties on a node using Cypher" )
    @Documented( "Set all properties on a node." )
    @Graph
    public void setAllPropertiesUsingMap() throws Exception
    {
        data.get();
        String script = "CREATE (n:Person { name: 'this property is to be deleted' } ) SET n = { props } RETURN n";
        String params = "\"props\" : { \"position\" : \"Developer\", \"firstName\" : \"Michael\", \"awesome\" : true, \"children\" : 3 }";
        String response = cypherRestCall( script, Status.OK, params );

        assertTrue( response.contains( "firstName" ) );
        assertTrue( response.contains( "Michael" ) );
        assertTrue( !response.contains( "name" ) );
        assertTrue( !response.contains( "deleted" ) );
    }

    @Test
    @Graph( nodes = {
            @NODE( name = "I", properties = {
                @PROP( key = "prop", value = "Hello", type = GraphDescription.PropType.STRING ) } ),
            @NODE( name = "you" ) },
            relationships = {
                @REL( start = "I", end = "him", type = "know", properties = {
                    @PROP( key = "prop", value = "World", type = GraphDescription.PropType.STRING ) } ) } )
    public void nodes_are_represented_as_nodes() throws Exception {
        data.get();
        String script = "MATCH (n)-[r]->() WHERE id(n) = %I% RETURN n, r";

        String response = cypherRestCall( script, Status.OK );

        assertThat( response, containsString( "Hello" ) );
        assertThat( response, containsString( "World" ) );
    }

    @Test
    @Title( "Syntax errors" )
    @Documented( "Sending a query with syntax errors will give a bad request (HTTP 400) response together with " +
                 "an error message." )
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_syntax_errors() throws Exception {
        data.get();
        String script = "START x  = node:node_auto_index(name={startName}) MATC path = (x-[r]-friend) WHERE friend"
                        + ".name = {name} RETURN TYPE(r)";
        String response = cypherRestCall( script, Status.BAD_REQUEST, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );


        Map<String, Object> output = jsonToMap( response );
        assertTrue( output.containsKey( "message" ) );
        assertTrue( output.containsKey( "stackTrace" ) );
    }

    @Test
    @Documented( "When sending queries that\n" +
                 "return nested results like list and maps,\n" +
                 "these will get serialized into nested JSON representations\n" +
                 "according to their types." )
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void nested_results() throws Exception {
        data.get();
        String script = "MATCH (n) WHERE n.name in ['I', 'you'] RETURN collect(n.name)";
        String response = cypherRestCall(script, Status.OK);System.out.println();

        Map<String, Object> resultMap = jsonToMap( response );
        assertEquals( 2, resultMap.size() );
        assertThat( response, anyOf( containsString( "\"I\",\"you\"" ), containsString(
                "\"you\",\"I\"" ), containsString( "\"I\", \"you\"" ), containsString(
                        "\"you\", \"I\"" )) );
    }

    @Test
    @Title( "Profile a query" )
    @Documented( "By passing in an extra parameter, you can ask the cypher executor to return a profile of the " +
                 "query as it is executed. This can help in locating bottlenecks." )
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = {
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) },
            relationships = {
                    @REL( start = "I", end = "him", type = "know", properties = { } ),
                    @REL( start = "I", end = "you", type = "know", properties = { } ) } )
    public void testProfiling() throws Exception {
        String script = createScript( "MATCH (x)-[r]->(n) WHERE id(x) = %I% RETURN type(r), n.name, n.age" );

        // WHEN
        String response = doCypherRestCall( cypherUri() + "?profile=true", script, Status.OK );

        // THEN
        Map<String, Object> des = jsonToMap( response );
        assertThat( des.get( "plan" ), instanceOf( Map.class ));

        @SuppressWarnings("unchecked")
        Map<String, Object> plan = (Map<String, Object>)des.get( "plan" );
        assertThat( plan.get( "name" ), instanceOf( String.class ) );
        assertThat( plan.get( "children" ), instanceOf( Collection.class ));
        assertThat( plan.get( "rows" ), instanceOf( Number.class ));
        assertThat( plan.get( "dbHits" ), instanceOf( Number.class ));
    }

    @Test
    @Graph( value = { "I know you" }, autoIndexNodes = false )
    public void array_property() throws Exception {
        setProperty("I", "array1", new int[] { 1, 2, 3 } );
        setProperty("I", "array2", new String[] { "a", "b", "c" } );

        String script = "MATCH n WHERE id(n) = %I% RETURN n.array1, n.array2";
        String response = cypherRestCall( script, Status.OK );

        assertThat( response, anyOf( containsString( "[ 1, 2, 3 ]" ), containsString( "[1,2,3]" )) );
        assertThat( response, anyOf( containsString( "[ \"a\", \"b\", \"c\" ]" ),
                containsString( "[\"a\",\"b\",\"c\"]" )) );
    }

    void setProperty(String nodeName, String propertyName, Object propertyValue) {
        Node i = this.getNode(nodeName);
        GraphDatabaseService db = i.getGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            i.setProperty(propertyName, propertyValue);
            tx.success();
        }
    }

    @Test
    @Title( "Send queries with errors" )
    @Documented( "This example shows what happens if you misspell an identifier." )
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_errors() throws Exception {
        data.get();
        String script = "START x = node:node_auto_index(name={startName}) MATCH path = (x)-[r]-(friend) WHERE frien"
                        + ".name = {name} RETURN type(r)";
        String response = cypherRestCall( script, Status.BAD_REQUEST, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );

        Map<String, Object> responseMap = jsonToMap( response );
        assertThat( responseMap.keySet(), containsInAnyOrder(
                "message", "exception", "fullname", "stackTrace", "cause", "errors" ) );
        assertThat( response, containsString( "message" ) );
        assertThat( ((String) responseMap.get( "message" )), containsString( "frien not defined" ) );
    }

    @SafeVarargs
    private final String cypherRestCall( String script, Status status, Pair<String, String>... params )
    {
        return doCypherRestCall( cypherUri(), script, status, params );
    }

    private String cypherRestCall( String script, Status status, String paramString )
    {
        return doCypherRestCall( cypherUri(), script, status, paramString );
    }

    private String cypherUri()
    {
        return getDataUri() + "cypher";
    }
}
