/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import javax.ws.rs.core.Response.Status;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

public class CypherFunctionalTest extends AbstractRestFunctionalTestBase {

    
    /**
     * A simple query returning all nodes connected to node 1, returning the
     * node and the name property, if it exists, otherwise `null`:
     */
    @Test
    @Documented
    @Title( "Send a Query" )
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = {
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) },
            relationships = {
                    @REL( start = "I", end = "him", type = "know", properties = { } ),
                    @REL( start = "I", end = "you", type = "know", properties = { } ) } )
    public void testPropertyColumn() throws UnsupportedEncodingException {
        String script = createScript( "start x  = node(%I%) match x -[r]-> n return type(r), n.name?, n.age?" );

        String response = cypherRestCall( script, Status.OK );

        assertThat( response, containsString( "you" ) );
        assertThat( response, containsString( "him" ) );
        assertThat( response, containsString( "25" ) );
        assertThat( response, not( containsString( "\"x\"" ) ) );
    }



    /**
     * Errors on the server will be reported as a JSON-formatted stacktrace and
     * message.
     */
    @Test
    @Documented
    @Title( "Server errors" )
    @Graph( "I know you" )
    public void error_gets_returned_as_json() throws Exception {
        String response = cypherRestCall( "start x = node(%I%) return x.dummy", Status.BAD_REQUEST );
        assertEquals( 3, ( JsonHelper.jsonToMap( response ) ).size() );
    }


    /**
     * Paths can be returned
     * together with other return types by just
     * specifying returns.
     */
    @Test
    @Documented
    @Graph( "I know you" )
    public void return_paths() throws Exception {
        String script = "start x  = node(%I%) match path = (x--friend) return path, friend.name";
        String response = cypherRestCall( script, Status.OK );

        assertEquals( 2, ( JsonHelper.jsonToMap( response ) ).size() );
        assertThat( response, containsString( "data" ) );
        assertThat( response, containsString( "you" ) );
    }

    /**
     * Cypher supports queries with parameters
     * which are submitted as a JSON map.
     */
    @Test
    @Documented
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_parameters() throws Exception {
        data.get();
        String script = "start x  = node:node_auto_index(name={startName}) match path = (x-[r]-friend) where friend" +
                ".name = {name} return TYPE(r)";
        String response = cypherRestCall( script, Status.OK, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );


        assertEquals( 2, ( JsonHelper.jsonToMap( response ) ).size() );
        assertTrue( response.contains( "know" ) );
        assertTrue( response.contains( "data" ) );
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
        String script = "start n = node(%I%) match n-[r]->() return n, r";

        String response = cypherRestCall( script, Status.OK );

        assertThat( response, containsString( "Hello" ) );
        assertThat( response, containsString( "World" ) );
    }
    
    /**
     * Sending a query with syntax errors will give a bad request (HTTP 400)
     * response together with an error message.
     */
    @Test
    @Documented
    @Title( "Send queries with syntax errors" )
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_syntax_errors() throws Exception {
        data.get();
        String script = "start x  = node:node_auto_index(name={startName}) matc path = (x-[r]-friend) where friend" +
                ".name = {name} return TYPE(r)";
        String response = cypherRestCall( script, Status.BAD_REQUEST, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );


        assertEquals( 3, ( JsonHelper.jsonToMap( response ) ).size() );
        assertTrue( response.contains( "message" ) );
    }
    
    /**
     * When sending queries that
     * return nested results like list and maps,
     * these will get serialized into nested JSON representations
     * according to their types.
     */
    @Test
    @Documented
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void nested_results() throws Exception {
        data.get();
        String script = "start n = node(%I%,%you%) return collect(n.name)";
        String response = cypherRestCall(script, Status.OK);

        Map<String, Object> resultMap = JsonHelper.jsonToMap( response );
        assertEquals(2, resultMap.size());
        assertThat(response, containsString("\"I\", \"you\""));
    }

    @Test
    @Graph( value = { "I know you" }, autoIndexNodes = false )
    public void array_property() throws Exception {
        setProperty("I", "array1", new int[] { 1, 2, 3 } );
        setProperty("I", "array2", new String[] { "a", "b", "c" } );

        String script = "start n = node(%I%) return n.array1, n.array2";
        String response = cypherRestCall( script, Status.OK );

        assertThat(response, containsString("[ 1, 2, 3 ]"));
        assertThat(response, containsString("[ \"a\", \"b\", \"c\" ]"));
    }

    void setProperty(String nodeName, String propertyName, Object propertyValue) {
        Node i = this.getNode(nodeName);
        GraphDatabaseService db = i.getGraphDatabase();

        Transaction tx = db.beginTx();
        i.setProperty(propertyName, propertyValue);
        tx.success();
        tx.finish();
    }

    /**
     * This example shows what happens if you misspell
     * an identifier.
     */
    @Test
    @Documented
    @Title("Send queries with errors")
    @Ignore
    @Graph( value = { "I know you" }, autoIndexNodes = true )
    public void send_queries_with_errors() throws Exception {
        data.get();
        String script = "start x  = node:node_auto_index(name={startName}) match path = (x-[r]-friend) where frien" +
                ".name = {name} return TYPE(r)";
        String response = cypherRestCall( script, Status.BAD_REQUEST, Pair.of( "startName", "I" ), Pair.of( "name", "you" ) );

        assertEquals( 3, ( JsonHelper.jsonToMap( response ) ).size() );
        assertThat( response, containsString( "message" ) );
    }

    private String cypherRestCall( String script, Status status, Pair<String, String> ...params )
    {
        return super.doCypherRestCall( cypherUri(), script, status, params );
    }

    private String cypherUri()
    {
        return getDataUri() + "cypher";
    }
}