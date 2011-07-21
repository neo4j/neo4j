/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.plugin.gremlin;

import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.DocsGenerator;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class GremlinPluginFunctionalTest implements GraphHolder
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script";
    private static org.neo4j.test.ImpermanentGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
    
    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );
    private static WrappingNeoServerBootstrapper server;

    /**
     * Send a Gremlin Script, URL-encoded with UTF-8 encoding, e.g.
     * the equivalent of the Gremlin Script `i = g.v(1);i.outE.inV`
     */
    @Test
    @Title("Send a Gremlin Script - URL encoded")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostURLEncoded() throws UnsupportedEncodingException
    {
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "script=" + URLEncoder.encode( "i = g.v("+data.get().get( "I" ).getId() +");i.outE.inV", "UTF-8") )
        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }

    /**
     * Send a Gremlin Script, URL-encoded with UTF-8 encoding, e.g.
     * the equivalent of the Gremlin Script `g.v(me).outE.inV`
     * with additional parameters as { "me" : 123 }.
     */
    @Test
    @Title("Send a Gremlin Script with variables in a JSON Map - URL encoded")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesURLEncoded() throws UnsupportedEncodingException
    {
        final String script = "g.v(me).outE.inV";
        final String params = "{ \"me\" : "+data.get().get("I").getId()+" }";
        String response = gen.get()
        .expectedStatus(Status.OK.getStatusCode())
        .payload( "script=" + URLEncoder.encode(script, "UTF-8") +
                  "&params=" + URLEncoder.encode(params, "UTF-8")
        )
        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }

    /**
     * Send a Gremlin Script, as JSON payload and additional parameters, e.g.
     * { "script" : "g.v(me).outE.inV", "params" : { "me" : 123 } }
     */
    @Test
    @Title("Send a Gremlin Script with variables in a JSON Map")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesAsJson() throws UnsupportedEncodingException
    {
        final String script = "g.v(me).outE.inV";
        final String params = "{ \\\"me\\\" : "+data.get().get("I").getId()+" }";
        final String payload = String.format("{ \"script\" : \"%s\", \"params\" : \"%s\" }", script, params);
        String response = gen.get()
        .expectedStatus(Status.OK.getStatusCode())
        .payload(payload)
        .payloadType(MediaType.APPLICATION_JSON_TYPE)
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }

    /**
     * Import a graph form a http://graphml.graphdrawing.org/[GraphML] file
     * can be achieved through the Gremlin GraphMLReader.
     * The following script imports 3 nodes into Neo4j
     * and then returns a list of all nodes in the graph.
     */
    @Test
    @Documented
    @Title("Load a sample graph graph")
    public void testGremlinImportGraph() throws UnsupportedEncodingException
    {
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "{\"script\":\"" +
        		"GraphMLReader.inputGraph(g, new URL('https://raw.github.com/neo4j/gremlin-plugin/master/src/data/graphml1.xml').openStream());" +
        		"g.V\"}" )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
        assertTrue(response.contains( "him" ));
    }
    
    

    /**
     * The following script returns a sorted list
     * of all nodes connected via outgoing relationships
     * to node 1, sorted by their `name`-property.
     */
    @Test
    @Documented
    @Title("Sort a result using raw Groovy operations")
    @Graph( value = { "I know you", "I know him" } )
    public void testSortResults() throws UnsupportedEncodingException
    {
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "{\"script\":\"g.v("+data.get().get( "I" ).getId()+").outE.inV.sort{it.name}.toList()\"}" )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
        assertTrue(response.contains( "him" ));
        assertTrue(response.indexOf( "you" ) > response.indexOf( "him" ));
    }

    /**
     * To send a Script JSON encoded, set the payload Content-Type Header.
     * In this example, find all the things that my friends like,
     * and return a table listing my friends by their name, 
     * and the names of the things they like in a table with two columns,
     * ignoring the third named step variable +I+.
     * Remember that everything in Gremlin is an iterator - in order
     * to populate the result table +t+, iterate through the pipes with
     * +>> -1+.
     */
    @Test
    @Title("Send a Gremlin Script - JSON encoded with table result")
    @Documented
    @Graph( value = { "I know Joe", "I like cats", "Joe like cats", "Joe like dogs" } )
    public void testGremlinPostJSONWithTableResult()
    {
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "{\"script\":\"i = g.v("+data.get().get( "I" ).getId() +");" +
        		"t= new Table();" +
        		"i.as('I').out('know').as('friend').out('like').as('likes').table(t,['friend','likes']){it.name}{it.name} >> -1;t;\"}" )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        System.out.println(response);
        //there is nothing returned at all.
        assertTrue(response.contains( "cats" ));
    }
    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase("target/db"+System.currentTimeMillis());
        
    }

    @AfterClass
    public static void stopDatabase()
    {
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }
    
    @Before
    public void startServer() {
        server = new WrappingNeoServerBootstrapper(
                graphdb );
        server.start();
    }
    
    @After
    public void shutdownServer() {
        server.stop();
    }
}
