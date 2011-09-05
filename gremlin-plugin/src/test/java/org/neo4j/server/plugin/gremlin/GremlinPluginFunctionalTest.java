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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.JSONPrettifier;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;

public class GremlinPluginFunctionalTest implements GraphHolder
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script";
    private static org.neo4j.test.ImpermanentGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    private static WrappingNeoServerBootstrapper server;

    /**
     * Scripts can be sent as URL-encoded
     */
    @Test
    @Title("Send a Gremlin Script - URL encoded")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostURLEncoded() throws UnsupportedEncodingException
    {
        String script = "i = g.v("+data.get().get( "I" ).getId() +");i.out";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .description( formatGroovy( script ) )
        .payload( "script=" + URLEncoder.encode( script, "UTF-8") )
        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }



    /**
     * Send a Gremlin Script, URL-encoded with UTF-8 encoding,
     * with additional parameters.
     */
    @Title("Send a Gremlin Script with variables in a JSON Map - URL encoded")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesURLEncoded() throws UnsupportedEncodingException
    {
        final String script = "g.v(me).out;";
        final String params = "{ \"me\" : "+data.get().get("I").getId()+" }";
        String response = gen.get()
        .description( formatGroovy( script ) )
        .expectedStatus(Status.OK.getStatusCode())
        .payload( "script=" + URLEncoder.encode(script, "UTF-8")+
                "&params=" + URLEncoder.encode(params, "UTF-8")
        )

        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }

    /**
     * Send a Gremlin Script, as JSON payload and additional parameters
     */
    @Test
    @Title("Send a Gremlin Script with variables in a JSON Map")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostWithVariablesAsJson() throws UnsupportedEncodingException
    {
        final String script = "g.v(me).out";
        final String params = "{ \"me\" : "+data.get().get("I").getId()+" }";
        final String payload = String.format("{ \"script\" : \"%s\", \"params\" : %s }", script, params);
        String response = gen.get()
        .description( formatGroovy( script ) )
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }

    /**
     * Import a graph form a http://graphml.graphdrawing.org/[GraphML] file
     * can be achieved through the Gremlin GraphMLReader.
     * The following script imports a small GraphML file from an URL into Neo4j, resulting
     * in the depicted graph. It then
     *  returns a list of all nodes in the graph.
     */
    @Test
    @Documented
    @Title( "Load a sample graph" )
    public void testGremlinImportGraph() throws UnsupportedEncodingException
    {
        String script = "g.loadGraphML('https://raw.github.com/neo4j/gremlin-plugin/master/src/data/graphml1.xml');" +
                "g.V;";
        String payload = "{\"script\":\"" +
        script  +"\"}";
        String response = gen.get()
        .description( formatGroovy( script ) )
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
        assertTrue(response.contains( "him" ));
    }
    
    /**
     * Exporting a graph can be done by simple emitting the appropriate
     * String.
     */
    @Test
    @Documented
    @Title( "Emit a sample graph" )
    @Graph( value = { "I know you", "I know him" } )
    public void emitGraph() throws UnsupportedEncodingException
    {
        data.get();
        String script = "writer = new GraphMLWriter(g);" +
        		"out = new java.io.ByteArrayOutputStream();" +
        		"writer.outputGraph(out);" +
        		"result = out.toString();";
        String payload = "{\"script\":\"" +
        script  +"\"}";
        String response = gen.get()
        .description( formatGroovy( script ) )
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        System.out.println(response);
        assertTrue(response.contains( "graphml" ));
        assertTrue(response.contains( "you" ));
    }


    /**
     * To set variables in the bindings for the Gremlin Script
     * Engine on the server, you can include a +params+ parameter
     * with a String representing a JSON map of variables to set
     * to initial values. These can then be accessed as normal
     * variables within the script.
     */
    @Test
    @Documented
    @Title("Set script variables")
    public void setVariables() throws UnsupportedEncodingException
    {
        String payload = "{\"script\":\"meaning_of_life\","
            + "\"params\":{\"meaning_of_life\" : 42.0}}";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "42.0" ));
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
        String script = "g.v(" + data.get()
        .get( "I" )
        .getId() + ").out.sort{it.name}.toList()";
        String payload = "{\"script\":\""+script +"\"}";
        String response = gen.get()
        .description( formatGroovy( script ) )
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
        assertTrue(response.contains( "him" ));
        assertTrue(response.indexOf( "you" ) > response.indexOf( "him" ));
    }
    
    /**
     * The following script returns a sorted list
     * of all nodes connected via outgoing relationships
     * to node 1, sorted by their `name`-property.
     */
    @Test
    @Title("Return paths from a Gremlin script")
    @Documented
    @Graph( value = { "I know you", "I know him" } )
    public void testScriptWithPaths()
    {
        String script = "g.v(" + data.get()
        .get( "I" )
        .getId() + ").out.name.paths";
        String payload = "{\"script\":\""+script+"\"}";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        System.out.println(response);
        assertTrue(response.contains( ", you]" ));
    }


    @Test
    public void testLineBreaks() throws UnsupportedEncodingException
    {
        //be aware that the string is parsed in Java before hitting the wire,
        //so escape the backslash once in order to get \n on the wire.
        String payload = "{\"script\":\"1\\n2\"}";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "2" ));
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
    @Title("Send a Gremlin Script - JSON encoded with table results")
    @Documented
    @Graph( value = { "I know Joe", "I like cats", "Joe like cats", "Joe like dogs" } )
    public void testGremlinPostJSONWithTableResult()
    {
        String script = "i = g.v("
            + data.get()
            .get( "I" )
            .getId()
            + ");"
            + "t= new Table();"
            + "i.as('I').out('know').as('friend').out('like').as('likes').table(t,['friend','likes']){it.name}{it.name} >> -1;t;";
        String payload = "{\"script\":\""+script +"\"}";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .description( formatGroovy( script ) )
        .post( ENDPOINT )
        
        .entity();
        assertTrue(response.contains( "cats" ));
    }
    
    /**
     * Send an arbitrary Groovy script - Lucene sorting.
     * 
     * This example demonstrates that you via the Groovy runtime
     * embedded with the server have full access to all of the servers
     * Java APIs.
     * The below example 
     * creates Nodes in the database both via the Blueprints and the Neo4j API
     * indexes the nodes via the native Neo4j Indexing API
     * constructs a custom Lucene sorting and searching
     * returns a Neo4j IndexHits result iterator.
     */
    @Test
    @Documented
    @Graph( value = { "I know Joe", "I like cats", "Joe like cats", "Joe like dogs" } )
    public void sendArbtiraryGroovy()
    {
        String script = "" +
                "import org.neo4j.graphdb.index.*;" +
                "import org.neo4j.index.lucene.*;" +
                "import org.apache.lucene.search.*;" +
                "neo4j = g.getRawGraph();" +
                "tx = neo4j.beginTx();" +
                "meVertex = g.addVertex([name:'me']);" +
                "meNode = meVertex.getRawVertex();" +
                "youNode = neo4j.createNode();" +
                "youNode.setProperty('name','you');" +
                "idxManager = neo4j.index();" +
                "personIndex = idxManager.forNodes('persons');" +
                "personIndex.add(meNode,'name',meVertex.name);" +
                "personIndex.add(youNode,'name',youNode.getProperty('name'));" +
                "tx.success();" +
                "tx.finish();" +
                "query = new QueryContext( 'name:*' ).sort( new Sort(new SortField( 'name',SortField.STRING, true ) ) );" +
                "results = personIndex.query( query );";
        String payload = "{\"script\":\""+script+"\"}";
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .description( formatGroovy(script) )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "me" ));
        
    }
    
    /**
     * Imagine a user being part of different groups.
     * A group can have different roles, and a user can
     * be part of different groups. He also can
     * have different roles in different groups apart
     * from the membership.
     * The association of a User, a Group and a Role can
     * be referred to as a _HyperEdge_. However, it can be easily modeled
     * in a property graph as a node that captures this n-ary
     * relationship, as depicted below in the +U1G2R1+ node.
     * 
     * To find out in what roles a user is for a particular
     * groups (here 'Group2'), 
     * the following script can traverse this HyperEdge node
     * and provide answers.
     */
    @Test
    @Title("HyperEdges - find user roles in groups")
    @Documented
    @Graph( value = {  
            "User1 in Group1", 
            "User1 in Group2",
            "Group2 canHave Role2", 
            "Group2 canHave Role1", 
            "Group1 canHave Role1", 
            "Group1 canHave Role2", 
            "Group1 isA Group", 
            "Group2 isA Group", 
            "Role1 isA Role", 
            "Role2 isA Role",
            "User1 hasRoleInGroup U1G2R1",
            "U1G2R1 hasRole Role1",
            "U1G2R1 hasGroup Group2",
            "User1 hasRoleInGroup U1G1R2",
            "U1G1R2 hasRole Role2",
            "U1G1R2 hasGroup Group1"} )
    public void findGroups()
    {
        String script = "" +
                "g.v(" +data.get().get( "User1" ).getId() + ")" +
                		".out('hasRoleInGroup').as('hyperedge')." +
                		"out('hasGroup').filter{it.name=='Group2'}." +
                		"back('hyperedge').out('hasRole').name";
        String payload = "{\"script\":\""+script+"\"}";
        data.get();
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
                .payload( JSONPrettifier.parse( payload ) )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .description( formatGroovy(script) )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "Role1" ));
        assertFalse(response.contains( "Role2" ));
        
    }
    
    
    private String formatGroovy( String script )
    {
        script = script.replace( ";", "\n" );
        if( !script.endsWith( "\n" ) )
        {
            script += "\n";
        }
        return "_Raw script source_\n\n" +
 "[source, groovy]\n" +
        		"----\n" +
        		script +
        		"----\n";
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
        ((ImpermanentGraphDatabase)graphdb).cleanContent();
        server = new WrappingNeoServerBootstrapper(
                graphdb );
        server.start();
        gen.get().setGraph( graphdb );
    }

    @After
    public void shutdownServer() {
        server.stop();
    }
}
