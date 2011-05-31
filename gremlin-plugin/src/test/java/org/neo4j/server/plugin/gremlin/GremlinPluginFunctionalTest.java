package org.neo4j.server.plugin.gremlin;

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
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.DocsGenerator;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;

public class GremlinPluginFunctionalTest implements GraphHolder
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script";
    private static ImpermanentGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
    
    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );
    private static WrappingNeoServerBootstrapper server;

    /**
     * Send a Gremlin Script, URL-encoded with UTF-8 encoding, e.g.
     * the equivalent of the Gremlin Script `i = g.v(1);i.outE.inV`"
     */
    @Test
    @Title("Send a Gremlin Script, URL encoded")
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
     * To send a Script JSON encoded, 
     * adjust the payload Content Headers
     */
    @Test
    @Title("Send a Gremlin Script, JSON encoded")
    @Documented
    @Graph( value = { "I know you" } )
    public void testGremlinPostJSON()
    {
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "{\"script\":\"i = g.v("+data.get().get( "I" ).getId() +");i.outE.inV\"}" )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( ENDPOINT )
        .entity();
        assertTrue(response.contains( "you" ));
    }
    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase("target/db" + System.currentTimeMillis());
        
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
