package org.neo4j.server.plugin.gremlin;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.rest.DocsGenerator;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;

public class TestGremlin implements GraphHolder
{
    private static ImpermanentGraphDatabase graphdb;
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
    
    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );
    private static WrappingNeoServerBootstrapper server;

    @Test
    @Ignore
    @Title("Send a Gremlin Script, URL encoded")
    @Documented("Send a Gremlin Script, URL-encoded")
    @Graph( value = { "I know you" } )
    public void testGremlinPostURLEncoded()
    {
        
        server = new WrappingNeoServerBootstrapper(
                graphdb );
        server.start();
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "script=g.v(2).outE.inV" )
        .payloadType( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
        .post( "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script" )
        .entity();
        System.out.println(response);
        assertTrue(response.contains( "you" ));
        server.stop();
        
    }

    @Test
    @Title("Send a Gremlin Script, JSON encoded")
    @Documented("To send a Script JSON encoded, " +
    		"adjust the payload Content Headers")
    @Graph( value = { "I know you" } )
    public void testGremlinPostJSON()
    {
        server = new WrappingNeoServerBootstrapper(
                graphdb );
        server.start();
        String response = gen.get()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( "{\"script\":\"g.v(2).outE.inV\"}" )
        .payloadType( MediaType.APPLICATION_JSON_TYPE )
        .post( "http://localhost:7474/db/data/ext/GremlinPlugin/graphdb/execute_script" )
        .entity();
        System.out.println(response);
        assertTrue(response.contains( "you" ));
        server.stop();
        
    }
    @BeforeClass
    public static void startDatabase()
    {
        graphdb = new ImpermanentGraphDatabase("target/db");
        
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
}
