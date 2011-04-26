package org.neo4j.server.plugin.gremlin;


import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import junit.framework.Assert;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import java.net.URI;

import static org.junit.Assert.assertNotNull;


public class GremlinPluginTest {

    private static ImpermanentGraphDatabase neo4j = null;
    private static GremlinPlugin plugin = null;
    private static OutputFormat json = null;
    private static JSONParser parser = new JSONParser();
    private static String directory = "target/db";
    private static Node firstNode = null;
    private static Node secondNode = null;
    private static Node thirdNode = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        json = new OutputFormat(new JsonFormat(), new URI("http://localhost/"), null);
        neo4j = new ImpermanentGraphDatabase("target/db");
        plugin = new GremlinPlugin();
        Graph graph = new Neo4jGraph(neo4j);
        Vertex marko = graph.addVertex("1");
        marko.setProperty("name", "marko");
        marko.setProperty("age", 29);

        Vertex vadas = graph.addVertex("2");
        vadas.setProperty("name", "vadas");
        vadas.setProperty("age", 27);

        Vertex lop = graph.addVertex("3");
        lop.setProperty("name", "lop");
        lop.setProperty("lang", "java");

        Vertex josh = graph.addVertex("4");
        josh.setProperty("name", "josh");
        josh.setProperty("age", 32);

        Vertex ripple = graph.addVertex("5");
        ripple.setProperty("name", "ripple");
        ripple.setProperty("lang", "java");

        Vertex peter = graph.addVertex("6");
        peter.setProperty("name", "peter");
        peter.setProperty("age", 35);

        graph.addEdge("7", marko, vadas, "knows").setProperty("weight", 0.5f);
        graph.addEdge("8", marko, josh, "knows").setProperty("weight", 1.0f);
        graph.addEdge("9", marko, lop, "created").setProperty("weight", 0.4f);

        graph.addEdge("10", josh, ripple, "created").setProperty("weight", 1.0f);
        graph.addEdge("11", josh, lop, "created").setProperty("weight", 0.4f);

        graph.addEdge("12", peter, lop, "created").setProperty("weight", 0.2f);
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testExecuteScriptVertex() throws Exception {
        JSONObject object = (JSONObject) parser.parse(json.format(GremlinPluginTest.executeTestScript("g.v(1)")));
        Assert.assertEquals(29l, ((JSONObject) object.get("data")).get("age"));
        Assert.assertEquals("marko", ((JSONObject) object.get("data")).get("name"));
    }

    @Test
    public void testExecuteScriptVertices() {
        System.out.println(json.format(GremlinPluginTest.executeTestScript("g.V")));
    }

    @Test
    public void testExecuteScriptEdges() {
        System.out.println(json.format(GremlinPluginTest.executeTestScript("g.E")));
    }

    @Test
    public void testExecuteScriptGraph() {
        System.out.println(json.format(GremlinPluginTest.executeTestScript("[g]")));
    }

    @Test
    public void testExecuteScriptLong() throws Exception {
        Assert.assertEquals(1l, parser.parse(json.format(GremlinPluginTest.executeTestScript("1"))));
    }

    @Test
    public void testExecuteScriptLongs() {
        Assert.assertEquals("[ 1, 2, 5, 6, 8 ]", json.format(GremlinPluginTest.executeTestScript("[1,2,5,6,8]")));
    }

    @Test
    public void testMultiThread() {
        for (int i = 0; i < 250; i++) {
            final int x = i;
            new Thread() {
                public void run() {
                    Assert.assertEquals(x + "", json.format(GremlinPluginTest.executeTestScript("x=" + x + "; x")));
                }
            }.start();
        }
    }

    private static Representation executeTestScript(final String script) {
        Transaction tx = null;
        try {
            tx = neo4j.beginTx();
            return plugin.executeScript(neo4j, script);
        } finally {
            tx.success();
            tx.finish();
        }
      }


    
    

    @Test
    public void testExecuteScriptGetVerticesBySpecifiedName() {
        String script = "g.V[[name:'firstNode']]";
        Transaction tx = null;
        try {
        	System.err.println("GremlinPluginTest::executeScript testExecuteScriptGetVerticesBySpecifiedName the contents of the representation object");
            tx = neo4j.beginTx();
            Representation representation = plugin.executeScript(neo4j, script);
            System.out.println(json.format(representation));
            assertNotNull(representation);
            tx.success();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            tx.finish();
        }

    }
    
    
    
    @Test
    public void testExecuteScriptGetVerticesBySpecifiedNodeType() {
        String script = "g.V[[nodeType:'type 1']]";
        Transaction tx = null;
        try {
        	System.err.println("GremlinPluginTest::executeScript testExecuteScriptGetVerticesBySpecifiedNodeType the contents of the representation object");
            tx = neo4j.beginTx();
            Representation representation = plugin.executeScript(neo4j, script);
            System.out.println(json.format(representation));
            assertNotNull(representation);
            tx.success();

            
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            tx.finish();
        }

    }

}
