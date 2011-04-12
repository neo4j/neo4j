package org.neo4j.server.plugin.gremlin;


import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import java.net.URI;

import static org.junit.Assert.assertNotNull;


public class GremlinPluginTest {

    private static ImpermanentGraphDatabase neo4j = null;
    private static GremlinPlugin plugin = null;
    private static String directory = "target/db";
    private static Node firstNode = null;
    private static Node secondNode = null;
    private static Node thirdNode = null;
    private static OutputFormat json = null;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        json = new OutputFormat(new JsonFormat(), new URI("http://localhost/"), null);
        Transaction tx = null;
        try {
            plugin = new GremlinPlugin();
            neo4j = new ImpermanentGraphDatabase(directory);
            tx = neo4j.beginTx();
            firstNode = neo4j.createNode();
            if (firstNode != null) {
                firstNode.setProperty("name", "firstNode");
                firstNode.setProperty("x", 10.23245);
                firstNode.setProperty("y", -112.346);
                firstNode.setProperty("altitude", 12.34456);
            }

            secondNode = neo4j.createNode();
            if (secondNode != null) {
                secondNode.setProperty("name", "secondNode");
                secondNode.setProperty("x", 1.23245);
                secondNode.setProperty("y", -12.346);
                secondNode.setProperty("altitude", 100.34456);
            }


            thirdNode = neo4j.createNode();
            if (thirdNode != null) {
                thirdNode.setProperty("name", "thirdNode");
                thirdNode.setProperty("x", 11.23245);
                thirdNode.setProperty("y", -112.346);
                thirdNode.setProperty("altitude", 40.34456);
            }
            tx.success();
        } catch (Exception graphEx) {
            System.err.println("Caught a graph exception with message " + graphEx.getMessage());
        } finally {
            tx.finish();
        }

    }


    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testExecuteScript() {
        String script = "g.V";
        Transaction tx = null;
        Representation representation = null;
        try {
            tx = neo4j.beginTx();
            representation = (Representation) plugin.executeScript(neo4j, script);
            System.out.println(json.format(representation));
            assertNotNull(representation);
            tx.success();

            System.err.println("GremlinPluginTest::executeScript the contents of the representation object=" + representation.toString());
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            tx.finish();
        }

    }

}
