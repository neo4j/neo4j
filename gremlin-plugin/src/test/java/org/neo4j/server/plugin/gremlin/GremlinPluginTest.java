package org.neo4j.server.plugin.gremlin;


import static org.junit.Assert.*;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.plugin.gremlin.GremlinPlugin;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;


public class GremlinPluginTest {

	private static EmbeddedGraphDatabase curGraphDBServiceObj = null;
	private static GremlinPlugin curGremlinPluginObj = null;
	private static String curGraphDBEmbeddedPath="target/db";
	private static Node firstNode = null;
	private static Node secondNode = null;
	private static Node thirdNode = null;
	private static OutputFormat json = null;	


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	    json = new OutputFormat(new JsonFormat(), new URI( "http://localhost/" ), null );
		Transaction tx= null;
		try
		{
			curGremlinPluginObj = new GremlinPlugin();
			curGraphDBServiceObj=new EmbeddedGraphDatabase(curGraphDBEmbeddedPath);
			tx=curGraphDBServiceObj.beginTx();
			firstNode=curGraphDBServiceObj.createNode();
			if (firstNode!=null)
			{
				firstNode.setProperty("name","firstNode");
				firstNode.setProperty("x", 10.23245);
				firstNode.setProperty("y", -112.346);
				firstNode.setProperty("altitude",12.34456);
			}
		
			secondNode=curGraphDBServiceObj.createNode();
			if (secondNode!=null)
			{
				secondNode.setProperty("name","secondNode");
				secondNode.setProperty("x", 1.23245);
				secondNode.setProperty("y", -12.346);
				secondNode.setProperty("altitude",100.34456);
			}
		
		
			thirdNode=curGraphDBServiceObj.createNode();
			if (thirdNode!=null)
			{
				thirdNode.setProperty("name","thirdNode");
				thirdNode.setProperty("x", 11.23245);
				thirdNode.setProperty("y", -112.346);
				thirdNode.setProperty("altitude",40.34456);
			}
			tx.success();
		}
		catch (Exception graphEx)
		{
			System.err.println("Caught a graph exception with message "+graphEx.getMessage());
		}
		finally
		{
			tx.finish();
		}
		
	}
	
	

	@After
	public void tearDown() throws Exception {
	}
	
	
	
	/* Pass in a reference node and the current
	 * embedded graph database that we're testing
	 * and get back the list of vertices
	 * 
	*/
	@Test
	public void testGetVertices() {
		String script="results.add(g.V)";
		Transaction tx= null;
		Representation curRepresentationObj = null;
		try
		{
			tx=curGraphDBServiceObj.beginTx();
			curRepresentationObj=curGremlinPluginObj.getVertices(script, curGraphDBServiceObj);
			assertNotNull(curRepresentationObj);
			tx.success();
		}
		catch (Throwable t) {
			t.printStackTrace ();
		}
		finally
		{
			tx.finish();
		}
		System.out.println(json.format( curRepresentationObj ));
	}
	
	
	
	
	/* Pass in a reference node and the current
	 * embedded graph database that we're testing
	 * and get back the list of edges
	 * 
	*/
	@Test
	public void testGetEdges() {
		String script="results.add(g.E)";
		Transaction tx= null;
		Representation curRepresentationObj = null;
		try
		{
			tx=curGraphDBServiceObj.beginTx();
			curRepresentationObj=(Representation)curGremlinPluginObj.getEdges(script, curGraphDBServiceObj);
			assertNotNull(curRepresentationObj);
			tx.success();
			
			System.err.println("GremlinPluginTest::testGetEdges the contents of the representation object="+curRepresentationObj.toString());
		}
		catch (Throwable t) {
			t.printStackTrace ();
		}
		finally
		{
			tx.finish();
		}
		
	}

}
