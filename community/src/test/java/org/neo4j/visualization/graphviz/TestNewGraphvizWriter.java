package org.neo4j.visualization.graphviz;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.visualization.graphviz.GraphvizWriter;
import org.neo4j.walk.Walker;

public class TestNewGraphvizWriter
{
	enum type implements RelationshipType
	{
		KNOWS, WORKS_FOR
	}

	private EmbeddedNeo neo;

	@Before
	public void setUp()
	{
		neo = new EmbeddedNeo( "target/neo" );
	}

	@After
	public void tearDown()
	{
		neo.shutdown();
	}

	@Test
	public void testSimpleGraph() throws Exception
	{
		Transaction tx = neo.beginTx();
		try
		{
			final Node emil = neo.createNode();
			emil.setProperty( "name", "Emil Eifrém" );
			emil.setProperty( "age", 30 );
			final Node tobias = neo.createNode();
			tobias.setProperty( "name", "Tobias \"snuttis\" Ivarsson" );
			tobias.setProperty( "age", 23 );
			tobias.setProperty( "hours", new int[] { 10, 10, 4, 4, 0 } );
			final Node johan = neo.createNode();
			johan.setProperty( "name", "Johan '\\n00b' Svensson" );
			final Relationship emilKNOWStobias = emil.createRelationshipTo(
			    tobias, type.KNOWS );
			emilKNOWStobias.setProperty( "since", "2003-08-17" );
			final Relationship johanKNOWSemil = johan.createRelationshipTo(
			    emil, type.KNOWS );
			final Relationship tobiasKNOWSjohan = tobias.createRelationshipTo(
			    johan, type.KNOWS );
			final Relationship tobiasWORKS_FORemil = tobias
			    .createRelationshipTo( emil, type.WORKS_FOR );
			OutputStream out = new ByteArrayOutputStream();
			GraphvizWriter writer = new GraphvizWriter();
			writer.emit( out, new Walker( emil.traverse( Order.DEPTH_FIRST,
			    StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
			    type.KNOWS, Direction.BOTH, type.WORKS_FOR, Direction.BOTH ),
			    type.KNOWS, type.WORKS_FOR ) );
			tx.success();
			System.out.println( out.toString() );
		}
		finally
		{
			tx.finish();
		}
	}
}
