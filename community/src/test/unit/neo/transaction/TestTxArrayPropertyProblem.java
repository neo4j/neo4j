package unit.neo.transaction;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

import junit.framework.TestCase;

public class TestTxArrayPropertyProblem extends TestCase
{
	private static enum SomeRelTypes implements RelationshipType
	{
		NO_RELATIONSHIP
	};
	
	private EmbeddedNeo neo;
	
	@Override
	protected void setUp() throws Exception
	{
		neo = new EmbeddedNeo( SomeRelTypes.class, "var/maan" );
	}
	
	@Override
	protected void tearDown() throws Exception
	{
		neo.shutdown();
	}
	
	public void testMaan() throws Exception
	{
		Node node = null;
		Transaction tx = Transaction.begin();
		try
		{
			node = neo.createNode();
			tx.success();
		}
		finally
		{
			tx.finish();
		}
		
		tx = Transaction.begin();
		String key = "test";
		try
		{
			node.setProperty( key, new String[] { "value1" } );
			tx.success();
		}
		finally
		{
			tx.finish();
		}
		
		tx = Transaction.begin();
		try
		{
			node.setProperty( key, new String[] { "value1", "value2" } );
			// No tx.success() here.
		}
		finally
		{
			tx.finish();
		}
		
		tx = Transaction.begin();
		try
		{
			String[] value = ( String[] ) node.getProperty( key );
			assertEquals( 1, value.length );
			assertEquals( "value1", value[ 0 ] );
		}
		finally
		{
			tx.finish();
		}
	}
}
