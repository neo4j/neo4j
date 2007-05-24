package unit.neo.api;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.impl.core.IllegalValueException;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.TransactionFactory;

public class TestNode extends TestCase
{
	public TestNode( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestNode.class );
		return suite;
	}
	
	public void setUp()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() != Status.STATUS_NO_TRANSACTION )
			{
				fail ( "Status is not STATUS_NO_TRANSACTION but: " + 
					ut.getStatus() );
			}
			ut.begin();
		}
		catch ( Exception e )
		{
			fail( "Failed to start transaction, " + e );
		}
	}
	
	public void tearDown()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() == Status.STATUS_ACTIVE )
			{
				ut.commit();
			}
			else if ( ut.getStatus() == Status.STATUS_MARKED_ROLLBACK )
			{
				ut.rollback();
			}
			else if ( ut.getStatus() == Status.STATUS_NO_TRANSACTION )
			{
				// do nothing
			}
			else
			{
				System.out.println( "ARGH." );
				fail( "Unkown transaction status[" + ut.getStatus() + "]." );
			}
		}
		catch ( Exception e )
		{
			fail( "Failed to end transaciton, " + e );
		}
	}

	public void testNodeCreateAndDelete()
	{
		int nodeId = -1;
		try
		{
			Node node = NodeManager.getManager().createNode();
			nodeId = (int) node.getId();
			NodeManager.getManager().getNodeById( nodeId );
			node.delete();
			try
			{
				TransactionFactory.getUserTransaction().commit();
			}
			catch ( Exception e )
			{
				fail( "Commit failed." );
			}
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}

		try
		{
			// setup a new transaction
			TransactionFactory.getUserTransaction().begin();
		}
		catch ( Exception e )
		{
			fail( "Begin transaction." );
		}
		try
		{
			NodeManager.getManager().getNodeById( nodeId );
			fail( "Node[" + nodeId + "] should be deleted." );
		}
		catch ( NotFoundException e )
		{ }
	}
	
	public void testDeletedNode()
	{
		// do some evil stuff
		Node node = NodeManager.getManager().createNode();
		node.delete();
		Logger log = Logger.getLogger( 
			"org.neo4j.impl.core.NeoConstraintsListener" );
		Level level = log.getLevel();
		log.setLevel( Level.OFF );
		try
		{
			node.setProperty( "key1", new Integer( 1 ) );
			fail( "Adding stuff to deleted node should throw exception" );
		}
		catch ( IllegalValueException e )
		{ // good
		}
		log.setLevel( level );
	}
	
	public void testNodeAddProperty()
	{
		try
		{
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			try
			{
				node1.setProperty( null, null );
				fail( "Null argument should result in exception." );
			}
			catch ( IllegalValueException e )
			{}
			String key1 = "key1";
			String key2 = "key2";
			String key3 = "key3";
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );
			
			// add property
			node1.setProperty( key1, int1 );
			node2.setProperty( key1, string1 );
			node1.setProperty( key2, string2 );
			node2.setProperty( key2, int2 );
			assertTrue( node1.hasProperty( key1 ) );
			assertTrue( node2.hasProperty( key1 ) );
			assertTrue( node1.hasProperty( key2 ) );
			assertTrue( node2.hasProperty( key2 ) );
			assertTrue( !node1.hasProperty( key3 ) );
			assertTrue( !node2.hasProperty( key3 ) );
			assertEquals( int1, node1.getProperty( key1 ) );
			assertEquals( string1, node2.getProperty( key1 ) );
			assertEquals( string2, node1.getProperty( key2 ) );
			assertEquals( int2, node2.getProperty( key2 ) );
			
			// should also test add of already existing property on a node
			// in phase I or phase II where the property isn't cached...
			// how do we do that?
			
			TransactionFactory.getUserTransaction().setRollbackOnly();
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( javax.transaction.SystemException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeRemoveProperty()
	{
		try
		{
			String key1 = "key1";
			String key2 = "key2";
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );

			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();

			try
			{
				node1.removeProperty( key1 );
				fail( "Remove of non existing property should throw exception." 
					);
			}
			catch ( NotFoundException e )
			{}
			try
			{
				node1.removeProperty( null );
				fail( "Remove null property should throw exception." );
			}
			catch ( NotFoundException e ) 
			{}
			
			node1.setProperty( key1, int1 );
			node2.setProperty( key1, string1 );
			node1.setProperty( key2, string2 );
			node2.setProperty( key2, int2 );
			try
			{
				node1.removeProperty( null );
				fail( "Null argument should result in exception." );
			}
			catch ( NotFoundException e )
			{}
			
			// test remove property
			assertEquals( int1, node1.removeProperty( key1 ) );
			assertEquals( string1, node2.removeProperty( key1 ) );
			// test remove of non exsisting property
			try
			{
				node2.removeProperty( key1 );
				fail( "Remove of non existing property should fail." );
			}
			catch ( NotFoundException e )
			{
				// must mark as rollback only
				try
				{
					TransactionFactory.getUserTransaction().setRollbackOnly();
				}
				catch ( javax.transaction.SystemException se )
				{
					fail( "Mark as rollback only failed. " + se );
				}
			}
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeChangeProperty()
	{
		try
		{
			String key1 = "key1";
			String key2 = "key2";
			String key3 = "key3";
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );
			Boolean bool1 = new Boolean( true );
			Boolean bool2 = new Boolean( false );
			
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			node1.setProperty( key1, int1 );
			node2.setProperty( key1, string1 );
			node1.setProperty( key2, string2 );
			node2.setProperty( key2, int2 );
			
			try
			{
				node1.setProperty( null, null );
				fail( "Null argument should result in exception." );
			}
			catch ( IllegalValueException e )
			{}
			catch ( NotFoundException e )
			{
				fail( "wrong exception" );
			}
			
			// test change property
			node1.setProperty( key1, int2 );
			node2.setProperty( key1, string2 );
			assertEquals( string2, node2.getProperty( key1 ) );
			node1.setProperty( key3, bool1 );
			node1.setProperty( key3, bool2 );
			// test type change of exsisting property
			// cannot test this for now because of exceptions in PL
			/*try
			{
				node2.changeProperty( key1, int1 );
				fail( "Changing type should throw exception." );
			}
			catch ( IllegalValueException e )
			{
				ut.rollback();
			}*/
			assertEquals( string2, node2.getProperty( key1 ) );
			node1.delete();
			node2.delete();
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeChangeProperty2()
	{
		try
		{
			String key1 = "key1";
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );
			Boolean bool1 = new Boolean( true );
			Boolean bool2 = new Boolean( false );
			Node node1 = NodeManager.getManager().createNode();
			node1.setProperty( key1, int1 );
			node1.setProperty( key1, int2 );
			assertEquals( int2, node1.getProperty( key1 ) );
			node1.removeProperty( key1 );
			node1.setProperty( key1, string1 );
			node1.setProperty( key1, string2 );
			assertEquals( string2, node1.getProperty( key1 ) );
			node1.removeProperty( key1 );
			node1.setProperty( key1, bool1 );
			node1.setProperty( key1, bool2 );
			assertEquals( bool2, node1.getProperty( key1 ) );
			node1.removeProperty( key1 );
			node1.delete();
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeGetProperties()
	{
		try
		{
			String key1 = "key1";
			String key2 = "key2";
			String key3 = "key3";
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string = new String( "3" );

			Node node1 = NodeManager.getManager().createNode();
			assertTrue( !node1.getPropertyValues().iterator().hasNext() );
			try
			{
				node1.getProperty( key1 );
				fail( "get non existing property din't throw exception" );
			}
			catch ( NotFoundException e )
			{}
			try
			{
				node1.getProperty( null );
				fail( "get of null key din't throw exception" );
			}
			catch ( NotFoundException e )
			{}
			assertTrue( !node1.hasProperty( key1 ) );
			assertTrue( !node1.hasProperty( null ) );
			node1.setProperty( key1, int1 );
			node1.setProperty( key2, int2 );
			node1.setProperty( key3, string );
			Iterator<Object> values = node1.getPropertyValues().iterator();
			values.next(); values.next(); values.next();
			Iterator<String> keys = node1.getPropertyKeys().iterator();
			keys.next(); keys.next(); keys.next();
			assertTrue( node1.hasProperty( key1 ) );
			assertTrue( node1.hasProperty( key2 ) );
			assertTrue( node1.hasProperty( key3 ) );
			try
			{
				node1.removeProperty( key3 );
			}
			catch ( NotFoundException e )
			{
				fail( "Remove of property failed." );
			}
			assertTrue( !node1.hasProperty( key3 ) );
			assertTrue( !node1.hasProperty( null ) );
			node1.delete();
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
	}
	
	public void testAddPropertyThenDelete()
	{
		try
		{
			UserTransaction ut = TransactionFactory.getUserTransaction();
			Node node = NodeManager.getManager().createNode();
			node.setProperty( "test", "test" );
			ut.commit();
			ut.begin();
			node.setProperty( "test2", "test2" );
			node.delete();
			ut.commit();
			ut.begin();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
}
