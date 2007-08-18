package unit.neo.api;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.IllegalValueException;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionUtil;

import unit.neo.MyRelTypes;

public class TestRelationship extends TestCase
{
	private String key1 = "key1";
	private String key2 = "key2";
	private String key3 = "key3";
	
	public TestRelationship( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestRelationship.class );
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
			e.printStackTrace();
			fail( "Failed to end transaciton, " + e );
		}
	}

	public void testRelationshipCreateAndDelete()
	{
		Node node1 = NodeManager.getManager().createNode();
		Node node2 = NodeManager.getManager().createNode();
		Relationship relationship = 
			NodeManager.getManager().createRelationship( node1, node2, 
				MyRelTypes.TEST );
		Relationship relArray1[] = getRelationshipArray( 
			node1.getRelationships() );
		Relationship relArray2[] = getRelationshipArray( 
			node2.getRelationships() );
		assertEquals( 1, relArray1.length );
		assertEquals( relationship, relArray1[0] );
		assertEquals( 1, relArray2.length );
		assertEquals( relationship, relArray2[0] );
		relArray1 = getRelationshipArray( node1.getRelationships( 
			MyRelTypes.TEST ) );
		assertEquals( 1, relArray1.length );
		assertEquals( relationship,relArray1[0] );
		relArray2 = getRelationshipArray( node2.getRelationships( 
			MyRelTypes.TEST ) );
		assertEquals( 1, relArray2.length );
		assertEquals( relationship, relArray2[0] );
		relArray1 = getRelationshipArray( node1.getRelationships( 
			MyRelTypes.TEST, Direction.OUTGOING ) );
		assertEquals( 1, relArray1.length );
		relArray2 = getRelationshipArray( node2.getRelationships( 
			MyRelTypes.TEST, Direction.INCOMING ) );
		assertEquals( 1, relArray2.length );
		relArray1 = getRelationshipArray( node1.getRelationships( 
			MyRelTypes.TEST, Direction.INCOMING ) );
		assertEquals( 0, relArray1.length );
		relArray2 = getRelationshipArray( node2.getRelationships( 
			MyRelTypes.TEST, Direction.OUTGOING ) );
		assertEquals( 0, relArray2.length );
		relationship.delete();
		node2.delete();
		node1.delete();
	}
	
	private Relationship[] getRelationshipArray( 
		Iterable<Relationship> relsIterable )
	{
		ArrayList<Relationship> relList = new ArrayList<Relationship>();
		for ( Relationship rel : relsIterable )
		{
			relList.add( rel );
		}
		return relList.toArray( new Relationship[ relList.size() ] );
	}

	public void testDeleteWithRelationship()
	{
		// do some evil stuff
		Node node1 = NodeManager.getManager().createNode();
		Node node2 = NodeManager.getManager().createNode();
		NodeManager.getManager().createRelationship( node1, node2, 
			MyRelTypes.TEST );
		node1.delete();
		node2.delete();
		Logger log = Logger.getLogger(
			"org.neo4j.impl.core.NeoConstraintsListener" );
		Level level = log.getLevel();
		log.setLevel( Level.OFF );
		try
		{
			TransactionFactory.getUserTransaction().commit();
			fail( "deleting node with relaitonship should not commit." );
		}
		catch ( Exception e )
		{
			// good
		}
		log.setLevel( level );
	}
	
	public void testDeletedRelationship()
	{
		Node node1 = NodeManager.getManager().createNode();
		Node node2 = NodeManager.getManager().createNode();
		Relationship relationship = 
			NodeManager.getManager().createRelationship( node1, node2, 
				MyRelTypes.TEST );
		relationship.delete();
		Logger log = Logger.getLogger(
			"org.neo4j.impl.core.NeoConstraintsListener" );
		Level level = log.getLevel();
		log.setLevel( Level.OFF );
		try
		{
			relationship.setProperty( "key1", new Integer(1) );
			fail( "Adding property to deleted rel should throw exception." );
		}
		catch ( IllegalValueException e )
		{ // good
		}
		log.setLevel( level );
	}
	
	public void testRelationshipAddProperty()
	{
		try
		{
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			Relationship rel2 = 
				NodeManager.getManager().createRelationship( node2, node1, 
					MyRelTypes.TEST );
			try
			{
				rel1.setProperty( null, null );
				fail( "Null argument should result in exception." );
			}
			catch ( IllegalValueException e )
			{}
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );
			
			// add property
			rel1.setProperty( key1, int1 );
			rel2.setProperty( key1, string1 );
			rel1.setProperty( key2, string2 );
			rel2.setProperty( key2, int2 );
			assertTrue( rel1.hasProperty( key1 ) );
			assertTrue( rel2.hasProperty( key1 ) );
			assertTrue( rel1.hasProperty( key2 ) );
			assertTrue( rel2.hasProperty( key2 ) );
			assertTrue( !rel1.hasProperty( key3 ) );
			assertTrue( !rel2.hasProperty( key3 ) );
			assertEquals( int1, rel1.getProperty( key1 ) );
			assertEquals( string1, rel2.getProperty( key1 ) );
			assertEquals( string2, rel1.getProperty( key2 ) );
			assertEquals( int2, rel2.getProperty( key2 ) );

			// should also test add of already existing property on a 
			// relationship where the property isn't cached...
			// how do we do that?
		
			TransactionFactory.getUserTransaction().setRollbackOnly();
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( javax.transaction.SystemException e )
		{
			fail( "" + e );
		}
	}
	
	public void testRelationshipRemoveProperty()
	{
		try
		{
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );

			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			Relationship rel2 = 
				NodeManager.getManager().createRelationship( node2, node1, 
					MyRelTypes.TEST );
			// verify that we can rely on PL to reomve non existing properties
			try
			{
				if ( rel1.removeProperty( key1 ) != null )
				{
					fail( "Remove of non existing property should return null" 
						);
				}
			}
			catch ( NotFoundException e )
			{}
			try
			{
				rel1.removeProperty( null );
				fail( "Remove null property should throw exception." );
			}
			catch ( IllegalArgumentException e ) 
			{}
			
			rel1.setProperty( key1, int1 );
			rel2.setProperty( key1, string1 );
			rel1.setProperty( key2, string2 );
			rel2.setProperty( key2, int2 );
			try
			{
				rel1.removeProperty( null );
				fail( "Null argument should result in exception." );
			}
			catch ( IllegalArgumentException e )
			{}
			
			// test remove property
			assertEquals( int1, rel1.removeProperty( key1 ) );
			assertEquals( string1, rel2.removeProperty( key1 ) );
			// test remove of non exsisting property
			try
			{
				if ( rel2.removeProperty( key1 ) !=  null )
				{
					fail( "Remove of non existing property should return null" 
						);
				}
			}
			catch ( NotFoundException e )
			{
				// have to set rollback only here
				TransactionFactory.getUserTransaction().setRollbackOnly();
			}
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( javax.transaction.SystemException e )
		{
			fail( "" + e );
		}
	}
	
	public void testRelationshipChangeProperty()
	{
		try
		{
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );

			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			Relationship rel2 = 
				NodeManager.getManager().createRelationship( node2, node1, 
					MyRelTypes.TEST );
			rel1.setProperty( key1, int1 );
			rel2.setProperty( key1, string1 );
			rel1.setProperty( key2, string2 );
			rel2.setProperty( key2, int2 );

			try
			{
				rel1.setProperty( null, null );
				fail( "Null argument should result in exception." );
			}
			catch ( IllegalValueException e )
			{}
			catch ( NotFoundException e )
			{
				fail( "wrong exception" );
			}
			
			// test type change of exsisting property
			// cannot test this for now because of exceptions in PL
			rel2.setProperty( key1, int1 );
			
//			rel1.delete();
//			rel2.delete();
//			node2.delete();
//			node1.delete();
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
	
	public void testRelationshipChangeProperty2()
	{
		try
		{
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string1 = new String( "1" );
			String string2 = new String( "2" );
			Boolean bool1 = new Boolean( true );
			Boolean bool2 = new Boolean( false );

			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			rel1.setProperty( key1, int1 );
			rel1.setProperty( key1, int2 );
			assertEquals( int2, rel1.getProperty( key1 ) );
			rel1.removeProperty( key1 );
			rel1.setProperty( key1, string1 );
			rel1.setProperty( key1, string2 );
			assertEquals( string2, rel1.getProperty( key1 ) );
			rel1.removeProperty( key1 );
			rel1.setProperty( key1, bool1 );
			rel1.setProperty( key1, bool2 );
			assertEquals( bool2, rel1.getProperty( key1 ) );
			rel1.removeProperty( key1 );

			rel1.delete();
			node2.delete();
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
			Integer int1 = new Integer( 1 );
			Integer int2 = new Integer( 2 );
			String string = new String( "3" );

			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			try
			{
				rel1.getProperty( key1 );
				fail( "get non existing property din't throw exception" );
			}
			catch ( NotFoundException e )
			{}
			try
			{
				rel1.getProperty( null );
				fail( "get of null key din't throw exception" );
			}
			catch ( IllegalArgumentException e )
			{}
			assertTrue( !rel1.hasProperty( key1 ) );
 			assertTrue( !rel1.hasProperty( null ) );
			rel1.setProperty( key1, int1 );
			rel1.setProperty( key2, int2 );
			rel1.setProperty( key3, string );
			assertTrue( rel1.hasProperty( key1 ) );
			assertTrue( rel1.hasProperty( key2 ) );
			assertTrue( rel1.hasProperty( key3 ) );
			try
			{
				rel1.removeProperty( key3 );
			}
			catch ( NotFoundException e )
			{
				fail( "Remove of property failed." );
			}
			assertTrue( !rel1.hasProperty( key3 ) );
 			assertTrue( !rel1.hasProperty( null ) );
			rel1.delete();
			node2.delete();
			node1.delete();
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
	}
	
	public void testDirectedRelationship()
	{
		Node node1 = NodeManager.getManager().createNode();
		Node node2 = NodeManager.getManager().createNode();
		Relationship rel2 = 
			NodeManager.getManager().createRelationship( node1, node2, 
				MyRelTypes.TEST );
		Relationship rel3 = 
			NodeManager.getManager().createRelationship( node2, node1, 
				MyRelTypes.TEST );
		Node[] nodes = rel2.getNodes();
		assertEquals( 2, nodes.length );
		assertTrue( nodes[0].equals( node1 ) && nodes[1].equals( node2 ) ); 
		nodes = rel3.getNodes();
		assertEquals( 2, nodes.length );
		assertTrue( nodes[0].equals( node2 ) && nodes[1].equals( node1 ) ); 
		assertEquals( node1, rel2.getStartNode() );
		assertEquals( node2, rel2.getEndNode() );
		assertEquals( node2, rel3.getStartNode() );
		assertEquals( node1, rel3.getEndNode() );

		Relationship relArray[] = getRelationshipArray( 
			node1.getRelationships( MyRelTypes.TEST, Direction.OUTGOING ) );
		assertEquals( 1, relArray.length );
		assertEquals( rel2, relArray[0] );
		relArray = getRelationshipArray( node1.getRelationships( 
			MyRelTypes.TEST, Direction.INCOMING) );
		assertEquals( 1, relArray.length );
		assertEquals( rel3, relArray[0] );
		
		relArray = getRelationshipArray( node2.getRelationships( 
			MyRelTypes.TEST, Direction.OUTGOING ) );
		assertEquals( 1, relArray.length );
		assertEquals( rel3, relArray[0] );
		relArray = getRelationshipArray( node2.getRelationships( 
			MyRelTypes.TEST, Direction.INCOMING ) );
		assertEquals( 1, relArray.length );
		assertEquals( rel2, relArray[0] );
		
		rel2.delete();
		rel3.delete();
		node1.delete();
		node2.delete();
	}
	
	public void testRollbackDeleteRelationship()
	{
		try
		{
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel1 = 
				NodeManager.getManager().createRelationship( node1, node2, 
					MyRelTypes.TEST );
			UserTransaction ut = TransactionFactory.getUserTransaction();
			ut.commit();
			ut.begin();
			node1.delete();
			rel1.delete();
			ut.rollback();
			ut.begin();
			node1.delete();
			node2.delete();
			rel1.delete();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testCreateRelationshipWithCommitts()// throws NotFoundException
	{
		NodeManager nm = NodeManager.getManager();
		Node n1 = nm.createNode();
		TransactionUtil.commitTx( true );
		nm.clearCache();
		TransactionUtil.beginTx();
		n1 = nm.getNodeById( (int) n1.getId() ); 
		Node n2 = nm.createNode();
		nm.createRelationship( n1, n2, MyRelTypes.TEST );
		TransactionUtil.commitTx( true );
		TransactionUtil.beginTx();
		Relationship[] relArray = getRelationshipArray( 
			n1.getRelationships() );
		assertEquals( 1, relArray.length );
		relArray = getRelationshipArray( n1.getRelationships() );
		relArray[0].delete();
		n1.delete();
		n2.delete();
	}

	public void testAddPropertyThenDelete()
	{
		try
		{
			UserTransaction ut = TransactionFactory.getUserTransaction();
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel = NodeManager.getManager().createRelationship( 
				node1, node2, MyRelTypes.TEST );
			rel.setProperty( "test", "test" );
			ut.commit();
			ut.begin();
			rel.setProperty( "test2", "test2" );
			rel.delete();
			node1.delete();
			node2.delete();
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
