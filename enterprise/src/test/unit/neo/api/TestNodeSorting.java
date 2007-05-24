package unit.neo.api;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.transaction.TransactionFactory;


public class TestNodeSorting extends TestCase
{
	public TestNodeSorting(String testName)
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestNodeSorting.class );
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
	
	public void testSortByProperty1()
	{
/*		Node nodes[] = new Node[10];
		String key = "akey";
		try
		{
			for ( int i = 0; i < 10; i++ )
			{
				nodes[i] = NodeManager.getManager().createNode();
				nodes[i].setProperty( key, new Integer( 10 - i ) );
			}
			for ( int i = 0; i < 9; i++ )
			{
				NodeManager.getManager().createRelationship( nodes[i], 
					nodes[i + 1],	MyRelTypes.TEST );
			}
			Traverser trav = TraverserFactory.getFactory().createTraverser( 
				Order.DEPTH_FIRST, nodes[0], 
				new RelationshipType[] { MyRelTypes.TEST }, 
				StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );
			trav = trav.sort( new NodeSortInfo.PropertySortInfo<Node>( key ) );
			int nr = 1;
			while ( trav.hasNext() )
			{
				Integer intValue = new Integer( nr++ );
				assertEquals( intValue, trav.nextNode().getProperty( key ) );
			}
			for ( int i = 0; i < 10; i++ )
			{
				for ( Relationship rel : nodes[i].getRelationships() )
				{
					rel.delete();
				}
				nodes[i].delete();
			}
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}*/
	}
	
	public void testSortByProperty2()
	{
/*		Node nodes[] = new Node[10];
		String key = "akey";
		try
		{
			for ( int i = 0; i < 10; i++ )
			{
				nodes[i] = NodeManager.getManager().createNode();
				if ( i < 5 )
				{
					nodes[i].setProperty( key, new Integer( 10 - i ) );
				}
				else if ( i < 7 )
				{
					nodes[i].setProperty( key, "" + (10 - i) );
				}
				else if ( i == 9 )
				{
					nodes[i].setProperty( key, new Boolean( false ) );
				}
			}
			for ( int i = 0; i < 9; i++ )
			{
				NodeManager.getManager().createRelationship( nodes[i], 
					nodes[i + 1], MyRelTypes.TEST );
			}
			Traverser trav = TraverserFactory.getFactory().createTraverser( 
				Order.DEPTH_FIRST, nodes[0], 
				new RelationshipType[] { MyRelTypes.TEST }, 
				StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );
			trav = trav.sort( new NodeSortInfo.PropertySortInfo<Node>( key ) );
			int nr = 1;
			try
			{
				trav.nextNode().getProperty( key );
				fail( "First sorted node shouldn't have property " + key );
			}
			catch ( NotFoundException e )
			{
				// good
			}
			try
			{
				trav.nextNode().getProperty( key );
				fail( "Second sorted node shouldn't have property " + key );
			}
			catch ( NotFoundException e )
			{
				// good
			}
			assertTrue( trav.nextNode().getProperty( key ).equals(
				new Boolean( false ) ) );
			assertEquals( "4", trav.nextNode().getProperty( key ) );
			assertEquals( "5", trav.nextNode().getProperty( key ) );
			nr = 6;
			while ( trav.hasNext() )
			{
				Integer intValue = new Integer( nr++ );
				assertEquals( intValue, trav.nextNode().getProperty( key ) );
			}
			
			for ( int i = 0; i < 10; i++ )
			{
				for ( Relationship rel : nodes[i].getRelationships() )
				{
					rel.delete();
				}
				nodes[i].delete();
			}
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}*/
	}
	
	public void testSortByProperty3()
	{
/*		Node nodes[] = new Node[10];
		String key = "akey";
		try
		{
			for ( int i = 0; i < 10; i++ )
			{
				nodes[i] = NodeManager.getManager().createNode();
				nodes[i].setProperty( key, new Integer( 10 - i ) );
			}
			for ( int i = 0; i < 9; i++ )
			{
				NodeManager.getManager().createRelationship( nodes[i], 
					nodes[i + 1], MyRelTypes.TEST );
			}
			Traverser trav = TraverserFactory.getFactory().createTraverser( 
				Order.DEPTH_FIRST, nodes[0], 
				new RelationshipType[] { MyRelTypes.TEST }, 
				StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );
			trav = trav.sort( 
				new NodeSortInfo.PropertySortInfo<Node>( key, true ) );
			int nr = 10;
			while ( trav.hasNext() )
			{
				Integer intValue = new Integer( nr-- );
				assertEquals( intValue, trav.nextNode().getProperty( key ) );
			}
			for ( int i = 0; i < 10; i++ )
			{
				for ( Relationship rel : nodes[i].getRelationships() )
				{
					rel.delete();
				}
				nodes[i].delete();
			}
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}*/
	}
	
	public void testSortByProperty4()
	{
/*		Node nodes[] = new Node[10];
		String key = "akey";
		try
		{
			for ( int i = 0; i < 10; i++ )
			{
				nodes[i] = NodeManager.getManager().createNode();
				if ( i < 5 )
				{
					nodes[i].setProperty( key, new Integer( 10 - i ) );
				}
				else if ( i < 7 )
				{
					nodes[i].setProperty( key, "" + (10 - i) );
				}
				else if ( i == 9 )
				{
					nodes[i].setProperty( key, new Boolean( false ) );
				}
			}
			for ( int i = 0; i < 9; i++ )
			{
				NodeManager.getManager().createRelationship( nodes[i], 
					nodes[i + 1], MyRelTypes.TEST );
			}
			Traverser trav = TraverserFactory.getFactory().createTraverser( 
				Order.DEPTH_FIRST, nodes[0], 
				new RelationshipType[] { MyRelTypes.TEST }, 
				StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );
			trav = trav.sort( 
				new NodeSortInfo.PropertySortInfo<Node>( key, true ) );

			int nr = 10;
			for ( int i = 0; i < 5; i++ )
			{
				Integer intValue = new Integer( nr-- );
				assertEquals( intValue, trav.nextNode().getProperty( key ) );
			}
			
			assertEquals( "5", trav.nextNode().getProperty( key ) );
			assertEquals( "4", trav.nextNode().getProperty( key ) );
			assertTrue( trav.nextNode().getProperty( key ).equals(
				new Boolean( false ) ) );
			try
			{
				trav.nextNode().getProperty( key );
				fail( "Second sorted node shouldn't have property " + key );
			}
			catch ( NotFoundException e )
			{
				// good
			}
			try
			{
				trav.nextNode().getProperty( key );
				fail( "First sorted node shouldn't have property " + key );
			}
			catch ( NotFoundException e )
			{
				// good
			}
			for ( int i = 0; i < 10; i++ )
			{
				for ( Relationship rel : nodes[i].getRelationships() )
				{
					rel.delete();
				}
				nodes[i].delete();
			}
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}*/
	}
}
