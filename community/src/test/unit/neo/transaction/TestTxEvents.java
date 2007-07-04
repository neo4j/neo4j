package unit.neo.transaction;

import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.event.ReActiveEventListener;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.UserTransactionImpl;

import unit.neo.MyRelTypes;

public class TestTxEvents extends TestCase
{	
	public TestTxEvents( String name )
	{
		super( name );
	}
	
	public static Test suite()
	{
		return new TestSuite( TestTxEvents.class );
	}
	
	// Small inner class that listens to a specific event (specified in the
	// constructor) and turns on a flag if it's been received
	private static class EventConsumer implements ReActiveEventListener
	{
		private boolean received = false;
		private Event eventType = null;
		private Integer eventIdentifier = null;
		
		EventConsumer( Event eventType )
		{
			this.eventType = eventType;
		}
		
		public void reActiveEventReceived( Event event, EventData data )
		{
			if ( event == this.eventType )
			{
				this.received = true;
				this.eventIdentifier = ( Integer ) data.getData();
			}
		}
		
		boolean received()
		{
			return this.received;
		}
		
		Integer getEventIdentifier()
		{
			return this.eventIdentifier;
		}
	}
	
	private Integer performOperation( Event operation, UserTransactionImpl tx )
	{
		Integer eventIdentifier = null;
		try
		{
			if ( operation == Event.TX_BEGIN )
			{
				tx.begin();
				eventIdentifier = tx.getEventIdentifier();
			}
			else if ( operation == Event.TX_ROLLBACK )
			{
				tx.begin();
				eventIdentifier = tx.getEventIdentifier();
				tx.rollback();
			}
			else if ( operation == Event.TX_COMMIT )
			{
				tx.begin();
				eventIdentifier = tx.getEventIdentifier();
				tx.commit();
			}
			else
			{
				fail( "Unknown operation to test: " + operation );
			}
		}
		catch ( Exception e )
		{
			fail( "Failed executing operation for: " + operation );
		}
		return eventIdentifier;
	}
	
	// operation = { TX_BEGIN, TX_ROLLBACK, TX_COMMIT }
	private void testTxOperation( Event operation )
	{
		EventConsumer eventHook = new EventConsumer( operation );
		UserTransactionImpl tx = 
			( UserTransactionImpl ) TransactionFactory.getUserTransaction();
		try
		{
			EventManager.getManager().
				registerReActiveEventListener( eventHook, operation );
			Integer eventIdentifier = performOperation( operation, tx );
			Thread.sleep( 500 ); // should be enough to propagate the event
			assertTrue( operation + " event not generated",
						eventHook.received() );
			assertEquals( "Event generated, but with wrong event identifier",
						  eventHook.getEventIdentifier(),
						  eventIdentifier );
		}
		catch ( Exception e )
		{
			fail( "Exception raised during testing of TX_BEGIN event: " + e );
		}
		finally
		{
			try
			{
				EventManager.getManager().
					unregisterReActiveEventListener( eventHook, operation );
				tx.rollback();
			}
			catch ( Exception e ) { } // Just ignore
		}
	}
	
	public void testTxBegin()
	{
		this.testTxOperation( Event.TX_BEGIN );
	}
	
	public void testTxRollback()
	{
		this.testTxOperation( Event.TX_ROLLBACK );		
	}
	
	public void testTxCommit()
	{
		this.testTxOperation( Event.TX_COMMIT );		
	}
	
	public void testSelfMarkedRollback()
	{
		EventConsumer eventHook = new EventConsumer( Event.TX_ROLLBACK );
		UserTransactionImpl tx = 
			( UserTransactionImpl ) TransactionFactory.getUserTransaction();
		try
		{
			EventManager.getManager().
				registerReActiveEventListener( eventHook, Event.TX_ROLLBACK );
			tx.begin();
			Integer eventIdentifier = tx.getEventIdentifier();
			Node node = NodeManager.getManager().createNode();
			tx.setRollbackOnly();
			try
			{
				tx.commit();
				fail( "Marked rollback tx should throw exception on commit" );
			}
			catch ( javax.transaction.RollbackException e )
			{ // good
			}
			Thread.sleep( 500 ); // should be enough to propagate the event
			assertTrue( Event.TX_ROLLBACK + " event not generated",
						eventHook.received() );
			assertEquals( "Event generated, but with wrong event identifier",
						  eventHook.getEventIdentifier(),
						  eventIdentifier );
			tx.begin();
			try
			{
				NodeManager.getManager().getNodeById( (int) node.getId() );
				fail( "Node exist but tx should have rolled back" );
			}
			catch ( org.neo4j.impl.core.NotFoundException e )
			{ // good
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Exception raised during testing of TX_BEGIN event: " + e );
		}
		finally
		{
			try
			{
				EventManager.getManager().
					unregisterReActiveEventListener( eventHook, 
						Event.TX_ROLLBACK );
				tx.rollback();
			}
			catch ( Exception e ) { } // Just ignore
		}
	}

	public void testMarkedRollback()
	{
		Logger log = Logger.getLogger(
			"org.neo4j.impl.core.NeoConstraints" );
		Level level = log.getLevel();
		log.setLevel( Level.OFF );
		EventConsumer eventHook = new EventConsumer( Event.TX_ROLLBACK );
		UserTransactionImpl tx = 
			( UserTransactionImpl ) TransactionFactory.getUserTransaction();
		try
		{
			EventManager.getManager().
				registerReActiveEventListener( eventHook, Event.TX_ROLLBACK );
			tx.begin();
			Integer eventIdentifier = tx.getEventIdentifier();
			NodeManager nm = NodeManager.getManager();
			Node node1 = nm.createNode();
			Node node2 = nm.createNode();
			nm.createRelationship( node1, node2, MyRelTypes.TEST );
			node1.delete();
			try
			{
				tx.commit();
				fail( "tx should throw exception on commit" );
			}
			catch ( javax.transaction.RollbackException e )
			{ // good
			}
			Thread.sleep( 500 ); // should be enough to propagate the event
			assertTrue( Event.TX_ROLLBACK + " event not generated",
						eventHook.received() );
			assertEquals( "Event generated, but with wrong event identifier",
						  eventHook.getEventIdentifier(),
						  eventIdentifier );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Exception raised during testing of TX_BEGIN event: " + e );
		}
		finally
		{
			try
			{
				EventManager.getManager().
					unregisterReActiveEventListener( eventHook, 
						Event.TX_ROLLBACK );
				tx.rollback();
			}
			catch ( Exception e ) { } // Just ignore
			log.setLevel( level );
		}
	}
}

