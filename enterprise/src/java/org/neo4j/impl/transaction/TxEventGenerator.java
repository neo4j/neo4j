package org.neo4j.impl.transaction;

import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;


/**
 * A transaction hook that generates an event upon transaction completion.
 * The {@link Event#TX_BEGIN TX_BEGIN} event is generated in
 * {@link UserTransactionImpl#begin} and this class generates
 * {@link Event#TX_COMMIT} or {@link Event#TX_ROLLBACK} depending on
 * whether the transaction is successful.
 */
class TxEventGenerator implements Synchronization
{
	private static Logger log = 
		Logger.getLogger( TxEventGenerator.class.getName() );
	private static TxEventGenerator instance = new TxEventGenerator();
	
	static TxEventGenerator getInstance()
	{
		return instance;
	}

	public void afterCompletion( int param )
	{
		try
		{
			TransactionImpl tx = ( TransactionImpl ) 
				TxManager.getManager().getTransaction();
			if ( tx == null )
			{
				log.severe( "Unable to get transaction after " +
				   "completion: [" + param + "]. No completion" +
				   "event generated." );
				return;
			}
			Integer eventIdentifier = tx.getEventIdentifier();
			switch ( param )
			{
				case Status.STATUS_COMMITTED:
					EventManager.getManager().generateReActiveEvent(
						Event.TX_COMMIT, new EventData( eventIdentifier ) );
					break;
				case Status.STATUS_ROLLEDBACK:
					EventManager.getManager().generateReActiveEvent(
						Event.TX_ROLLBACK, new EventData( eventIdentifier ) );
					break;
				default:
					log.severe( "Unexpected and unknown tx status after " +
							   "completion: [" + param + "]. No completion" +
							   "event generated." );
			}
		}
		catch ( javax.transaction.SystemException e )
		{
			log.severe( "Unable to get transaction after " +
					   "completion: [" + param + "]. No completion" +
					   "event generated." );
		}
	}

	public void beforeCompletion()
	{
		// Do nothing
	}
}
