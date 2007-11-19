/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
	private static final TxEventGenerator instance = new TxEventGenerator();
	
	static TxEventGenerator getInstance()
	{
		return instance;
	}

	public void afterCompletion( int param )
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

	public void beforeCompletion()
	{
		// Do nothing
	}
}
