/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.transaction;

import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;

class TxEventGenerator implements Synchronization
{
    private static Logger log = Logger.getLogger( TxEventGenerator.class
        .getName() );

    private final EventManager eventManager;
    private final TransactionImpl tx;

    TxEventGenerator( EventManager eventManager, TransactionImpl tx )
    {
        this.eventManager = eventManager;
        this.tx = tx;
    }

    public void afterCompletion( int param )
    {
        if ( tx == null )
        {
            log.severe( "Unable to get transaction after " + "completion: ["
                + param + "]. No completion" + "event generated." );
            return;
        }
        Integer eventIdentifier = tx.getEventIdentifier();
        switch ( param )
        {
            case Status.STATUS_COMMITTED:
                eventManager.generateReActiveEvent( Event.TX_COMMIT,
                    new EventData( eventIdentifier ) );
                break;
            case Status.STATUS_ROLLEDBACK:
                eventManager.generateReActiveEvent( Event.TX_ROLLBACK,
                    new EventData( eventIdentifier ) );
                break;
            default:
                log.severe( "Unexpected and unknown tx status after "
                    + "completion: [" + param + "]. No completion"
                    + "event generated." );
        }
    }

    public void beforeCompletion()
    {
        // Do nothing
    }
}