/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cluster.timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Timeout management for state machines. First call setTimeout to setup a timeout.
 * Then either the timeout will trigger or cancelTimeout will have been called with
 * the key used to create the timeout.
 */
public class Timeouts implements MessageSource
{
    private long now = 0;

    private MessageProcessor receiver;
    private TimeoutStrategy timeoutStrategy;

    private Map<Object, Timeout> timeouts = new HashMap<Object, Timeout>();
    private List<Map.Entry<Object, Timeout>> triggeredTimeouts = new ArrayList<Map.Entry<Object, Timeout>>();

    public Timeouts( TimeoutStrategy timeoutStrategy )
    {
        this.timeoutStrategy = timeoutStrategy;
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {
        if(receiver != null)
        {
            throw new UnsupportedOperationException( "Timeouts does not yet support multiple message processors" );
        }
        receiver = messageProcessor;
    }

    /**
     * Add a new timeout to the list
     * If this is not cancelled it will trigger a message on the message processor
     *
     * @param key
     * @param timeoutMessage
     */
    public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
    {
        long timeoutAt = now + timeoutStrategy.timeoutFor( timeoutMessage );
        timeouts.put( key, new Timeout( timeoutAt, timeoutMessage ) );
    }

    public long getTimeoutFor( Message<? extends MessageType> timeoutMessage )
    {
        return timeoutStrategy.timeoutFor( timeoutMessage );
    }

    /**
     * Cancel a timeout corresponding to a particular key. Use the same key
     * that was used to set it up.
     *
     * @param key
     */
    public Message<? extends MessageType> cancelTimeout( Object key )
    {
        Timeout timeout = timeouts.remove( key );
        if ( timeout != null )
        {
            timeoutStrategy.timeoutCancelled( timeout.timeoutMessage );
            return timeout.getTimeoutMessage();
        }
        return null;
    }

    /**
     * Cancel all current timeouts. This is typically used when shutting down.
     */
    public void cancelAllTimeouts()
    {
        for ( Timeout timeout : timeouts.values() )
        {
            timeoutStrategy.timeoutCancelled( timeout.getTimeoutMessage() );
        }
        timeouts.clear();
    }

    public Map<Object, Timeout> getTimeouts()
    {
        return timeouts;
    }

    public Message<? extends MessageType> getTimeoutMessage( String timeoutName )
    {
        Timeout timeout = timeouts.get( timeoutName );
        if ( timeout != null )
        {
            return timeout.getTimeoutMessage();
        }
        else
        {
            return null;
        }
    }

    public void tick( long time )
    {
        synchronized ( this )
        {
            // Time has passed
            now = time;

            timeoutStrategy.tick( now );

            // Check if any timeouts needs to be triggered
            triggeredTimeouts.clear();
            for ( Map.Entry<Object, Timeout> timeout : timeouts.entrySet() )
            {
                if ( timeout.getValue().checkTimeout( now ) )
                {
                    triggeredTimeouts.add( timeout );
                }
            }

            // Remove all timeouts that were triggered
            for ( Map.Entry<Object, Timeout> triggeredTimeout : triggeredTimeouts )
            {
                timeouts.remove( triggeredTimeout.getKey() );
            }
        }

        // Trigger timeouts
        // This needs to be done outside of the synchronized block as it will trigger a message
        // which will cause the statemachine to synchronize on Timeouts
        for ( Map.Entry<Object, Timeout> triggeredTimeout : triggeredTimeouts )
        {
            triggeredTimeout.getValue().trigger( receiver );
        }
    }

    public class Timeout
    {
        private long timeout;
        private Message<? extends MessageType> timeoutMessage;

        public Timeout( long timeout, Message<? extends MessageType> timeoutMessage )
        {
            this.timeout = timeout;
            this.timeoutMessage = timeoutMessage;
        }

        public Message<? extends MessageType> getTimeoutMessage()
        {
            return timeoutMessage;
        }

        public boolean checkTimeout( long now )
        {
            if ( now >= timeout )
            {
                timeoutStrategy.timeoutTriggered( timeoutMessage );

                return true;
            }
            else
            {
                return false;
            }
        }

        public void trigger( MessageProcessor receiver )
        {
            receiver.process( timeoutMessage );
        }

        @Override
        public String toString()
        {
            return timeout + ": " + timeoutMessage;
        }
    }
}
