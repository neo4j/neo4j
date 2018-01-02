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
package org.neo4j.ha.correctness;

import java.net.URI;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Pair;

class ProverTimeouts extends Timeouts
{
    private final Map<Object, Pair<ProverTimeout, Long>> timeouts;
    private final URI to;
    private long time = 0;

    public ProverTimeouts( URI to )
    {
        super(new FixedTimeoutStrategy(1));
        this.to = to;
        timeouts = new LinkedHashMap<>();
    }

    private ProverTimeouts( URI to, Map<Object, Pair<ProverTimeout, Long>> timeouts )
    {
        super( new FixedTimeoutStrategy( 0 ) );
        this.to = to;
        this.timeouts = new LinkedHashMap<>(timeouts);
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {

    }

    @Override
    public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
    {
        // Should we add support for using timeout strategies to order the timeouts here?
        long timeout = time++;

        timeouts.put( key, Pair.of( new ProverTimeout( timeout, timeoutMessage
                .setHeader( Message.TO, to.toASCIIString() )
                .setHeader( Message.FROM, to.toASCIIString() ) ), timeout ) );
    }

    @Override
    public Message<? extends MessageType> cancelTimeout( Object key )
    {
        Pair<ProverTimeout,Long> timeout = timeouts.remove( key );
        if ( timeout != null )
        {
            return timeout.first().getTimeoutMessage();
        }
        return null;
    }

    @Override
    public void cancelAllTimeouts()
    {
        timeouts.clear();
    }

    @Override
    public Map<Object, Timeout> getTimeouts()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tick( long time )
    {
        // Don't pass this on to the parent class.
    }

    public ProverTimeouts snapshot()
    {
        return new ProverTimeouts(to, timeouts);
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ProverTimeouts that = (ProverTimeouts) o;

        Iterator<Pair<ProverTimeout,Long>> those = that.timeouts.values().iterator();
        Iterator<Pair<ProverTimeout,Long>> mine  = timeouts.values().iterator();

        while(mine.hasNext())
        {
            if(!those.hasNext() || !those.next().first().equals( mine.next().first() ))
            {
                return false;
            }
        }

        if(those.hasNext())
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return timeouts.hashCode();
    }

    public boolean hasTimeouts()
    {
        return timeouts.size() > 0;
    }

    public ClusterAction pop()
    {
        Map.Entry<Object, Pair<ProverTimeout, Long>> lowestTimeout = nextTimeout();
        timeouts.remove( lowestTimeout.getKey() );
        return new MessageDeliveryAction( lowestTimeout.getValue().first().getTimeoutMessage() );
    }

    public ClusterAction peek()
    {
        Map.Entry<Object, Pair<ProverTimeout, Long>> next = nextTimeout();
        if(next != null)
        {
            return new MessageDeliveryAction( next.getValue().first().getTimeoutMessage() );
        }
        else
        {
            return null;
        }
    }

    private Map.Entry<Object, Pair<ProverTimeout, Long>> nextTimeout()
    {
        Map.Entry<Object, Pair<ProverTimeout, Long>> lowestTimeout = null;
        for ( Map.Entry<Object, Pair<ProverTimeout, Long>> current : timeouts.entrySet() )
        {
            if(lowestTimeout == null || lowestTimeout.getValue().other() > current.getValue().other())
            {
                lowestTimeout = current;
            }
        }
        return lowestTimeout;
    }

    class ProverTimeout extends Timeout
    {
        public ProverTimeout( long timeout, Message<? extends MessageType> timeoutMessage )
        {
            super( timeout, timeoutMessage );
        }

        @Override
        public int hashCode()
        {
            return getTimeoutMessage().hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if(obj.getClass() != getClass())
            {
                return false;
            }

            ProverTimeout that = (ProverTimeout)obj;

            return getTimeoutMessage().toString().equals( that.getTimeoutMessage().toString() );
        }
    }
}
