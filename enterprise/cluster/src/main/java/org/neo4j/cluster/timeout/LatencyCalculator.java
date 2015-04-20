/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate roundtrip latency from this host to all other hosts. This can be used for realistic timeouts.
 */
public class LatencyCalculator
    implements MessageProcessor, TimeoutStrategy
{
    TimeoutStrategy delegate;

    Map<String, Long> conversations = new HashMap<String, Long>(  );

    Map<String, List<Long>> latencies = new HashMap<String, List<Long>>(  );

    Logger logger = LoggerFactory.getLogger(LatencyCalculator.class);

    long now;

    int latencyCount = 5;

    public LatencyCalculator(TimeoutStrategy delegate, MessageSource incoming)
    {
        this.delegate = delegate;

        incoming.addMessageProcessor( new MessageProcessor()
        {
            @Override
            public boolean process( Message<? extends MessageType> message )
            {
                synchronized(LatencyCalculator.this)
                {
                    Long sent = conversations.get( message.getHeader( Message.CONVERSATION_ID ) );
                    if (sent != null)
                    {
                        long received = now;

                        String from = message.getHeader( Message.FROM );
                        List<Long> hostLatencies = latencies.get( from );
                        if (hostLatencies == null)
                        {
                            hostLatencies = new ArrayList<Long>(  );
                            latencies.put( from, hostLatencies );
                        }
                        long latency = received - sent;
                        if (latency < 0)
                            logger.warn( "Negative latency!" );
                        hostLatencies.add( latency );

                        if (hostLatencies.size() == latencyCount)
                        {
                            long latencySum = 0;
                            for( Long hostLatency : hostLatencies )
                            {
                                latencySum += hostLatency;
                            }

                            long latencyAvg = latencySum / latencyCount;

//                            logger.info( from+" roundtrip latency: "+latencyAvg );

                            hostLatencies.clear();
                        }
                    }
                }
                return true;
            }
        } );
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        if ( !message.isInternal() && !message.getHeader( Message.TO ).equals( message.getHeader( Message.CREATED_BY ) ) )
        {
            conversations.put( message.getHeader( Message.CONVERSATION_ID ), now );
        }
        return true;
    }

    @Override
    public long timeoutFor( Message message )
    {
        return delegate.timeoutFor( message );
    }

    @Override
    public void timeoutTriggered( Message timeoutMessage )
    {
        delegate.timeoutTriggered( timeoutMessage );
    }

    @Override
    public void timeoutCancelled( Message timeoutMessage )
    {
        delegate.timeoutCancelled( timeoutMessage );
    }

    public synchronized void tick(long now)
    {
        this.now = now;
        delegate.tick( now );
    }
}
