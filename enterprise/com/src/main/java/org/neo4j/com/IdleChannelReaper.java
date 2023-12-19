/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import org.jboss.netty.channel.Channel;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Keeps track of a set of channels and when they were last active.
 * Run this object periodically to close any channels that have been inactive for longer than the threshold.
 * This functionality is required because sometimes Netty doesn't tell us when channels are
 * closed or disconnected. Most of the time it does, but this acts as a safety
 * net for those we don't get notifications for. When the bug is fixed remove this class.
 */
public class IdleChannelReaper implements Runnable
{
    private final Map<Channel,Request> connectedChannels = new HashMap<>();
    private Clock clock;
    private final Log msgLog;
    private ChannelCloser channelCloser;
    private long thresholdMillis;

    public IdleChannelReaper( ChannelCloser channelCloser, LogProvider logProvider, Clock clock, long thresholdMillis )
    {
        this.channelCloser = channelCloser;
        this.clock = clock;
        this.thresholdMillis = thresholdMillis;
        msgLog = logProvider.getLog( getClass() );
    }

    public synchronized void add( Channel channel, RequestContext requestContext )
    {
        Request previous = connectedChannels.get( channel );
        if ( previous != null )
        {
            previous.lastTimeHeardOf = clock.millis();
        }
        else
        {
            connectedChannels.put( channel, new Request( requestContext, clock.millis() ) );
        }
    }

    public synchronized Request remove( Channel channel )
    {
        return connectedChannels.remove( channel );
    }

    public synchronized boolean update( Channel channel )
    {
        Request request = connectedChannels.get( channel );
        if ( request == null )
        {
            return false;
        }

        request.lastTimeHeardOf = clock.millis();
        return true;
    }

    @Override
    public synchronized void run()
    {
        for ( Map.Entry<Channel,Request> entry : connectedChannels.entrySet() )
        {
            Channel channel = entry.getKey();
            long age = clock.millis() - entry.getValue().lastTimeHeardOf;
            if ( age > thresholdMillis )
            {
                msgLog.info( "Found a silent channel " + entry + ", " + age );
                channelCloser.tryToCloseChannel( channel );
            }
            else if ( age > thresholdMillis / 2 )
            {
                if ( !(channel.isOpen() && channel.isConnected() && channel.isBound()) )
                {
                    channelCloser.tryToCloseChannel( channel );
                }
            }
        }
    }

    public static class Request
    {
        private final RequestContext requestContext;

        private long lastTimeHeardOf;

        public Request( RequestContext requestContext, long lastTimeHeardOf )
        {
            this.requestContext = requestContext;
            this.lastTimeHeardOf = lastTimeHeardOf;
        }

        public RequestContext getRequestContext()
        {
            return requestContext;
        }
    }
}
