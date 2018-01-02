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
package org.neo4j.com;

import org.jboss.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Clock;
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
            previous.lastTimeHeardOf = clock.currentTimeMillis();
        }
        else
        {
            connectedChannels.put( channel, new Request( requestContext, clock.currentTimeMillis() ) );
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

        request.lastTimeHeardOf = clock.currentTimeMillis();
        return true;
    }

    @Override
    public synchronized void run()
    {
        for ( Map.Entry<Channel,Request> entry : connectedChannels.entrySet() )
        {
            Channel channel = entry.getKey();
            long age = clock.currentTimeMillis() - entry.getValue().lastTimeHeardOf;
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
