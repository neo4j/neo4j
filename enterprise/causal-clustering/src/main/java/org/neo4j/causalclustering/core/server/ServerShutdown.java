/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.server;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;

public class ServerShutdown
{
    private static final int TIMEOUT = 30;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private final Log log;
    private final EventLoopGroup eventLoopGroup;
    private final Channel channel;

    public ServerShutdown( Log log, EventLoopGroup eventLoopGroup, Channel channel )
    {
        this.log = log;
        this.eventLoopGroup = eventLoopGroup;
        this.channel = channel;
    }

    public void shutdown() throws Throwable
    {
        try ( ErrorHandler errorHandler = new ErrorHandler() )
        {
            if ( channel != null )
            {
                errorHandler.execute( () -> channel.close().syncUninterruptibly() );
            }
            if ( eventLoopGroup != null )
            {
                errorHandler.execute( () ->
                {
                    Future<?> future = eventLoopGroup.shutdownGracefully( 2, TIMEOUT - 10, TIME_UNIT );
                    if ( !future.awaitUninterruptibly( TIMEOUT, TIME_UNIT ) )
                    {
                        log.warn( String.format( "Worker group not shutdown within %s %s.", TIMEOUT, TIME_UNIT ) );
                    }
                    Throwable cause = future.cause();
                    if ( cause != null )
                    {
                        log.error( "Exception when shutting down event loop group", cause );
                        throw cause;
                    }
                } );
            }
        }
    }

    private interface ErrorRunner
    {
        void run() throws Throwable;
    }

    private class ErrorHandler implements AutoCloseable
    {
        private Throwable throwable = null;

        void execute( ErrorRunner errorRunner )
        {
            try
            {
                errorRunner.run();
            }
            catch ( Throwable t )
            {
                if ( throwable == null )
                {
                    throwable = t;
                }
                else
                {
                    throwable.addSuppressed( t );
                }
            }
        }

        @Override
        public void close() throws Exception
        {
            if ( throwable != null )
            {
                throw new RuntimeException( throwable );
            }
        }
    }
}
