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
package org.neo4j.causalclustering.messaging;

import java.util.concurrent.Future;

import org.neo4j.logging.Log;

public class SimpleNettyChannel implements Channel
{
    private final Log log;
    private final io.netty.channel.Channel channel;
    private volatile boolean disposed;

    public SimpleNettyChannel( io.netty.channel.Channel channel, Log log )
    {
        this.channel = channel;
        this.log = log;
    }

    @Override
    public boolean isDisposed()
    {
        return disposed;
    }

    @Override
    public synchronized void dispose()
    {
        log.info( "Disposing channel: " + channel );
        disposed = true;
        channel.close();
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public Future<Void> write( Object msg )
    {
        checkDisposed();
        return channel.write( msg );
    }

    @Override
    public Future<Void> writeAndFlush( Object msg )
    {
        checkDisposed();
        return channel.writeAndFlush( msg );
    }

    private void checkDisposed()
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }
    }
}
