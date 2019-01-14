/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import io.netty.channel.Channel;

import java.net.SocketAddress;

import org.neo4j.bolt.logging.BoltMessageLogger;

/**
 * A channel through which Bolt messaging can occur.
 */
public class BoltChannel implements AutoCloseable, BoltConnectionDescriptor
{
    private final String connector;
    private final Channel rawChannel;
    private final BoltMessageLogger messageLogger;

    public static BoltChannel open( String connector, Channel rawChannel,
                                    BoltMessageLogger messageLogger )
    {
        return new BoltChannel( connector, rawChannel, messageLogger );
    }

    private BoltChannel( String connector, Channel rawChannel,
                         BoltMessageLogger messageLogger )
    {
        this.connector = connector;
        this.rawChannel = rawChannel;
        this.messageLogger = messageLogger;
        messageLogger.serverEvent( "OPEN" );
    }

    public Channel rawChannel()
    {
        return rawChannel;
    }

    public BoltMessageLogger log()
    {
        return messageLogger;
    }

    @Override
    public void close()
    {
        Channel rawChannel = rawChannel();
        if ( rawChannel.isOpen() )
        {
            messageLogger.serverEvent( "CLOSE" );
            rawChannel.close().syncUninterruptibly();
        }
    }

    @Override
    public String id()
    {
        return rawChannel().id().asLongText();
    }

    @Override
    public String connector()
    {
        return connector;
    }

    @Override
    public SocketAddress clientAddress()
    {
        return rawChannel.remoteAddress();
    }

    @Override
    public SocketAddress serverAddress()
    {
        return rawChannel.localAddress();
    }

}
