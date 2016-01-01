/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.tools.boltalyzer;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.bolt.v1.transport.BoltV1Dechunker;

/**
 * Stateful object tracking an ongoing session, able to decode transport transmissions as they arrive and describe them in a helpful way.
 */
public class AnalyzedSession
{
    private final String name;
    private final long id;
    private final BoltMessageDescriber clientStreamDescriber = new BoltMessageDescriber();
    private final BoltMessageDescriber serverStreamDescriber = new BoltMessageDescriber();

    private final BoltV1Dechunker clientStream = new BoltV1Dechunker( clientStreamDescriber, () -> {} );
    private final BoltV1Dechunker serverStream = new BoltV1Dechunker( serverStreamDescriber, () -> {} );

    private int clientHandshakeRemaining = 16;
    private int serverHandshakeRemaining = 4;

    public AnalyzedSession( String name, long id )
    {
        this.name = name;
        this.id = id;
    }

    public String name()
    {
        return name;
    }

    public String describeServerPayload( ByteBuffer payload ) throws IOException
    {
        ByteBuf data = Unpooled.wrappedBuffer( payload );
        String out = " <EMPTY>";

        // TODO: Something more sophisticated than this
        if(serverHandshakeRemaining > 0 && data.readableBytes() > 0)
        {
            int toRead = Math.min( data.readableBytes(), serverHandshakeRemaining );
            serverHandshakeRemaining -= toRead;
            data.skipBytes( toRead );
            out = " <HANDSHAKE RESPONSE>";
        }

        if( data.readableBytes() == 0 )
        {
            return out;
        }

        serverStream.handle( data );
        return serverStreamDescriber.flushDescription();
    }

    public String describeClientPayload( ByteBuffer payload ) throws IOException
    {
        ByteBuf data = Unpooled.wrappedBuffer( payload );
        String out = " <EMPTY>";

        if(clientHandshakeRemaining > 0 && data.readableBytes() > 0)
        {
            int toRead = Math.min( data.readableBytes(), clientHandshakeRemaining );
            clientHandshakeRemaining -= toRead;
            data.skipBytes( toRead );
            out = " <HANDSHAKE>";
        }

        if( data.readableBytes() == 0 )
        {
            return out;
        }

        clientStream.handle( data );
        return clientStreamDescriber.flushDescription();
    }

    public long id()
    {
        return id;
    }
}