/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.bolt.v1.messaging.MessageHandler;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;

public class BoltV1Dechunker
{
    private final ChunkedInput input;
    private final PackStreamMessageFormatV1.Reader unpacker;
    private final MessageHandler<RuntimeException> onMessage;
    private final Runnable onMessageStarted;

    public enum State
    {
        AWAITING_CHUNK,
        IN_CHUNK,
        IN_HEADER,
        CLOSED
    }

    private State state = State.AWAITING_CHUNK;
    private int chunkSize = 0;

    public BoltV1Dechunker( MessageHandler<RuntimeException> messageHandler, Runnable onMessageStarted )
    {
        this.onMessage = messageHandler;
        this.onMessageStarted = onMessageStarted;
        this.input = new ChunkedInput();
        this.unpacker = new PackStreamMessageFormatV1.Reader( new Neo4jPack.Unpacker( input ) );
    }

    /** Check if we are currently "in the middle of" a message, eg. we've gotten parts of it, but are waiting for more. */
    public boolean isInMiddleOfAMessage()
    {
        return chunkSize != 0;
    }

    public void handle( ByteBuf data ) throws IOException
    {
        while ( data.readableBytes() > 0 )
        {
            switch ( state )
            {
            case AWAITING_CHUNK:
            {
                if ( data.readableBytes() >= 2 )
                {
                    // Whole header available, read that
                    chunkSize = data.readUnsignedShort();
                    handleHeader();
                }
                else
                {
                    // Only one byte available, read that and wait for the second byte
                    chunkSize = data.readByte() << 8;
                    state = State.IN_HEADER;
                }
                break;
            }
            case IN_HEADER:
            {
                // First header byte read, now we read the next one
                chunkSize = (chunkSize | data.readByte()) & 0xFFFF;
                handleHeader();
                break;
            }
            case IN_CHUNK:
            {
                if ( chunkSize < data.readableBytes() )
                {
                    // Current packet is larger than current chunk, slice of the chunk
                    input.append( data.readSlice( chunkSize ) );
                    state = State.AWAITING_CHUNK;
                }
                else if ( chunkSize == data.readableBytes() )
                {
                    // Current packet perfectly maps to current chunk
                    input.append( data );
                    state = State.AWAITING_CHUNK;
                    return;
                }
                else
                {
                    // Current packet is smaller than the chunk we're reading, split the current chunk itself up
                    chunkSize -= data.readableBytes();
                    input.append( data );
                    return;
                }
                break;
            }
            case CLOSED:
            {
                // No-op
                return;
            }
            }
        }
    }

    public synchronized void close()
    {
        state = State.CLOSED;
        input.close();
    }

    private void handleHeader() throws IOException
    {
        if(chunkSize == 0)
        {
            // Message boundary
            try
            {
                onMessageStarted.run();
                unpacker.read( onMessage );
            }
            finally
            {
                input.clear();
            }
            state = State.AWAITING_CHUNK;
        }
        else
        {
            state = State.IN_CHUNK;
        }
    }

}
