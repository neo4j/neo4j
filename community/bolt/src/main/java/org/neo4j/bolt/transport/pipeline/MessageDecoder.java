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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.ByteBufInput;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static io.netty.buffer.ByteBufUtil.hexDump;

public class MessageDecoder extends SimpleChannelInboundHandler<ByteBuf>
{
    private final ByteBufInput input;
    private final BoltRequestMessageReader reader;
    private final BoltRequestMessageHandler messageHandler;
    private final Log log;

    public MessageDecoder( Neo4jPack pack, BoltRequestMessageHandler messageHandler, LogService logService )
    {
        this.input = new ByteBufInput();
        this.reader = new BoltRequestMessageReader( pack.newUnpacker( input ) );
        this.messageHandler = messageHandler;
        this.log = logService.getInternalLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf ) throws Exception
    {
        input.start( byteBuf );
        byteBuf.markReaderIndex();
        try
        {
            reader.read( messageHandler );
        }
        catch ( BoltIOException ex )
        {
            if ( ex.causesFailureMessage() )
            {
                messageHandler.onExternalError( Neo4jError.from( ex ) );
            }
            else
            {
                logMessageOnError( byteBuf );
                throw ex;
            }
        }
        catch ( Throwable error )
        {
            logMessageOnError( byteBuf );
            throw error;
        }
        finally
        {
            input.stop();
        }
    }

    private void logMessageOnError( ByteBuf byteBuf )
    {
        // move reader index back to the beginning of the message in order to log its full content
        byteBuf.resetReaderIndex();
        log.error( "Failed to read an inbound message:\n" + hexDump( byteBuf ) + '\n' );
    }
}
