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
package org.neo4j.coreedge.catchup.storecopy;

import java.io.OutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileReceiver;
import org.neo4j.coreedge.catchup.CatchupClientProtocol;

import static org.neo4j.coreedge.catchup.CatchupClientProtocol.NextMessage;

public class FileContentHandler extends SimpleChannelInboundHandler<ByteBuf>
{
    private final CatchupClientProtocol protocol;
    private long expectedBytes = 0;

    private StoreFileReceiver location;
    private String destination;

    public FileContentHandler( CatchupClientProtocol protocol, StoreFileReceiver location )
    {
        this.protocol = protocol;
        this.location = location;
    }

    public void setExpectedFile( FileHeader fileHeader )
    {
        this.expectedBytes = fileHeader.fileLength();
        this.destination = fileHeader.fileName();
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
    {
        if ( protocol.isExpecting( NextMessage.FILE_CONTENTS ) )
        {
            int bytesInMessage = msg.readableBytes();
            try ( OutputStream outputStream = location.getStoreFileStreams().createStream( destination ) )
            {
                msg.readBytes( outputStream, bytesInMessage );
            }

            expectedBytes -= bytesInMessage;

            if ( expectedBytes <= 0 )
            {
                protocol.expect( NextMessage.MESSAGE_TYPE );
            }
        }
    }
}
