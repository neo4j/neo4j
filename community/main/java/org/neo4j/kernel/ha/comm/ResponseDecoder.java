/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class ResponseDecoder extends FrameDecoder
{
    private static final int HEADER_SIZE = TransactionDataReader.HEADER_SIZE;
    private TransactionDataReader data = null;

    @Override
    protected Object decode( ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer )
            throws Exception
    {
        if ( data != null )
        {
            Object result = data.read( buffer );
            if ( result != null ) data = null; // Done reading this transaction
            return result;
        }
        if ( buffer.readableBytes() >= HEADER_SIZE )
        {
            int header = buffer.getShort( buffer.readerIndex() );
            if ( header < 0 )
            {
                header = -header;
                data = TransactionDataReader.tryInitStream( header, buffer );
            }
            else
            {
                if ( buffer.readableBytes() < HEADER_SIZE + header )
                {
                    return null;
                }
                else
                {
                    ChannelBuffer frame = buffer.factory().getBuffer( header );
                    int index = buffer.readerIndex() + HEADER_SIZE;
                    frame.writeBytes( buffer, index, header );
                    buffer.readerIndex( index + header );
                    return frame;
                }
            }
        }
        return null;
    }
}
