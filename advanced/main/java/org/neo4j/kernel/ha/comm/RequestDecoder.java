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

public class RequestDecoder extends FrameDecoder
{
    private TransactionDataReader data = null;

    @Override
    protected Object decode( ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer )
            throws Exception
    {
        if ( data != null )
        {
            Object result = data.read( buffer );
            if ( result != null ) data = null; // Done reading transaction data
            return result;
        }
        int pos = buffer.readerIndex();
        Object result = null;
        try
        {
            RequestType requestType = RequestType.get( buffer.readUnsignedByte() );
            result = requestType.readRequest( buffer );
        }
        finally
        {
            if ( result == null ) /*reset reader*/buffer.readerIndex( pos );
        }
        if ( result instanceof TransactionDataReader )
        {
            data = (TransactionDataReader) result;
            return null;
        }
        else
        {
            return result;
        }
    }
}
