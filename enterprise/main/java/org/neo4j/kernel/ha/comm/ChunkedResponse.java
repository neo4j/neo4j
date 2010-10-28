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

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.TransactionStream;

public class ChunkedResponse extends ChunkedData
{
    private static final int CHUNK_SIZE = 1 << 15;
    private final Iterator<Pair<String, TransactionStream>> stream;
    private DataWriter response;
    private String resource;
    private Iterator<Pair<Long, ReadableByteChannel>> tx;
    private ReadableByteChannel current;
    private final ByteBuffer bytes = ByteBuffer.allocate( CHUNK_SIZE );

    public ChunkedResponse( Response<DataWriter> result )
    {
        this.response = result.response();
        this.stream = result.transactions().getStreams().iterator();
    }

    @Override
    protected ChannelBuffer writeNextChunk()
    {
        if ( true ) throw new Error( "implementation not done!" );
        // FIXME: this is not done!
        if ( response == null ) return null;
        ChannelBuffer buffer = ChannelBuffers.buffer( CHUNK_SIZE );
        while ( tx == null || !tx.hasNext() )
        {
            if ( stream == null || !stream.hasNext() )
            {
                response.write( buffer );
                response = null;
                return buffer;
            }
            else
            {
                Pair<String, TransactionStream> next = stream.next();
                resource = next.first();
                tx = next.other().getChannels().iterator();
            }
        }
        Pair<Long, ReadableByteChannel> next = tx.next();
        CommunicationUtils.writeString( resource, buffer, false );
        current = next.other();
        return buffer;
    }

    @Override
    public void close()
    {
        response = null;
        tx = null;
    }
}
