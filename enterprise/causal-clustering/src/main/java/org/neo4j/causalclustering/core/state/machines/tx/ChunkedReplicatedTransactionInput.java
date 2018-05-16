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
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentChunk;
import org.neo4j.causalclustering.messaging.marshalling.v2.SerializableContent;
import org.neo4j.storageengine.api.WritableChannel;

public class ChunkedReplicatedTransactionInput implements SerializableContent, ChunkedInput<ReplicatedContentChunk>
{

    private final ChunkedStream chunkedStream;
    private final byte txContentType;
    private final ReplicatedTransaction replicatedTransaction;

    public ChunkedReplicatedTransactionInput( byte txContentType, ReplicatedTransaction replicatedTransaction, int chunkSize )
    {
        if ( chunkSize < 4 )
        {
            throw new IllegalArgumentException( "Chunk size must be at least 4 bytes" );
        }
        this.txContentType = txContentType;
        this.replicatedTransaction = replicatedTransaction;
        chunkedStream = new ChunkedStream( new ByteArrayInputStream( replicatedTransaction.getTxBytes() ), chunkSize - 3 );
    }

    public ChunkedReplicatedTransactionInput( byte txContentType, ReplicatedTransaction replicatedTransaction )
    {
        this.txContentType = txContentType;
        this.replicatedTransaction = replicatedTransaction;
        chunkedStream = new ChunkedStream( new ByteArrayInputStream( replicatedTransaction.getTxBytes() ) );
    }

    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( txContentType );
        ReplicatedTransactionSerializer.marshal( replicatedTransaction, channel );
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return chunkedStream.isEndOfInput();
    }

    @Override
    public void close() throws Exception
    {
        chunkedStream.close();
    }

    @Override
    public ReplicatedContentChunk readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ReplicatedContentChunk readChunk( ByteBufAllocator allocator ) throws Exception
    {
        boolean isFirst = progress() == 0;
        ByteBuf byteBuf = chunkedStream.readChunk( allocator );
        if ( byteBuf == null )
        {
            byteBuf = new EmptyByteBuf( allocator );
        }
        return new ReplicatedContentChunk( txContentType, isFirst, isEndOfInput(), byteBuf );
    }

    @Override
    public long length()
    {
        return -1;
    }

    @Override
    public long progress()
    {
        return chunkedStream.progress();
    }
}
