/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.causalclustering.messaging.BoundedNetworkChannel;
import org.neo4j.causalclustering.messaging.marshalling.ByteArrayChunkedEncoder;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory.transactionalRepresentationWriter;

public class ReplicatedTransactionSerializer
{
    private ReplicatedTransactionSerializer()
    {
    }

    public static ReplicatedTransaction unmarshal( ByteBuf byteBuf )
    {
        int length = byteBuf.readInt();
        if ( length == -1 )
        {
            length = byteBuf.readableBytes();
        }
        byte[] bytes = new byte[length];
        byteBuf.readBytes( bytes );
        return ReplicatedTransaction.from( bytes );
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException
    {
        int txBytesLength = channel.getInt();
        byte[] txBytes = new byte[txBytesLength];
        channel.get( txBytes, txBytesLength );
        return ReplicatedTransaction.from( txBytes );
    }

    public static void marshal( WritableChannel writableChannel, ByteArrayReplicatedTransaction replicatedTransaction ) throws IOException
    {
        int length = replicatedTransaction.getTxBytes().length;
        writableChannel.putInt( length );
        writableChannel.put( replicatedTransaction.getTxBytes(), length );
    }

    public static void marshal( WritableChannel writableChannel, TransactionRepresentationReplicatedTransaction replicatedTransaction ) throws IOException
    {
       /*
        Unknown length. This method will never be used in production. When a ReplicatedTransaction is serialized it has already passed over the network
        and a more efficient marshalling is used in ByteArrayReplicatedTransaction.
        */
        ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter = transactionalRepresentationWriter( replicatedTransaction.tx() );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( 1024 );
        OutputStreamWritableChannel outputStreamWritableChannel = new OutputStreamWritableChannel( outputStream );
        while ( txWriter.canWrite() )
        {
            txWriter.write( outputStreamWritableChannel );
        }
        int length = outputStream.size();
        writableChannel.putInt( length );
        writableChannel.put( outputStream.toByteArray(), length );
    }

    public static ChunkedInput<ByteBuf> encode( TransactionRepresentationReplicatedTransaction representationReplicatedTransaction )
    {
        return new TxRepresentationMarshal( representationReplicatedTransaction.tx() );
    }

    public static ChunkedInput<ByteBuf> encode( ByteArrayReplicatedTransaction byteArrayReplicatedTransaction )
    {
        return new ByteArrayChunkedEncoder( byteArrayReplicatedTransaction.getTxBytes() );
    }

    private static class TxRepresentationMarshal implements ChunkedInput<ByteBuf>
    {
        private static final int CHUNK_SIZE = 32 * 1024;
        private final ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter;
        private BoundedNetworkChannel channel;
        private Queue<ByteBuf> chunks = new LinkedList<>();

        private TxRepresentationMarshal( TransactionRepresentation replicatedTransaction )
        {
            txWriter = ReplicatedTransactionFactory.transactionalRepresentationWriter( replicatedTransaction );
        }

        @Override
        public boolean isEndOfInput()
        {
            return channel != null && channel.closed() && chunks.isEmpty();
        }

        @Override
        public void close()
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Closing TxRepresentationMarshal" ) )
            {
                if ( channel != null )
                {
                    try
                    {
                        channel.close();
                    }
                    catch ( Throwable t )
                    {
                        errorHandler.add( t );
                    }
                }
                if ( !chunks.isEmpty() )
                {
                    for ( ByteBuf byteBuf : chunks )
                    {
                        try
                        {
                            ReferenceCountUtil.release( byteBuf );
                        }
                        catch ( Throwable t )
                        {
                            errorHandler.add( t );
                        }
                    }
                }
            }
        }

        @Override
        public ByteBuf readChunk( ChannelHandlerContext ctx ) throws Exception
        {
            return readChunk( ctx.alloc() );
        }

        @Override
        public ByteBuf readChunk( ByteBufAllocator allocator ) throws Exception
        {
            if ( isEndOfInput() )
            {
                return null;
            }
            if ( channel == null )
            {
                // Ensure that the written buffers does not overflow the allocators chunk size.
                channel = new BoundedNetworkChannel( allocator, CHUNK_SIZE, chunks );
                /*
                Unknown length. The reason for sending this int is to avoid conflicts with Raft V1.
                This way, the serialized result of this object is identical to a serialized byte array. Which is the only type in Raft V1.
                */
                channel.putInt( -1 );
            }
            try
            {
                // write to chunks if empty and there is more to write
                while ( txWriter.canWrite() && chunks.isEmpty() )
                {
                    txWriter.write( channel );
                }
                // nothing more to write, close the channel to get the potential last buffer
                if ( chunks.isEmpty() )
                {
                    channel.close();
                }
                return chunks.poll();
            }
            catch ( Throwable t )
            {
                try
                {
                    close();
                }
                catch ( Exception e )
                {
                    t.addSuppressed( e );
                }
                throw t;
            }
        }

        @Override
        public long length()
        {
            return -1;
        }

        @Override
        public long progress()
        {
            return 0;
        }
    }
}
