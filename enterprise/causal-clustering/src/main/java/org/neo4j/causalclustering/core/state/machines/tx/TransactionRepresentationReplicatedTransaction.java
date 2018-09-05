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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.causalclustering.messaging.BoundedNetworkChannel;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.WritableChannel;

public class TransactionRepresentationReplicatedTransaction implements ReplicatedTransaction
{
    private static final int CHUNK_SIZE = 32 * 1024;
    private final TransactionRepresentation tx;

    TransactionRepresentationReplicatedTransaction( TransactionRepresentation tx )
    {
        this.tx = tx;
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return new TxRepresentationMarshal( this );
    }

    @Override
    public void marshal( WritableChannel writableChannel ) throws IOException
    {
        ReplicatedTransactionSerializer.marshal( writableChannel, this );
    }

    @Override
    public TransactionRepresentation extract( TransactionRepresentationExtractor extractor )
    {
        return extractor.extract( this );
    }

    public TransactionRepresentation tx()
    {
        return tx;
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    public class TxRepresentationMarshal implements ChunkedInput<ByteBuf>
    {
        private final ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter;
        private final TransactionRepresentationReplicatedTransaction thisTx;
        private BoundedNetworkChannel channel;
        private Queue<ByteBuf> output = new LinkedList<>();

        private TxRepresentationMarshal( TransactionRepresentationReplicatedTransaction tx )
        {
            txWriter = ReplicatedTransactionFactory.transactionalRepresentationWriter( tx.tx );
            thisTx = tx;
        }

        @Override
        public boolean isEndOfInput()
        {
            return channel != null && channel.closed() && output.isEmpty();
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
                if ( !output.isEmpty() )
                {
                    for ( ByteBuf byteBuf : output )
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
                channel = new BoundedNetworkChannel( allocator, CHUNK_SIZE, output );
                // Add metadata to first chunk
                ReplicatedTransactionSerializer.writeInitialMetaData( channel, thisTx );
            }
            try
            {
                // write to output if empty and there is more to write
                while ( txWriter.canWrite() && output.isEmpty() )
                {
                    txWriter.write( channel );
                }
                // nothing more to write, flush latest chunk and close channel
                if ( output.isEmpty() )
                {
                    channel.prepareForFlush().flush();
                    channel.close();
                }
                return output.poll();
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
