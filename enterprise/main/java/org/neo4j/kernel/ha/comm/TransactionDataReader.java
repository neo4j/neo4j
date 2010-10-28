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

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.ha.TransactionStream;

abstract class TransactionDataReader
{
    private static final int TXID_SIZE = 8, DATASIZE_SIZE = 2;
    static final int HEADER_SIZE = 2;

    public static Single tryInitStream( int nameSize, ChannelBuffer buffer )
    {
        if ( buffer.readableBytes() < nameSize + HEADER_SIZE + TXID_SIZE + DATASIZE_SIZE )
        {
            return null;
        }
        buffer.skipBytes( HEADER_SIZE );
        String resource = CommunicationUtils.readString( buffer, nameSize / 2 );
        long txId = buffer.readLong();
        int datasize = buffer.readInt();
        return new Single( resource, txId, datasize );
    }

    /* Sent from client to server */
    public static final class Multiple extends TransactionDataReader
    {
        private final SlaveContext context;
        private int txLeft;
        private Single data = null;
        private final TransactionEntry[] transactions;

        public Multiple( SlaveContext context, int numTransactions )
        {
            this.context = context;
            this.txLeft = numTransactions;
            this.transactions = new TransactionEntry[numTransactions];
        }

        @Override
        MasterInvoker read( ChannelBuffer buffer )
        {
            if ( data != null )
            {
                TransactionEntry entry = data.read( buffer );
                if ( entry != null )
                {
                    data = null;
                    return addEntry( entry );
                }
            }
            else if ( buffer.readableBytes() >= HEADER_SIZE )
            {
                data = tryInitStream( buffer.getUnsignedShort( buffer.readerIndex() ), buffer );
            }
            return null;
        }

        private MasterInvoker addEntry( TransactionEntry entry )
        {
            transactions[--txLeft] = entry;
            if ( txLeft == 0 )
            {
                return new MultiTransactionEntry( context, transactions );
            }
            else
            {
                return null;
            }
        }
    }

    /* Sent in a stream from server to client */
    public static final class Single extends TransactionDataReader
    {
        private final String resource;
        private final long txId;
        private int leftInChunk;
        private boolean moreChunks;
        private byte[] current;
        private final Collection<byte[]> data;

        Single( String resource, long txId, int chunkSize )
        {
            this.resource = resource;
            this.txId = txId;
            if ( chunkSize < 0 )
            {
                moreChunks = true;
                chunkSize = -chunkSize;
            }
            else
            {
                moreChunks = false;
            }
            leftInChunk = chunkSize;
            current = new byte[chunkSize];
            data = new ArrayList<byte[]>();
        }

        @Override
        TransactionEntry read( ChannelBuffer buffer )
        {
            if ( current == null )
            {
                if ( leftInChunk != 0 )
                {
                    current = new byte[leftInChunk];
                }
                else if ( moreChunks )
                {
                    if ( buffer.readableBytes() >= DATASIZE_SIZE )
                    {
                        int chunkSize = buffer.readShort();
                        if ( chunkSize < 0 )
                        {
                            moreChunks = true;
                            chunkSize = -chunkSize;
                        }
                        else
                        {
                            moreChunks = false;
                        }
                        leftInChunk = chunkSize;
                        if ( chunkSize == 0 )
                        {
                            return done();
                        }
                        current = new byte[leftInChunk];
                        data.add( current );
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return done();
                }
            }
            int toRead = Math.min( buffer.readableBytes(), leftInChunk );
            buffer.readBytes( current, current.length - leftInChunk, toRead );
            if ( leftInChunk == 0 )
            {
                current = null;
                if ( !moreChunks )
                {
                    return done();
                }
            }
            return null;
        }

        private TransactionEntry done()
        {
            return new TransactionEntry( resource, txId,//
                    new ByteArrayIteratorChannel( data.iterator() ) );
        }
    }

    private TransactionDataReader()
    {
    }

    abstract Object read( ChannelBuffer buffer );

    private static class MultiTransactionEntry implements MasterInvoker
    {
        private final SlaveContext context;
        private final TransactionEntry[] transactions;

        MultiTransactionEntry( SlaveContext context, TransactionEntry[] transactions )
        {
            this.context = context;
            this.transactions = transactions;
        }

        public Response<DataWriter> invoke( Master master )
        {
            for ( int i = transactions.length - 1; i >= 0; --i )
            {
                applyTransaction( master, transactions[i] );
            }
            return null; // FIXME: return data!
        }

        @SuppressWarnings( { "unchecked", "boxing" } )
        private Response<Long> applyTransaction( Master master, TransactionEntry transaction )
        {
            return master.commitSingleResourceTransaction( context, transaction.resource,
                    new TransactionStream( Arrays.asList( new Pair<Long, ReadableByteChannel>(
                            transaction.txId, transaction.data ) ) ) );
        }
    }
}
