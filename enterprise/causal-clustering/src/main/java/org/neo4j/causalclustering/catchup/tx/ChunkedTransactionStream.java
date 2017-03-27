/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

/**
 * Returns a chunked stream of transactions.
 */
public class ChunkedTransactionStream implements ChunkedInput<Object>
{
    private final StoreId storeId;
    private final IOCursor<CommittedTransactionRepresentation> txCursor;
    private final CatchupServerProtocol protocol;

    private boolean endOfInput;
    private boolean noMoreTransactions;
    private long lastTxId;

    private Object pending;

    ChunkedTransactionStream( StoreId storeId, IOCursor<CommittedTransactionRepresentation> txCursor, CatchupServerProtocol protocol )
    {
        this.storeId = storeId;
        this.txCursor = txCursor;
        this.protocol = protocol;
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return endOfInput;
    }

    @Override
    public void close() throws Exception
    {
        txCursor.close();
    }

    @Override
    public Object readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public Object readChunk( ByteBufAllocator allocator ) throws Exception
    {
        assert !endOfInput;

        if ( pending != null )
        {
            if ( noMoreTransactions )
            {
                endOfInput = true;
            }

            return consumePending();
        }
        else if ( noMoreTransactions )
        {
            /* finalization should always have a last ending message */
            throw new IllegalStateException();
        }
        else if ( txCursor.next() )
        {
            assert pending == null;

            CommittedTransactionRepresentation tx = txCursor.get();
            lastTxId = tx.getCommitEntry().getTxId();
            pending = new TxPullResponse( storeId, tx );
            return ResponseMessageType.TX;
        }
        else
        {
            assert pending == null;

            noMoreTransactions = true;
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );

            pending = new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, lastTxId );
            return ResponseMessageType.TX_STREAM_FINISHED;
        }
    }

    private Object consumePending()
    {
        Object prevPending = pending;
        pending = null;
        return prevPending;
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
