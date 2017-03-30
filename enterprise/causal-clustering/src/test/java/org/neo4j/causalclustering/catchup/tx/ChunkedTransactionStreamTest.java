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
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class ChunkedTransactionStreamTest
{
    @Test
    public void shouldStreamTransactions() throws Exception
    {
        // given
        StoreId storeId = StoreId.DEFAULT;
        @SuppressWarnings( "unchecked" )
        IOCursor<CommittedTransactionRepresentation> cursor = mock( IOCursor.class );
        ChunkedTransactionStream txStream = new ChunkedTransactionStream( storeId, cursor, mock( CatchupServerProtocol.class ) );
        ByteBufAllocator allocator = mock( ByteBufAllocator.class );

        CommittedTransactionRepresentation tx1 = tx( BASE_TX_ID + 1 );
        CommittedTransactionRepresentation tx2 = tx( BASE_TX_ID + 2 );
        CommittedTransactionRepresentation tx3 = tx( BASE_TX_ID + 3 );
        long lastTxId = BASE_TX_ID + 3;

        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.get() ).thenReturn( tx1, tx2, tx3 );

        // when/then
        assertFalse( txStream.isEndOfInput() );

        assertEquals( ResponseMessageType.TX, txStream.readChunk( allocator ) );
        assertEquals( new TxPullResponse( storeId, tx1 ), txStream.readChunk( allocator ) );
        assertEquals( ResponseMessageType.TX, txStream.readChunk( allocator ) );
        assertEquals( new TxPullResponse( storeId, tx2 ), txStream.readChunk( allocator ) );
        assertEquals( ResponseMessageType.TX, txStream.readChunk( allocator ) );
        assertEquals( new TxPullResponse( storeId, tx3 ), txStream.readChunk( allocator ) );

        assertEquals( ResponseMessageType.TX_STREAM_FINISHED, txStream.readChunk( allocator ) );
        assertEquals( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, lastTxId ), txStream.readChunk( allocator ) );

        assertTrue( txStream.isEndOfInput() );

        // when
        txStream.close();
        // then
        verify( cursor ).close();
    }

    private CommittedTransactionRepresentation tx( long txId )
    {
        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        when( tx.getCommitEntry() ).thenReturn( new OnePhaseCommit( txId, 0 ) );
        return tx;
    }
}
