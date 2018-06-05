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
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;

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
        ChunkedTransactionStream txStream = new ChunkedTransactionStream( storeId, BASE_TX_ID + 1, cursor, mock( CatchupServerProtocol.class ) );
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
        when( tx.getCommitEntry() ).thenReturn( new LogEntryCommit( txId, 0 ) );
        return tx;
    }
}
