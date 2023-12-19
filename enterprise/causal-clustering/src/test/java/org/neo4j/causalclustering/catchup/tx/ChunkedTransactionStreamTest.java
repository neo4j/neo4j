/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

@SuppressWarnings( {"unchecked", "UnnecessaryLocalVariable"} )
public class ChunkedTransactionStreamTest
{
    private final StoreId storeId = StoreId.DEFAULT;
    private final ByteBufAllocator allocator = mock( ByteBufAllocator.class );
    private final CatchupServerProtocol protocol = mock( CatchupServerProtocol.class );
    private final IOCursor<CommittedTransactionRepresentation> cursor = mock( IOCursor.class );
    private final int baseTxId = (int) BASE_TX_ID;

    @Test
    public void shouldSucceedExactNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 10;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    public void shouldSucceedWithNoTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = baseTxId;
        int txIdPromise = baseTxId;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    public void shouldSucceedExcessiveNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 9;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    public void shouldFailIncompleteStreamOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 11;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, E_TRANSACTION_PRUNED );
    }

    @Test
    public void shouldSucceedLargeNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 1000;
        int txIdPromise = 900;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testTransactionStream( int firstTxId, int lastTxId, int txIdPromise, CatchupResult expectedResult ) throws Exception
    {
        ChunkedTransactionStream txStream = new ChunkedTransactionStream( NullLog.getInstance(), storeId, firstTxId, txIdPromise, cursor, protocol );

        List<Boolean> more = new ArrayList<>();
        List<CommittedTransactionRepresentation> txs = new ArrayList<>();

        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            more.add( true );
            txs.add( tx( txId ) );
        }
        txs.add( null );
        more.add( false );

        when( cursor.next() ).thenAnswer( returnsElementsOf( more ) );
        when( cursor.get() ).thenAnswer( returnsElementsOf( txs ) );

        // when/then
        assertFalse( txStream.isEndOfInput() );

        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            assertEquals( ResponseMessageType.TX, txStream.readChunk( allocator ) );
            assertEquals( new TxPullResponse( storeId, txs.get( txId - firstTxId ) ), txStream.readChunk( allocator ) );
        }

        assertEquals( ResponseMessageType.TX_STREAM_FINISHED, txStream.readChunk( allocator ) );
        assertEquals( new TxStreamFinishedResponse( expectedResult, lastTxId ), txStream.readChunk( allocator ) );

        assertTrue( txStream.isEndOfInput() );

        // when
        txStream.close();

        // then
        verify( cursor ).close();
    }

    private CommittedTransactionRepresentation tx( int txId )
    {
        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        when( tx.getCommitEntry() ).thenReturn( new LogEntryCommit( txId, 0 ) );
        return tx;
    }
}
