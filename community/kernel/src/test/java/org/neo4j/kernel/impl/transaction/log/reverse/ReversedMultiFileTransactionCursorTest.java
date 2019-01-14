/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.reverse;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.given;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.start;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class ReversedMultiFileTransactionCursorTest
{
    @Test
    public void shouldReadSingleVersionReversed() throws Exception
    {
        // GIVEN
        TransactionCursor cursor = new ReversedMultiFileTransactionCursor( log( 5 ), 0, start( 0 ) );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertTransactionRange( reversed, 5, 0 );
    }

    @Test
    public void shouldReadMultipleVersionsReversed() throws Exception
    {
        // GIVEN
        TransactionCursor cursor = new ReversedMultiFileTransactionCursor( log( 5, 3, 8 ), 2, start( 0 ) );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertTransactionRange( reversed, 5 + 3 + 8, 0 );
    }

    @Test
    public void shouldRespectStartLogPosition() throws Exception
    {
        // GIVEN
        TransactionCursor cursor = new ReversedMultiFileTransactionCursor( log( 5, 6, 8 ), 2, new LogPosition( 1, LOG_HEADER_SIZE + 3 ) );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertTransactionRange( reversed, 5 + 6 + 8, 5 + 3 );
    }

    @Test
    public void shouldHandleEmptyLogsMidStream() throws Exception
    {
        // GIVEN
        TransactionCursor cursor = new ReversedMultiFileTransactionCursor( log( 5, 0, 2, 0, 3 ), 4, start( 0 ) );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertTransactionRange( reversed, 5 + 2 + 3, 0 );
    }

    @Test
    public void shouldHandleEmptySingleLogVersion() throws Exception
    {
        // GIVEN
        TransactionCursor cursor = new ReversedMultiFileTransactionCursor( log( 0 ), 0, start( 0 ) );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertTransactionRange( reversed, 0, 0 );
    }

    private void assertTransactionRange( CommittedTransactionRepresentation[] reversed, long highTxId, long lowTxId )
    {
        long expectedTxId = highTxId;
        for ( CommittedTransactionRepresentation transaction : reversed )
        {
            expectedTxId--;
            assertEquals( expectedTxId, transaction.getCommitEntry().getTxId() );
        }
        assertEquals( lowTxId, expectedTxId );
    }

    private ThrowingFunction<LogPosition,TransactionCursor,IOException> log( int... transactionCounts ) throws IOException
    {
        long baseOffset = LogPosition.start( 0 ).getByteOffset();

        @SuppressWarnings( "unchecked" )
        ThrowingFunction<LogPosition,TransactionCursor,IOException> result = mock( ThrowingFunction.class );
        AtomicLong txId = new AtomicLong( 0 );
        CommittedTransactionRepresentation[][] logs = new CommittedTransactionRepresentation[transactionCounts.length][];
        for ( int logVersion = 0; logVersion < transactionCounts.length; logVersion++ )
        {
            logs[logVersion] = transactions( transactionCounts[logVersion], txId );
        }

        when( result.apply( any( LogPosition.class ) ) ).thenAnswer( invocation ->
        {
            LogPosition position = invocation.getArgument( 0 );
            if ( position == null )
            {
                // A mockito issue when calling the "when" methods, I believe
                return null;
            }

            // For simplicity the offset means, in this test, the array offset
            CommittedTransactionRepresentation[] transactions = logs[toIntExact( position.getLogVersion() )];
            CommittedTransactionRepresentation[] subset =
                    copyOfRange( transactions, toIntExact( position.getByteOffset() - baseOffset ), transactions.length );
            ArrayUtil.reverse( subset );
            return given( subset );
        } );
        return result;
    }

    private CommittedTransactionRepresentation[] transactions( int count, AtomicLong txId )
    {
        CommittedTransactionRepresentation[] result = new CommittedTransactionRepresentation[count];
        for ( int i = 0; i < count; i++ )
        {
            CommittedTransactionRepresentation transaction = result[i] = mock( CommittedTransactionRepresentation.class );
            LogEntryCommit commitEntry = mock( LogEntryCommit.class );
            when( commitEntry.getTxId() ).thenReturn( txId.getAndIncrement() );
            when( transaction.getCommitEntry() ).thenReturn( commitEntry );
        }
        return result;
    }
}
