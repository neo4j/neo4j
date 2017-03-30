/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;

public class AllNodeProgressionTest
{
    private final int start = 1;
    private final long end = 42L;
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final NodeProgression.Batch batch = new NodeProgression.Batch();

    {
        when( nodeStore.getNumberOfReservedLowIds() ).thenReturn( start );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( end );
        batch.nothing();
    }

    @Test
    public void shouldReturnABatchFromLowReservedIdsToHighIdPossibleInUse() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore, ReadableTransactionState.EMPTY );

        // when
        boolean hasNext = progression.nextBatch( batch );

        // then
        assertTrue( hasNext );
        checkBatch( start, end, progression );

        assertNoMoreValueInBatchAndProgression( progression );
    }

    @Test
    public void shouldCheckIfTheHighIdHasChangedAndIssueAnExtraBatchWithTheRemainingElements() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore, ReadableTransactionState.EMPTY );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( start, end, progression );

        // when / then
        long movedEnd = end + 10;
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( movedEnd );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( end + 1, movedEnd, progression );

        assertNoMoreValueInBatchAndProgression( progression );
    }

    @Test
    public void shouldNeverReturnNewBatchesIfTheProgressionHasReturnFalseToSignalTermination() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore, ReadableTransactionState.EMPTY );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( start, end, progression );

        assertNoMoreValueInBatchAndProgression( progression );

        // when / then
        long movedEnd = end + 10;
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( movedEnd );
        assertNoMoreValueInBatchAndProgression( progression );
    }

    @Test
    public void shouldReturnNoAddedNodesIfNoTransactionStateIsGiven() throws Throwable
    {
        AllNodeProgression progression = new AllNodeProgression( nodeStore, ReadableTransactionState.EMPTY );
        assertFalse( progression.addedNodes().hasNext() );
    }

    @Test
    public void shouldReturnAddedNodesFromTheTxState() throws Throwable
    {
        TxState txState = new TxState();
        long id = 42;
        txState.nodeDoCreate( id );
        AllNodeProgression progression = new AllNodeProgression( nodeStore, txState );
        assertEquals( singletonList( id ), asList( progression.addedNodes() ) );
    }

    @Test
    public void shouldMarkTheDeletedNodesAsNonFetchableFromDisk() throws Throwable
    {
        TxState txState = new TxState();
        for ( long i = start; i <= end; i++ )
        {
            if ( i % 3 == 0 )
            {
                txState.nodeDoDelete( i );
            }
        }

        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore, txState );

        // when
        boolean hasNext = progression.nextBatch( batch );

        // then
        assertTrue( hasNext );

        for ( long i = start; i <= end; i++ )
        {
            assertTrue( batch.hasNext() );
            assertEquals( i, batch.next() );
            assertEquals( i % 3 != 0, progression.fetchFromDisk( i ) );
        }

        assertNoMoreValueInBatchAndProgression( progression );
    }

    private void checkBatch( long start, long end, NodeProgression progression )
    {
        for ( long i = start; i <= end; i++ )
        {
            assertTrue( batch.hasNext() );
            assertEquals( i, batch.next() );
            assertTrue( progression.fetchFromDisk( i ) );
        }
    }

    private void assertNoMoreValueInBatchAndProgression( AllNodeProgression progression )
    {
        assertFalse( batch.hasNext() );
        assertFalse( progression.nextBatch( batch ) );
        assertFalse( batch.hasNext() );
    }
}
