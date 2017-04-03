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

import org.neo4j.kernel.impl.store.NodeStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllNodeProgressionTest
{
    private final int start = 1;
    private final long end = 42L;
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final Batch batch = new Batch();

    {
        when( nodeStore.getNumberOfReservedLowIds() ).thenReturn( start );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( end );
        batch.nothing();
    }

    @Test
    public void shouldReturnABatchFromLowReservedIdsToHighIdPossibleInUse() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore );

        // when
        boolean hasNext = progression.nextBatch( batch );

        // then
        assertTrue( hasNext );
        checkBatch( start, end );

        assertNoMoreValueInBatchAndProgression( progression );
    }

    @Test
    public void shouldCheckIfTheHighIdHasChangedAndIssueAnExtraBatchWithTheRemainingElements() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( start, end );

        // when / then
        long movedEnd = end + 10;
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( movedEnd );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( end + 1, movedEnd );

        assertNoMoreValueInBatchAndProgression( progression );
    }

    @Test
    public void shouldNeverReturnNewBatchesIfTheProgressionHasReturnFalseToSignalTermination() throws Throwable
    {
        // given
        AllNodeProgression progression = new AllNodeProgression( nodeStore );
        assertTrue( progression.nextBatch( batch ) );
        checkBatch( start, end );

        assertNoMoreValueInBatchAndProgression( progression );

        // when / then
        long movedEnd = end + 10;
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( movedEnd );
        assertNoMoreValueInBatchAndProgression( progression );
    }

    private void checkBatch( long start, long end )
    {
        for ( long i = start; i <= end; i++ )
        {
            assertTrue( batch.hasNext() );
            assertEquals( i, batch.next() );
        }
    }

    private void assertNoMoreValueInBatchAndProgression( AllNodeProgression progression )
    {
        assertFalse( batch.hasNext() );
        assertFalse( progression.nextBatch( batch ) );
        assertFalse( batch.hasNext() );
    }
}
