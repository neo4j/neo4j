/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store.id;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class RenewableBatchIdSequenceTest
{
    public static final int BATCH_SIZE = 5;

    private final IdSource idSource = new IdSource();
    private final List<Long> excessIds = new ArrayList<>();
    private final RenewableBatchIdSequence ids = new RenewableBatchIdSequence( idSource, BATCH_SIZE, excessIds::add );

    @Test
    public void shouldRequestIdBatchFromSourceOnFirstCall()
    {
        // given
        assertEquals( 0, idSource.calls );

        // when/then
        assertEquals( 0, ids.nextId() );
        assertEquals( 1, idSource.calls );
        for ( int i = 1; i < BATCH_SIZE; i++ )
        {
            assertEquals( i, ids.nextId() );
            assertEquals( 1, idSource.calls );
        }
    }

    @Test
    public void shouldRequestIdBatchFromSourceOnDepletingCurrent()
    {
        // given
        assertEquals( 0, idSource.calls );
        for ( int i = 0; i < BATCH_SIZE; i++ )
        {
            assertEquals( i, ids.nextId() );
        }
        assertEquals( 1, idSource.calls );

        // when
        long firstIdOfNextBatch = ids.nextId();

        // then
        assertEquals( BATCH_SIZE, firstIdOfNextBatch );
        assertEquals( 2, idSource.calls );
    }

    @Test
    public void shouldGiveBackExcessIdsOnClose()
    {
        // given
        for ( int i = 0; i < BATCH_SIZE / 2; i++ )
        {
            ids.nextId();
        }

        // when
        ids.close();

        // then
        assertEquals( BATCH_SIZE - BATCH_SIZE / 2, excessIds.size() );
        for ( long i = BATCH_SIZE / 2; i < BATCH_SIZE; i++ )
        {
            assertTrue( excessIds.contains( i ) );
        }
    }

    @Test
    public void shouldHandleCloseWithNoCurrentBatch()
    {
        // when
        ids.close();

        // then
        assertTrue( excessIds.isEmpty() );
    }

    @Test
    public void shouldOnlyCloseOnce()
    {
        // given
        for ( int i = 0; i < BATCH_SIZE / 2; i++ )
        {
            ids.nextId();
        }

        // when
        ids.close();

        // then
        for ( long i = BATCH_SIZE / 2; i < BATCH_SIZE; i++ )
        {
            assertTrue( excessIds.remove( i ) );
        }

        // and when closing one more time
        ids.close();

        // then
        assertTrue( excessIds.isEmpty() );
    }

    @Test
    public void shouldContinueThroughEmptyIdBatch()
    {
        // given
        IdSequence idSource = mock( IdSequence.class );
        Iterator<IdRange> ranges = asList(
                new IdRange( EMPTY_LONG_ARRAY, 0, BATCH_SIZE ),
                new IdRange( EMPTY_LONG_ARRAY, BATCH_SIZE, 0 ),
                new IdRange( EMPTY_LONG_ARRAY, BATCH_SIZE, BATCH_SIZE ) ).iterator();
        when( idSource.nextIdBatch( anyInt() ) ).thenAnswer( invocation -> ranges.next() );
        RenewableBatchIdSequence ids = new RenewableBatchIdSequence( idSource, BATCH_SIZE, excessIds::add );

        // when/then
        for ( long expectedId = 0; expectedId < BATCH_SIZE * 2; expectedId++ )
        {
            assertEquals( expectedId, ids.nextId() );
        }
    }

    private static class IdSource implements IdSequence
    {
        int calls;
        long nextId;

        @Override
        public IdRange nextIdBatch( int batchSize )
        {
            calls++;
            try
            {
                return new IdRange( EMPTY_LONG_ARRAY, nextId, batchSize );
            }
            finally
            {
                nextId += batchSize;
            }
        }

        @Override
        public long nextId()
        {
            throw new UnsupportedOperationException( "Should not be used" );
        }
    }
}
