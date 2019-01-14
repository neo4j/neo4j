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
package org.neo4j.kernel.impl.store.id;

import org.junit.Test;

import org.neo4j.kernel.impl.store.id.validation.IdValidator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BatchingIdSequenceTest
{
    @Test
    public void ShouldSkipNullId()
    {
        BatchingIdSequence idSequence = new BatchingIdSequence();

        idSequence.set( IdGeneratorImpl.INTEGER_MINUS_ONE - 1 );
        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE - 1, idSequence.peek() );

        // The 'NULL Id' should be skipped, and never be visible anywhere.
        // Peek should always return what nextId will return

        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE - 1, idSequence.nextId() );
        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idSequence.peek() );
        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idSequence.nextId() );

        // And what if someone were to set it directly to the NULL id
        idSequence.set( IdGeneratorImpl.INTEGER_MINUS_ONE );

        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idSequence.peek() );
        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idSequence.nextId() );
    }

    @Test
    public void resetShouldSetDefault()
    {
        BatchingIdSequence idSequence = new BatchingIdSequence();

        idSequence.set( 99L );

        assertEquals( 99L, idSequence.peek() );
        assertEquals( 99L, idSequence.nextId() );
        assertEquals( 100L, idSequence.peek() );

        idSequence.reset();

        assertEquals( 0L, idSequence.peek() );
        assertEquals( 0L, idSequence.nextId() );
        assertEquals( 1L, idSequence.peek() );
    }

    @Test
    public void shouldSkipReservedIdWhenGettingBatches()
    {
        // GIVEN
        int batchSize = 10;
        BatchingIdSequence idSequence = new BatchingIdSequence(
                IdGeneratorImpl.INTEGER_MINUS_ONE - batchSize - batchSize / 2 );

        // WHEN
        IdRange range1 = idSequence.nextIdBatch( batchSize );
        IdRange range2 = idSequence.nextIdBatch( batchSize );

        // THEN
        assertNoReservedId( range1 );
        assertNoReservedId( range2 );
    }

    private void assertNoReservedId( IdRange range )
    {
        for ( long id : range.getDefragIds() )
        {
            assertFalse( IdValidator.isReservedId( id ) );
        }

        assertFalse( IdValidator.hasReservedIdInRange( range.getRangeStart(),
                range.getRangeStart() + range.getRangeLength() ) );
    }
}
