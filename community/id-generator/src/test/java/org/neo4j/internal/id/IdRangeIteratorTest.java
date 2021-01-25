/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution( CONCURRENT )
class IdRangeIteratorTest
{
    @Test
    void shouldReturnValueRepresentingNullIfWeExhaustIdRange()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        for ( int i = 0; i < rangeLength; i++ )
        {
            iterator.nextId();
        }

        // then
        assertEquals( IdRangeIterator.VALUE_REPRESENTING_NULL, iterator.nextId() );
    }

    @Test
    void shouldNotHaveAnyGaps()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        Set<Long> seenIds = new HashSet<>();
        for ( int i = 0; i < rangeLength; i++ )
        {
            seenIds.add( iterator.nextId() );
            if ( i > 0 )
            {
                // then
                assertTrue( seenIds.contains( (long) i - 1 ), "Missing id " + (i - 1) );
            }
        }
    }

    @Test
    void shouldUseDefragIdsFirst()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[] {7, 8, 9}, 1024, rangeLength ).iterator();

        // then
        assertEquals( 7, iterator.nextId() );
        assertEquals( 8, iterator.nextId() );
        assertEquals( 9, iterator.nextId() );
        assertEquals( 1024, iterator.nextId() );
    }
}
