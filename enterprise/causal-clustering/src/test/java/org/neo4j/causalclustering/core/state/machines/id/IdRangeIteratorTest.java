/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;
import org.neo4j.kernel.impl.store.id.IdRange;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IdRangeIteratorTest
{
    @Test
    public void shouldReturnValueRepresentingNullIfWeExhaustIdRange() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRangeIterator( new IdRange( new long[]{}, 0, rangeLength ) );

        // when
        for ( int i = 0; i < rangeLength; i++ )
        {
            iterator.next();
        }

        // then
        assertEquals( IdRangeIterator.VALUE_REPRESENTING_NULL, iterator.next() );
    }

    @Test
    public void shouldNotHaveAnyGaps() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRangeIterator( new IdRange( new long[]{}, 0, rangeLength ) );

        // when
        Set<Long> seenIds = new HashSet<>();
        for ( int i = 0; i < rangeLength; i++ )
        {
            seenIds.add( iterator.next() );
            if ( i > 0 )
            {
                // then
                assertTrue( "Missing id " + (i - 1), seenIds.contains( (long) i - 1 ) );
            }
        }
    }

    @Test
    public void shouldUseDefragIdsFirst() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRangeIterator( new IdRange( new long[]{7,8,9}, 1024, rangeLength ) );

        // then
        assertEquals(7, iterator.next());
        assertEquals(8, iterator.next());
        assertEquals(9, iterator.next());
        assertEquals(1024, iterator.next());
    }
}
