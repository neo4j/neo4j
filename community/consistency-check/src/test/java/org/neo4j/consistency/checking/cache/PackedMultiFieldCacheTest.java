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
package org.neo4j.consistency.checking.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.consistency.checking.cache.CacheSlots.ID_SLOT_SIZE;

class PackedMultiFieldCacheTest
{
    @Test
    void shouldPutValuesIntoSlots()
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( 8, 16, 24, 32, 1 );
        int index = 10;
        long[] values = new long[] {3, 100, 12345, 67890, 0};

        // WHEN
        cache.put( index, values );

        // THEN
        for ( int i = 0; i < values.length; i++ )
        {
            assertEquals( values[i], cache.get( index, i ) );
        }
    }

    @Test
    void shouldHaveCorrectDefaultValues()
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( ID_SLOT_SIZE, 5, 1 );
        int index = 0;

        // WHEN
        cache.clear( index );

        // THEN
        assertEquals( -1, cache.get( index, 0 ) );
        assertEquals( 0, cache.get( index, 1 ) );
        assertEquals( 0, cache.get( index, 2 ) );
    }

    @Test
    void shouldBeAbleToChangeSlotSize()
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( 5, 1 );
        int index = 10;
        assertThrows( IllegalArgumentException.class, () -> cache.put( index, 2, 0 ) );

        // WHEN
        cache.setSlotSizes( 8, 8, 10 );

        // THEN
        cache.put( index, 2, 10 );
        assertEquals( 10, cache.get( index, 2 ) );
    }

    @Test
    void shouldHandleTwoIdsAndFourBooleans()
    {
        // given
        PackedMultiFieldCache cache = new PackedMultiFieldCache( ID_SLOT_SIZE, ID_SLOT_SIZE, 1, 1, 1, 1 );
        int index = 3;

        // when
        long v1 = (1L << ID_SLOT_SIZE) - 10;
        long v2 = (1L << ID_SLOT_SIZE) - 100;
        cache.put( index, v1, v2, 0, 1, 0, 1 );

        // then
        assertEquals( v1, cache.get( index, 0 ) );
        assertEquals( v2, cache.get( index, 1 ) );
        assertEquals( 0, cache.get( index, 2 ) );
        assertEquals( -1, cache.get( index, 3 ) );
        assertEquals( 0, cache.get( index, 4 ) );
        assertEquals( -1, cache.get( index, 5 ) );
    }
}
