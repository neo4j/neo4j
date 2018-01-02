/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.cache;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PackedMultiFieldCacheTest
{
    @Test
    public void shouldPutValuesIntoSlots() throws Exception
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( 5, 10, 25, 24 );
        int index = 10;
        long[] values = new long[] {3, 100, 12345, 67890};

        // WHEN
        cache.put( index, values );

        // THEN
        for ( int i = 0; i < values.length; i++ )
        {
            assertEquals( values[i], cache.get( index, i ) );
        }
    }

    @Test
    public void shouldHaveCorrectDefaultValues() throws Exception
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( 1, 34, 35 );
        int index = 0;

        // WHEN
        cache.clear( index );

        // THEN
        assertEquals( 0, cache.get( index, 0 ) );
        assertEquals( 0, cache.get( index, 1 ) );
        assertEquals( -1, cache.get( index, 2 ) );
    }

    @Test
    public void shouldBeAbleToChangeSlotSize() throws Exception
    {
        // GIVEN
        PackedMultiFieldCache cache = new PackedMultiFieldCache( 1, 5 );
        int index = 10;
        try
        {
            cache.put( index, 0, 10 );
            fail( "Shouldn't fit" );
        }
        catch ( IllegalStateException e )
        {
            // Good
        }

        // WHEN
        cache.setSlotSizes( 5, 20 );

        // THEN
        cache.put( index, 0, 10 );
        assertEquals( 10, cache.get( index, 0 ) );
    }
}
