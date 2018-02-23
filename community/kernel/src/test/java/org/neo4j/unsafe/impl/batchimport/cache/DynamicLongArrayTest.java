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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicLongArrayTest
{
    @Test
    public void shouldWorkOnSingleChunk()
    {
        // GIVEN
        long defaultValue = 0;
        LongArray array = NumberArrayFactory.AUTO_WITHOUT_PAGECACHE.newDynamicLongArray( 10, defaultValue );
        array.set( 4, 5 );

        // WHEN
        assertEquals( 5L, array.get( 4 ) );
        assertEquals( defaultValue, array.get( 12 ) );
        array.set( 7, 1324 );
        assertEquals( 1324L, array.get( 7 ) );
    }

    @Test
    public void shouldChunksAsNeeded()
    {
        // GIVEN
        LongArray array = NumberArrayFactory.AUTO_WITHOUT_PAGECACHE.newDynamicLongArray( 10, 0 );

        // WHEN
        long index = 243;
        long value = 5485748;
        array.set( index, value );

        // THEN
        assertEquals( value, array.get( index ) );
    }
}
